import json
import time
from collections.abc import Sequence
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastembed import TextEmbedding


class EmbeddingProvider(Protocol):
    @property
    def dimension(self) -> int: ...

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]: ...

    def embed_query(self, text: str) -> list[float]: ...


class FastEmbedProvider:
    def __init__(
        self,
        model_name: str,
        dimension: int,
        cache_dir: str,
        threads: int,
        enable_cpu_mem_arena: bool,
    ) -> None:
        self._dimension = dimension
        self._model = TextEmbedding(
            model_name=model_name,
            cache_dir=cache_dir,
            threads=threads,
            enable_cpu_mem_arena=enable_cpu_mem_arena,
        )

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        return [embedding.tolist() for embedding in self._model.embed(list(texts))]

    def embed_query(self, text: str) -> list[float]:
        return next(self._model.query_embed(text)).tolist()


_DEFAULT_REMOTE_BATCH_SIZE = 32
_RETRYABLE_HTTP_STATUSES = {408, 429, 500, 502, 503, 504}


class RemoteEmbeddingProvider:
    def __init__(
        self,
        url: str,
        dimension: int,
        timeout_seconds: float,
        batch_size: int = _DEFAULT_REMOTE_BATCH_SIZE,
        max_retries: int = 1,
        retry_base_delay_seconds: float = 1.0,
    ) -> None:
        self._url = url
        self._dimension = dimension
        self._timeout_seconds = timeout_seconds
        self._batch_size = max(1, batch_size)
        self._max_retries = max(0, max_retries)
        self._retry_base_delay_seconds = max(0.0, retry_base_delay_seconds)

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        all_texts = list(texts)
        if len(all_texts) <= self._batch_size:
            return self._request("documents", all_texts)
        results: list[list[float]] = []
        for i in range(0, len(all_texts), self._batch_size):
            batch = all_texts[i : i + self._batch_size]
            results.extend(self._request("documents", batch))
        return results

    def embed_query(self, text: str) -> list[float]:
        return self._request("query", [text])[0]

    def _request(self, mode: str, texts: list[str]) -> list[list[float]]:
        request = Request(
            self._url,
            data=json.dumps({"mode": mode, "texts": texts}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        for attempt in range(self._max_retries + 1):
            try:
                with urlopen(request, timeout=self._timeout_seconds) as response:
                    payload = json.load(response)
                break
            except (HTTPError, URLError, TimeoutError) as error:
                if attempt >= self._max_retries or not self._should_retry(error):
                    raise RuntimeError(
                        f"Servico de embeddings indisponivel: {error}"
                    ) from error
                time.sleep(self._retry_base_delay_seconds * (2**attempt))
        vectors = payload.get("vectors", [])
        if len(vectors) != len(texts):
            raise RuntimeError("Servico de embeddings retornou quantidade invalida de vetores.")
        return vectors

    def _should_retry(self, error: Exception) -> bool:
        if isinstance(error, HTTPError):
            return error.code in _RETRYABLE_HTTP_STATUSES
        return isinstance(error, (URLError, TimeoutError))
