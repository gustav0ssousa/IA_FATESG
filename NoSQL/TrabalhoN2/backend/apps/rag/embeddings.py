import json
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


class RemoteEmbeddingProvider:
    def __init__(self, url: str, dimension: int, timeout_seconds: float) -> None:
        self._url = url
        self._dimension = dimension
        self._timeout_seconds = timeout_seconds

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        return self._request("documents", list(texts))

    def embed_query(self, text: str) -> list[float]:
        return self._request("query", [text])[0]

    def _request(self, mode: str, texts: list[str]) -> list[list[float]]:
        request = Request(
            self._url,
            data=json.dumps({"mode": mode, "texts": texts}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.load(response)
        except (HTTPError, URLError, TimeoutError) as error:
            raise RuntimeError(f"Servico de embeddings indisponivel: {error}") from error
        vectors = payload.get("vectors", [])
        if len(vectors) != len(texts):
            raise RuntimeError("Servico de embeddings retornou quantidade invalida de vetores.")
        return vectors
