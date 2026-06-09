from collections.abc import Sequence
from typing import Protocol

from fastembed import TextEmbedding


class EmbeddingProvider(Protocol):
    @property
    def dimension(self) -> int: ...

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]: ...

    def embed_query(self, text: str) -> list[float]: ...


class FastEmbedProvider:
    def __init__(self, model_name: str, dimension: int) -> None:
        self._dimension = dimension
        self._model = TextEmbedding(model_name=model_name)

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        return [embedding.tolist() for embedding in self._model.embed(list(texts))]

    def embed_query(self, text: str) -> list[float]:
        return next(self._model.query_embed(text)).tolist()
