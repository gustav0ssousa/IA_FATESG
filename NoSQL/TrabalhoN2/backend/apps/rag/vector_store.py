from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from qdrant_client import QdrantClient, models

from apps.documents.models import DocumentChunk


@dataclass(frozen=True)
class SearchResult:
    chunk_id: str
    document_id: str
    score: float
    content: str
    source_name: str
    page_number: int | None
    metadata: dict


class VectorStore(Protocol):
    def index_chunks(
        self,
        chunks: Sequence[DocumentChunk],
        vectors: Sequence[list[float]],
    ) -> None: ...

    def search(self, vector: list[float], top_k: int) -> list[SearchResult]: ...


class QdrantVectorStore:
    def __init__(
        self,
        client: QdrantClient,
        collection_name: str,
        vector_size: int,
    ) -> None:
        self._client = client
        self._collection_name = collection_name
        self._vector_size = vector_size

    def ensure_collection(self) -> None:
        if not self._client.collection_exists(self._collection_name):
            self._client.create_collection(
                collection_name=self._collection_name,
                vectors_config=models.VectorParams(
                    size=self._vector_size,
                    distance=models.Distance.COSINE,
                ),
            )

    def index_chunks(
        self,
        chunks: Sequence[DocumentChunk],
        vectors: Sequence[list[float]],
    ) -> None:
        if len(chunks) != len(vectors):
            raise ValueError("A quantidade de chunks e vetores deve ser igual.")
        if not chunks:
            return

        self.ensure_collection()
        document_id = str(chunks[0].document_id)
        self._client.delete(
            collection_name=self._collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="document_id",
                            match=models.MatchValue(value=document_id),
                        )
                    ]
                )
            ),
        )
        self._client.upsert(
            collection_name=self._collection_name,
            points=[
                models.PointStruct(
                    id=str(chunk.id),
                    vector=vector,
                    payload={
                        "chunk_id": str(chunk.id),
                        "document_id": str(chunk.document_id),
                        "content": chunk.content,
                        "source_name": chunk.document.source_name,
                        "page_number": chunk.page_number,
                        "metadata": chunk.metadata,
                    },
                )
                for chunk, vector in zip(chunks, vectors, strict=True)
            ],
        )

    def search(self, vector: list[float], top_k: int) -> list[SearchResult]:
        self.ensure_collection()
        response = self._client.query_points(
            collection_name=self._collection_name,
            query=vector,
            limit=top_k,
            with_payload=True,
        )
        return [
            SearchResult(
                chunk_id=point.payload["chunk_id"],
                document_id=point.payload["document_id"],
                score=point.score,
                content=point.payload["content"],
                source_name=point.payload["source_name"],
                page_number=point.payload.get("page_number"),
                metadata=point.payload.get("metadata", {}),
            )
            for point in response.points
        ]
