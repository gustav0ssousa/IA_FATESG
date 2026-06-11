from collections.abc import Collection, Sequence
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


@dataclass(frozen=True)
class VectorReconciliationReport:
    expected_chunks: int
    scanned_points: int
    orphan_point_ids: tuple[str, ...]
    missing_chunk_ids: tuple[str, ...]
    deleted_points: int = 0

    @property
    def consistent(self) -> bool:
        return not self.orphan_point_ids and not self.missing_chunk_ids


class VectorStore(Protocol):
    def index_chunks(
        self,
        chunks: Sequence[DocumentChunk],
        vectors: Sequence[list[float]],
    ) -> None: ...

    def search(
        self,
        vector: list[float],
        top_k: int,
        filters: dict | None = None,
    ) -> list[SearchResult]: ...


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

    def search(
        self,
        vector: list[float],
        top_k: int,
        filters: dict | None = None,
    ) -> list[SearchResult]:
        self.ensure_collection()
        query_filter = None
        if filters:
            query_filter = models.Filter(
                must=[
                    models.FieldCondition(
                        key=f"metadata.{key}",
                        match=models.MatchValue(value=value),
                    )
                    for key, value in filters.items()
                ]
            )
        response = self._client.query_points(
            collection_name=self._collection_name,
            query=vector,
            query_filter=query_filter,
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

    def reconcile(
        self,
        expected_chunk_ids: Collection[str],
        *,
        apply: bool = False,
        batch_size: int = 256,
    ) -> VectorReconciliationReport:
        if batch_size < 1:
            raise ValueError("O tamanho do lote deve ser maior que zero.")

        expected = {str(chunk_id) for chunk_id in expected_chunk_ids}
        if not self._client.collection_exists(self._collection_name):
            return VectorReconciliationReport(
                expected_chunks=len(expected),
                scanned_points=0,
                orphan_point_ids=(),
                missing_chunk_ids=tuple(sorted(expected)),
            )

        seen: set[str] = set()
        orphan_ids: list[str] = []
        orphan_point_ids = []
        offset = None
        scanned_points = 0
        while True:
            points, offset = self._client.scroll(
                collection_name=self._collection_name,
                limit=batch_size,
                offset=offset,
                with_payload=["chunk_id"],
                with_vectors=False,
            )
            for point in points:
                scanned_points += 1
                point_id = str(point.id)
                chunk_id = str((point.payload or {}).get("chunk_id", ""))
                if chunk_id in expected and point_id == chunk_id:
                    seen.add(chunk_id)
                else:
                    orphan_ids.append(point_id)
                    orphan_point_ids.append(point.id)
            if offset is None:
                break

        if apply:
            for start in range(0, len(orphan_ids), batch_size):
                self._client.delete(
                    collection_name=self._collection_name,
                    points_selector=models.PointIdsList(
                        points=orphan_point_ids[start : start + batch_size]
                    ),
                )

        return VectorReconciliationReport(
            expected_chunks=len(expected),
            scanned_points=scanned_points,
            orphan_point_ids=tuple(sorted(orphan_ids)),
            missing_chunk_ids=tuple(sorted(expected - seen)),
            deleted_points=len(orphan_ids) if apply else 0,
        )
