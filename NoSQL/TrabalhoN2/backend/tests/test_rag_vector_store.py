import pytest
from qdrant_client import QdrantClient

from apps.documents.models import Document, DocumentChunk, DocumentStatus
from apps.rag.services import (
    DocumentIndexingError,
    DocumentIndexingService,
    SemanticSearchService,
)
from apps.rag.vector_store import QdrantVectorStore


pytestmark = pytest.mark.django_db


class FakeEmbeddingProvider:
    dimension = 3

    def _embed(self, text: str) -> list[float]:
        lowered = text.lower()
        return [
            float(lowered.count("rag")),
            float(lowered.count("postgresql")),
            float(lowered.count("qdrant")),
        ]

    def embed_documents(self, texts):
        return [self._embed(text) for text in texts]

    def embed_query(self, text):
        return self._embed(text)


class FailingEmbeddingProvider(FakeEmbeddingProvider):
    def embed_documents(self, texts):
        raise RuntimeError("modelo indisponivel")


def make_document_with_chunks() -> Document:
    document = Document.objects.create(
        title="Arquitetura RAG",
        source_name="arquitetura.md",
        source_type="md",
        content_hash="d" * 64,
        status=DocumentStatus.CHUNKED,
    )
    DocumentChunk.objects.create(
        document=document,
        position=0,
        content="RAG recupera contexto antes de gerar respostas.",
        content_hash="e" * 64,
    )
    DocumentChunk.objects.create(
        document=document,
        position=1,
        content="PostgreSQL armazena metadados estruturados.",
        content_hash="f" * 64,
    )
    return document


def make_vector_store() -> QdrantVectorStore:
    return QdrantVectorStore(
        client=QdrantClient(":memory:"),
        collection_name="test_documents",
        vector_size=3,
    )


def test_indexing_service_indexes_chunks_and_updates_status() -> None:
    document = make_document_with_chunks()
    service = DocumentIndexingService(FakeEmbeddingProvider(), make_vector_store())

    indexed_count = service.index(document)

    document.refresh_from_db()
    assert indexed_count == 2
    assert document.status == DocumentStatus.INDEXED


def test_indexing_service_rejects_document_without_chunks() -> None:
    document = Document.objects.create(
        title="Vazio",
        source_name="vazio.md",
        source_type="md",
        content_hash="0" * 64,
    )

    with pytest.raises(DocumentIndexingError, match="nao possui chunks"):
        DocumentIndexingService(FakeEmbeddingProvider(), make_vector_store()).index(
            document
        )


def test_semantic_search_returns_most_relevant_chunk() -> None:
    document = make_document_with_chunks()
    embeddings = FakeEmbeddingProvider()
    vector_store = make_vector_store()
    DocumentIndexingService(embeddings, vector_store).index(document)

    results = SemanticSearchService(embeddings, vector_store).search("Como funciona RAG?", 2)

    assert len(results) == 2
    assert results[0].document_id == str(document.id)
    assert "RAG recupera contexto" in results[0].content
    assert results[0].source_name == "arquitetura.md"


def test_reindexing_replaces_old_document_vectors() -> None:
    document = make_document_with_chunks()
    embeddings = FakeEmbeddingProvider()
    vector_store = make_vector_store()
    service = DocumentIndexingService(embeddings, vector_store)
    service.index(document)

    document.chunks.filter(position=1).delete()
    service.index(document)

    results = SemanticSearchService(embeddings, vector_store).search("PostgreSQL", 5)
    assert len(results) == 1


def test_indexing_failure_is_recorded_on_document() -> None:
    document = make_document_with_chunks()
    service = DocumentIndexingService(FailingEmbeddingProvider(), make_vector_store())

    with pytest.raises(DocumentIndexingError, match="modelo indisponivel"):
        service.index(document)

    document.refresh_from_db()
    assert document.status == DocumentStatus.FAILED
    assert "modelo indisponivel" in document.error_message
