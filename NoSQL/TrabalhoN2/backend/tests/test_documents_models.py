import pytest
from django.db import IntegrityError, transaction

from apps.documents.models import Document, DocumentChunk, DocumentStatus
from apps.documents.repositories import DocumentRepository


pytestmark = pytest.mark.django_db


def make_document(**overrides: object) -> Document:
    values = {
        "title": "Manual RAG",
        "source_name": "manual-rag.md",
        "source_type": "markdown",
        "content_hash": "a" * 64,
    }
    values.update(overrides)
    return Document.objects.create(**values)


def test_document_defaults_to_pending_and_keeps_metadata() -> None:
    document = make_document(metadata={"category": "technical"})

    assert document.status == DocumentStatus.PENDING
    assert document.metadata == {"category": "technical"}
    assert str(document) == "Manual RAG"


def test_document_content_hash_must_be_unique() -> None:
    make_document()

    with pytest.raises(IntegrityError), transaction.atomic():
        make_document(title="Duplicado")


def test_document_deletion_cascades_to_chunks() -> None:
    document = make_document()
    DocumentChunk.objects.create(
        document=document,
        position=0,
        content="Primeiro trecho.",
        content_hash="b" * 64,
    )

    document.delete()

    assert DocumentChunk.objects.count() == 0


def test_chunk_position_is_unique_per_document() -> None:
    document = make_document()
    chunk_data = {
        "document": document,
        "position": 0,
        "content": "Primeiro trecho.",
        "content_hash": "b" * 64,
    }
    DocumentChunk.objects.create(**chunk_data)

    with pytest.raises(IntegrityError), transaction.atomic():
        DocumentChunk.objects.create(**chunk_data)


def test_repository_finds_document_by_content_hash() -> None:
    document = make_document()

    found = DocumentRepository.get_by_content_hash(document.content_hash)

    assert found == document
    assert DocumentRepository.get_by_content_hash("missing") is None


def test_repository_replaces_chunks_during_reprocessing() -> None:
    document = make_document()
    DocumentChunk.objects.create(
        document=document,
        position=0,
        content="Trecho antigo.",
        content_hash="b" * 64,
    )

    created = DocumentRepository.replace_chunks(
        document,
        [
            {
                "position": 0,
                "content": "Trecho novo.",
                "content_hash": "c" * 64,
                "token_count": 3,
                "page_number": 1,
                "metadata": {"section": "intro"},
            }
        ],
    )

    assert len(created) == 1
    assert list(document.chunks.values_list("content", flat=True)) == [
        "Trecho novo."
    ]
