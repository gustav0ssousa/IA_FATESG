import pytest
from rest_framework import status
from rest_framework.test import APIClient

from apps.documents.models import Document, DocumentChunk, DocumentStatus


pytestmark = pytest.mark.django_db


def test_document_list_returns_persisted_documents_with_chunk_count() -> None:
    document = Document.objects.create(
        title="Guia RAG",
        source_name="guia.md",
        source_type="md",
        content_hash="8" * 64,
        status=DocumentStatus.CHUNKED,
    )
    DocumentChunk.objects.create(
        document=document,
        position=0,
        content="Conteudo do guia.",
        content_hash="7" * 64,
    )

    response = APIClient().get("/api/documents/")

    assert response.status_code == status.HTTP_200_OK
    assert response.json()[0]["id"] == str(document.id)
    assert response.json()[0]["chunk_count"] == 1
