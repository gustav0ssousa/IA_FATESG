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
    assert response.json()["results"][0]["id"] == str(document.id)
    assert response.json()["results"][0]["chunk_count"] == 1
    assert response.json()["pagination"]["total"] == 1


def test_document_list_paginates_and_returns_global_facets() -> None:
    for index in range(3):
        Document.objects.create(
            title=f"Manual {index}",
            source_name=f"manual-{index}.pdf",
            source_type="pdf",
            content_hash=str(index) * 64,
            metadata={
                "manufacturer": "Brother" if index < 2 else "Epson",
                "models": [f"MODEL-{index}"],
            },
        )

    response = APIClient().get("/api/documents/?page=2&page_size=2")
    body = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert len(body["results"]) == 1
    assert body["pagination"] == {
        "page": 2,
        "page_size": 2,
        "total": 3,
        "total_pages": 2,
    }
    assert body["facets"]["manufacturers"] == ["Brother", "Epson"]
    assert body["facets"]["models"] == ["MODEL-0", "MODEL-1", "MODEL-2"]
