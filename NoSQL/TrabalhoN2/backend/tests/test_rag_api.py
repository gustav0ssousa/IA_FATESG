from unittest.mock import patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient

from apps.documents.models import Document, DocumentStatus
from apps.rag.vector_store import SearchResult


pytestmark = pytest.mark.django_db


class FakeIndexingService:
    def index(self, document):
        document.status = DocumentStatus.INDEXED
        document.save(update_fields=["status"])
        return 2


class FakeSearchService:
    def search(self, query, top_k):
        return [
            SearchResult(
                chunk_id="chunk-1",
                document_id="document-1",
                score=0.98,
                content="RAG recupera contexto relevante.",
                source_name="guia.md",
                page_number=None,
                metadata={},
            )
        ]


def make_document() -> Document:
    return Document.objects.create(
        title="Guia",
        source_name="guia.md",
        source_type="md",
        content_hash="1" * 64,
        status=DocumentStatus.CHUNKED,
    )


@patch(
    "apps.rag.views.build_services",
    return_value=(FakeIndexingService(), FakeSearchService()),
)
def test_index_endpoint_returns_indexed_summary(build_services_mock) -> None:
    document = make_document()

    response = APIClient().post(f"/api/rag/documents/{document.id}/index")

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == DocumentStatus.INDEXED
    assert response.json()["indexed_chunks"] == 2


@patch(
    "apps.rag.views.build_services",
    return_value=(FakeIndexingService(), FakeSearchService()),
)
def test_search_endpoint_returns_sources(build_services_mock) -> None:
    response = APIClient().post(
        "/api/rag/search",
        {"query": "Como funciona RAG?", "top_k": 3},
        format="json",
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["results"][0]["score"] == 0.98
    assert response.json()["results"][0]["source_name"] == "guia.md"


def test_search_endpoint_validates_top_k() -> None:
    response = APIClient().post(
        "/api/rag/search",
        {"query": "RAG", "top_k": 50},
        format="json",
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
