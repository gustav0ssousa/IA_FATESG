import uuid

import pytest
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APIClient

from apps.documents.models import Document, DocumentStatus
from apps.rag.models import QueryStatus, RAGQueryRecord, RAGQuerySource
from apps.rag.observability import percentile
from apps.rag.services import RAGQueryError

pytestmark = pytest.mark.django_db


def make_query(status_value=QueryStatus.SUCCESS, duration_ms=100) -> RAGQueryRecord:
    return RAGQueryRecord.objects.create(
        request_id=uuid.uuid4(),
        question="Como funciona o RAG?",
        status=status_value,
        model="sabia-4" if status_value == QueryStatus.SUCCESS else "",
        top_k=5,
        source_count=1 if status_value == QueryStatus.SUCCESS else 0,
        duration_ms=duration_ms,
    )


def test_percentile_handles_empty_and_ordered_values() -> None:
    assert percentile([], 0.95) == 0
    assert percentile([100, 300, 200], 0.95) == 300


def test_kpi_overview_aggregates_queries_and_sources() -> None:
    document = Document.objects.create(
        title="Guia RAG",
        source_name="guia.md",
        source_type="md",
        content_hash="8" * 64,
        status=DocumentStatus.INDEXED,
    )
    successful = make_query(duration_ms=100)
    make_query(status_value=QueryStatus.ERROR, duration_ms=300)
    RAGQuerySource.objects.create(
        query=successful,
        document_id=str(document.id),
        source_name=document.source_name,
        rank=1,
        score=0.91,
    )

    response = APIClient().get("/api/rag/kpis/overview")

    assert response.status_code == status.HTTP_200_OK
    body = response.json()
    assert body["queries"]["total"] == 2
    assert body["queries"]["error_rate"] == 0.5
    assert body["queries"]["average_response_ms"] == 200
    assert body["documents"]["indexed"] == 1
    assert body["documents"]["top_retrieved"][0]["source_name"] == "guia.md"
    assert len(body["timeline"]) == 7
    assert body["recent_queries"][0]["question"] == "[conteudo oculto]"


@override_settings(OBSERVABILITY_EXPOSE_QUESTION_TEXT=True)
def test_kpi_overview_can_expose_questions_in_trusted_environment() -> None:
    make_query()

    response = APIClient().get("/api/rag/kpis/overview")

    assert response.json()["recent_queries"][0]["question"] == "Como funciona o RAG?"


def test_query_endpoint_persists_history_and_source() -> None:
    from unittest.mock import patch

    result = {
        "answer": "Resposta [Fonte 1].",
        "sources": [
            {
                "number": 1,
                "document_id": str(uuid.uuid4()),
                "source_name": "guia.md",
                "score": 0.93,
            }
        ],
        "model": "sabia-4",
        "usage": {"output_tokens": 12},
    }
    with patch("apps.rag.views.build_rag_query_service") as build_service_mock:
        build_service_mock.return_value.answer.return_value = result
        response = APIClient().post(
            "/api/rag/query",
            {"question": "Como funciona?", "top_k": 3},
            format="json",
        )

    record = RAGQueryRecord.objects.get()
    assert response.status_code == status.HTTP_200_OK
    assert record.request_id == uuid.UUID(response.json()["request_id"])
    assert record.status == QueryStatus.SUCCESS
    assert record.retrieved_sources.get().source_name == "guia.md"


def test_query_endpoint_persists_failure() -> None:
    from unittest.mock import patch

    with patch("apps.rag.views.build_rag_query_service") as build_service_mock:
        build_service_mock.return_value.answer.side_effect = RAGQueryError("LLM offline")
        response = APIClient().post(
            "/api/rag/query",
            {"question": "Pergunta com falha"},
            format="json",
        )

    record = RAGQueryRecord.objects.get()
    assert response.status_code == status.HTTP_502_BAD_GATEWAY
    assert response.json()["detail"] == "Nao foi possivel concluir a consulta RAG."
    assert "LLM offline" not in str(response.json())
    assert record.status == QueryStatus.ERROR
    assert "LLM offline" in record.error_message
