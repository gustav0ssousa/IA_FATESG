import uuid
from datetime import timedelta
from io import StringIO
from unittest.mock import patch

import pytest
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from apps.rag.models import RAGQueryRecord

pytestmark = pytest.mark.django_db


def query_result() -> dict:
    return {
        "answer": "Procedimento fundamentado [Fonte 1].",
        "sources": [
            {
                "number": 1,
                "chunk_id": str(uuid.uuid4()),
                "document_id": str(uuid.uuid4()),
                "source_name": "manual.pdf",
                "page_number": 35,
                "metadata": {"manufacturer": "Brother", "models": ["MFC-L5710DN"]},
                "score": 0.93,
            }
        ],
        "model": "sabia-4",
        "usage": {},
    }


@override_settings(API_REQUIRE_AUTHENTICATION=True, AUDIT_STORE_QUESTION_TEXT=False)
def test_query_audit_tracks_user_filters_and_redacts_question() -> None:
    user = get_user_model().objects.create_user(username="tecnico", password="senha")
    client = APIClient()
    client.force_authenticate(user)

    with patch("apps.rag.views.build_rag_query_service") as build_service_mock:
        build_service_mock.return_value.answer.return_value = query_result()
        response = client.post(
            "/api/rag/query",
            {
                "question": "Como corrigir o erro?",
                "top_k": 3,
                "manufacturer": "Brother",
                "model": "mfc-l5710dn",
            },
            format="json",
        )

    record = RAGQueryRecord.objects.get()
    source = record.retrieved_sources.get()
    assert response.status_code == status.HTTP_200_OK
    assert record.user == user
    assert record.authentication_method == "token"
    assert record.question == ""
    assert len(record.question_hash) == 64
    assert record.filters == {"manufacturer": "Brother", "models": "MFC-L5710DN"}
    assert source.chunk_id
    assert source.page_number == 35
    assert source.metadata["manufacturer"] == "Brother"


@override_settings(
    API_ACCESS_KEY="segredo",
    API_REQUIRE_AUTHENTICATION=True,
    AUDIT_STORE_QUESTION_TEXT=True,
)
def test_query_audit_identifies_api_key_and_can_store_question() -> None:
    with patch("apps.rag.views.build_rag_query_service") as build_service_mock:
        build_service_mock.return_value.answer.return_value = query_result()
        response = APIClient().post(
            "/api/rag/query",
            {"question": "Pergunta auditavel"},
            format="json",
            HTTP_X_API_KEY="segredo",
        )

    record = RAGQueryRecord.objects.get()
    assert response.status_code == status.HTTP_200_OK
    assert record.user is None
    assert record.authentication_method == "api_key"
    assert record.question == "Pergunta auditavel"


def test_purge_rag_audit_is_dry_run_by_default_and_applies_explicitly() -> None:
    record = RAGQueryRecord.objects.create(
        request_id=uuid.uuid4(),
        status="success",
        top_k=5,
        duration_ms=10,
    )
    RAGQueryRecord.objects.filter(id=record.id).update(
        created_at=timezone.now() - timedelta(days=100)
    )
    output = StringIO()

    call_command("purge_rag_audit", days=90, stdout=output)
    assert "DRY-RUN" in output.getvalue()
    assert RAGQueryRecord.objects.filter(id=record.id).exists()

    call_command("purge_rag_audit", days=90, apply=True)
    assert not RAGQueryRecord.objects.filter(id=record.id).exists()
