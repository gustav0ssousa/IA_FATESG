from types import SimpleNamespace
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APIClient

from apps.documents.models import Document, DocumentStatus
from apps.documents.services import DocumentIngestionError, DocumentIngestionService


pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def document_media(tmp_path, settings):
    settings.MEDIA_ROOT = tmp_path


def text_upload(name: str = "guia.md") -> SimpleUploadedFile:
    return SimpleUploadedFile(
        name,
        (
            "# Guia RAG\n\n"
            "Este documento explica o fluxo de ingestao e recuperacao. " * 8
        ).encode(),
        content_type="text/markdown",
    )


@override_settings(RAG_CHUNK_SIZE=100, RAG_CHUNK_OVERLAP=20)
def test_ingestion_service_persists_document_and_chunks() -> None:
    result = DocumentIngestionService().ingest(text_upload(), title="Guia tecnico")

    assert result.created is True
    assert result.document.title == "Guia tecnico"
    assert result.document.status == DocumentStatus.CHUNKED
    assert result.document.chunks.count() > 1
    assert result.document.file


def test_ingestion_service_returns_existing_duplicate() -> None:
    service = DocumentIngestionService()

    first = service.ingest(text_upload())
    duplicate = service.ingest(text_upload())

    assert first.created is True
    assert duplicate.created is False
    assert duplicate.document == first.document
    assert Document.objects.count() == 1


def test_ingestion_failure_is_auditable() -> None:
    invalid_utf8 = SimpleUploadedFile("invalido.txt", b"\xff\xfe")

    with pytest.raises(DocumentIngestionError, match="UTF-8"):
        DocumentIngestionService().ingest(invalid_utf8)

    document = Document.objects.get()
    assert document.status == DocumentStatus.FAILED
    assert "UTF-8" in document.error_message


@patch("apps.rag.tasks.index_document_task.delay")
def test_ingestion_endpoint_stages_document_and_enqueues_pipeline(delay_mock) -> None:
    delay_mock.return_value = SimpleNamespace(id="celery-ingest")
    response = APIClient().post(
        "/api/documents/ingest",
        {"file": text_upload(), "title": "Documento API"},
        format="multipart",
    )

    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.json()["document"]["status"] == DocumentStatus.PENDING
    assert response.json()["document"]["chunk_count"] == 0
    assert response.json()["job"]["status"] == "queued"
    assert response.json()["duplicate"] is False
    delay_mock.assert_called_once()


def test_ingestion_endpoint_rejects_unsupported_file() -> None:
    response = APIClient().post(
        "/api/documents/ingest",
        {"file": SimpleUploadedFile("dados.csv", b"a,b\n1,2")},
        format="multipart",
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Formato nao suportado" in str(response.json())
