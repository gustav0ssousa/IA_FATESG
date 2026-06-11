from types import SimpleNamespace
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from rest_framework import status
from rest_framework.test import APIClient

from apps.documents.models import DocumentStatus
from apps.documents.services import DocumentIngestionService

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def document_media(tmp_path, settings):
    settings.MEDIA_ROOT = tmp_path


def staged_document():
    return DocumentIngestionService().stage(
        SimpleUploadedFile("manual.md", b"# Manual\n\nTechnical procedure.")
    ).document


@patch("apps.rag.tasks.index_document_task.delay")
def test_metadata_update_normalizes_values_and_enqueues_reindex(delay_mock) -> None:
    delay_mock.return_value = SimpleNamespace(id="celery-metadata")
    document = staged_document()
    DocumentIngestionService().process(document)

    response = APIClient().patch(
        f"/api/documents/{document.id}",
        {
            "title": "Manual revisado",
            "manufacturer": "brother",
            "models": ["mfc-l5710dn", " MFC-L5710DN "],
            "manual_type": "service_manual",
        },
        format="json",
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["document"]["title"] == "Manual revisado"
    assert response.json()["document"]["metadata"]["manufacturer"] == "Brother"
    assert response.json()["document"]["metadata"]["models"] == ["MFC-L5710DN"]
    assert response.json()["job"]["status"] == "queued"
    assert document.chunks.first().metadata["manufacturer"] == "Brother"


@patch("apps.rag.tasks.index_document_task.delay")
def test_reprocess_endpoint_resets_document_and_enqueues_pipeline(delay_mock) -> None:
    delay_mock.return_value = SimpleNamespace(id="celery-reprocess")
    document = staged_document()
    DocumentIngestionService().process(document)

    response = APIClient().post(f"/api/documents/{document.id}/reprocess")

    document.refresh_from_db()
    assert response.status_code == status.HTTP_202_ACCEPTED
    assert document.status == DocumentStatus.PENDING
    assert response.json()["status"] == "queued"


def test_reprocess_rejects_legacy_document_without_original_file() -> None:
    document = staged_document()
    document.file.delete(save=False)
    document.file = ""
    document.save(update_fields=["file"])

    response = APIClient().post(f"/api/documents/{document.id}/reprocess")

    assert response.status_code == status.HTTP_409_CONFLICT
