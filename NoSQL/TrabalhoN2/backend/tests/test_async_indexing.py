from types import SimpleNamespace
from unittest.mock import patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient

from apps.documents.models import Document, DocumentStatus
from apps.rag.async_indexing import AsyncIndexingError, AsyncIndexingService
from apps.rag.models import IndexingJob, IndexingJobStatus
from apps.rag.tasks import index_document_task


pytestmark = pytest.mark.django_db


class FakeIndexingService:
    def index(self, document):
        document.status = DocumentStatus.INDEXED
        document.save(update_fields=["status"])
        return 2


class FailingIndexingService:
    def index(self, document):
        raise RuntimeError("Qdrant indisponivel")


def make_document() -> Document:
    return Document.objects.create(
        title="Guia assincrono",
        source_name="guia.md",
        source_type="md",
        content_hash="9" * 64,
        status=DocumentStatus.CHUNKED,
    )


@patch("apps.rag.tasks.index_document_task.delay")
def test_enqueue_creates_persisted_job(delay_mock) -> None:
    delay_mock.return_value = SimpleNamespace(id="celery-123")

    job = AsyncIndexingService().enqueue(make_document())

    assert job.status == IndexingJobStatus.QUEUED
    assert job.celery_task_id == "celery-123"
    delay_mock.assert_called_once_with(str(job.id))


@patch("apps.rag.tasks.index_document_task.delay", side_effect=RuntimeError("offline"))
def test_enqueue_records_broker_failure(delay_mock) -> None:
    with pytest.raises(AsyncIndexingError, match="offline"):
        AsyncIndexingService().enqueue(make_document())

    job = IndexingJob.objects.get()
    assert job.status == IndexingJobStatus.FAILED
    assert job.finished_at is not None


@patch(
    "apps.rag.tasks.build_services",
    return_value=(FakeIndexingService(), object()),
)
def test_task_completes_job(build_services_mock) -> None:
    job = IndexingJob.objects.create(document=make_document())

    result = index_document_task.run(str(job.id))

    job.refresh_from_db()
    assert result == 2
    assert job.status == IndexingJobStatus.COMPLETED
    assert job.attempts == 1
    assert job.indexed_chunks == 2
    assert job.started_at is not None
    assert job.finished_at is not None


@patch("apps.rag.tasks.settings.CELERY_INDEXING_MAX_RETRIES", 0)
@patch(
    "apps.rag.tasks.build_services",
    return_value=(FailingIndexingService(), object()),
)
def test_task_records_final_failure(build_services_mock) -> None:
    job = IndexingJob.objects.create(document=make_document())

    with pytest.raises(RuntimeError, match="Qdrant indisponivel"):
        index_document_task.run(str(job.id))

    job.refresh_from_db()
    assert job.status == IndexingJobStatus.FAILED
    assert job.attempts == 1
    assert job.finished_at is not None
    assert "Qdrant indisponivel" in job.error_message


@patch("apps.rag.tasks.index_document_task.delay")
def test_async_index_endpoint_returns_job_and_status_endpoint(delay_mock) -> None:
    delay_mock.return_value = SimpleNamespace(id="celery-456")
    document = make_document()

    response = APIClient().post(f"/api/rag/documents/{document.id}/index-async")

    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.json()["status"] == IndexingJobStatus.QUEUED
    job_id = response.json()["id"]

    detail = APIClient().get(f"/api/rag/jobs/{job_id}")
    assert detail.status_code == status.HTTP_200_OK
    assert detail.json()["celery_task_id"] == "celery-456"


@patch("apps.rag.tasks.index_document_task.delay", side_effect=RuntimeError("offline"))
def test_async_index_endpoint_returns_503_when_broker_is_unavailable(delay_mock) -> None:
    document = make_document()

    response = APIClient().post(f"/api/rag/documents/{document.id}/index-async")

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert response.json()["detail"] == (
        "O servico de indexacao esta temporariamente indisponivel."
    )
    assert response.json()["request_id"]
