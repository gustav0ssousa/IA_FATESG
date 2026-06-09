from celery import shared_task
from django.conf import settings
from django.utils import timezone

from apps.rag.models import IndexingJob, IndexingJobStatus
from apps.rag.services import build_services


@shared_task(bind=True)
def index_document_task(self, job_id: str) -> int | None:
    try:
        job = IndexingJob.objects.select_related("document").get(id=job_id)
    except IndexingJob.DoesNotExist:
        return None

    job.status = IndexingJobStatus.PROCESSING
    job.attempts += 1
    job.started_at = job.started_at or timezone.now()
    job.error_message = ""
    job.save(
        update_fields=[
            "status",
            "attempts",
            "started_at",
            "error_message",
            "updated_at",
        ]
    )

    try:
        indexed_chunks = build_services()[0].index(job.document)
    except Exception as error:
        return _retry_or_fail(self, job, error)

    job.status = IndexingJobStatus.COMPLETED
    job.indexed_chunks = indexed_chunks
    job.error_message = ""
    job.finished_at = timezone.now()
    job.save(
        update_fields=[
            "status",
            "indexed_chunks",
            "error_message",
            "finished_at",
            "updated_at",
        ]
    )
    return indexed_chunks


def _retry_or_fail(task, job: IndexingJob, error: Exception) -> int:
    job.error_message = str(error)
    if task.request.retries >= settings.CELERY_INDEXING_MAX_RETRIES:
        job.status = IndexingJobStatus.FAILED
        job.finished_at = timezone.now()
        job.save(
            update_fields=[
                "status",
                "error_message",
                "finished_at",
                "updated_at",
            ]
        )
        raise error

    job.status = IndexingJobStatus.RETRYING
    job.save(update_fields=["status", "error_message", "updated_at"])
    countdown = min(2 ** (task.request.retries + 1), 60)
    raise task.retry(
        exc=error,
        countdown=countdown,
        max_retries=settings.CELERY_INDEXING_MAX_RETRIES,
    )
