from django.utils import timezone

from apps.documents.models import Document
from apps.rag.models import IndexingJob, IndexingJobStatus


class AsyncIndexingError(RuntimeError):
    pass


class AsyncIndexingService:
    def enqueue(self, document: Document) -> IndexingJob:
        from apps.rag.tasks import index_document_task

        job = IndexingJob.objects.create(document=document)
        try:
            result = index_document_task.delay(str(job.id))
        except Exception as error:
            job.status = IndexingJobStatus.FAILED
            job.error_message = f"Falha ao publicar job de indexacao: {error}"
            job.finished_at = timezone.now()
            job.save(
                update_fields=[
                    "status",
                    "error_message",
                    "finished_at",
                    "updated_at",
                ]
            )
            raise AsyncIndexingError(job.error_message) from error

        job.celery_task_id = result.id
        job.save(update_fields=["celery_task_id", "updated_at"])
        return job
