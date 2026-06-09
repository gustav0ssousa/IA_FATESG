import uuid

from django.db import models

from apps.documents.models import Document


class IndexingJobStatus(models.TextChoices):
    QUEUED = "queued", "Na fila"
    PROCESSING = "processing", "Processando"
    RETRYING = "retrying", "Aguardando nova tentativa"
    COMPLETED = "completed", "Concluido"
    FAILED = "failed", "Falhou"


class IndexingJob(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey(
        Document,
        on_delete=models.CASCADE,
        related_name="indexing_jobs",
    )
    celery_task_id = models.CharField(max_length=255, blank=True, db_index=True)
    status = models.CharField(
        max_length=20,
        choices=IndexingJobStatus.choices,
        default=IndexingJobStatus.QUEUED,
        db_index=True,
    )
    attempts = models.PositiveIntegerField(default=0)
    indexed_chunks = models.PositiveIntegerField(default=0)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [models.Index(fields=["document", "status"])]

    def __str__(self) -> str:
        return f"{self.document.title} - {self.status}"
