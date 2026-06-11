import uuid

from django.conf import settings
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


class QueryStatus(models.TextChoices):
    SUCCESS = "success", "Sucesso"
    ERROR = "error", "Erro"


class RAGQueryRecord(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    request_id = models.UUIDField(unique=True, db_index=True)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="rag_queries",
    )
    authentication_method = models.CharField(max_length=30, blank=True, db_index=True)
    question = models.CharField(max_length=4000, blank=True)
    question_hash = models.CharField(max_length=64, blank=True, db_index=True)
    filters = models.JSONField(default=dict, blank=True)
    status = models.CharField(max_length=20, choices=QueryStatus.choices, db_index=True)
    model = models.CharField(max_length=100, blank=True)
    top_k = models.PositiveSmallIntegerField()
    source_count = models.PositiveSmallIntegerField(default=0)
    duration_ms = models.PositiveIntegerField()
    usage = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [models.Index(fields=["status", "created_at"])]

    def __str__(self) -> str:
        return f"{self.request_id} - {self.status}"


class RAGQuerySource(models.Model):
    query = models.ForeignKey(
        RAGQueryRecord,
        on_delete=models.CASCADE,
        related_name="retrieved_sources",
    )
    document_id = models.CharField(max_length=64, db_index=True)
    chunk_id = models.CharField(max_length=64, blank=True, db_index=True)
    source_name = models.CharField(max_length=500, db_index=True)
    page_number = models.PositiveIntegerField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    rank = models.PositiveSmallIntegerField()
    score = models.FloatField()

    class Meta:
        ordering = ["query_id", "rank"]
        constraints = [
            models.UniqueConstraint(
                fields=["query", "rank"],
                name="unique_query_source_rank",
            )
        ]
        indexes = [models.Index(fields=["document_id", "source_name"])]
