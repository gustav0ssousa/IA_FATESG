import uuid

from django.db import models


class DocumentStatus(models.TextChoices):
    PENDING = "pending", "Pendente"
    PROCESSING = "processing", "Processando"
    CHUNKED = "chunked", "Segmentado"
    INDEXED = "indexed", "Indexado"
    FAILED = "failed", "Falhou"


class Document(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(max_length=255)
    source_name = models.CharField(max_length=500)
    source_type = models.CharField(max_length=50)
    content_hash = models.CharField(max_length=64, unique=True)
    status = models.CharField(
        max_length=20,
        choices=DocumentStatus.choices,
        default=DocumentStatus.PENDING,
        db_index=True,
    )
    metadata = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["source_type", "status"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self) -> str:
        return self.title


class DocumentChunk(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey(
        Document,
        on_delete=models.CASCADE,
        related_name="chunks",
    )
    position = models.PositiveIntegerField()
    content = models.TextField()
    content_hash = models.CharField(max_length=64)
    token_count = models.PositiveIntegerField(null=True, blank=True)
    page_number = models.PositiveIntegerField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["document_id", "position"]
        constraints = [
            models.UniqueConstraint(
                fields=["document", "position"],
                name="unique_document_chunk_position",
            ),
        ]
        indexes = [
            models.Index(fields=["document", "content_hash"]),
        ]

    def __str__(self) -> str:
        return f"{self.document.title} - chunk {self.position}"
