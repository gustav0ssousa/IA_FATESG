from collections.abc import Iterable
from typing import TypedDict

from django.db import transaction

from apps.documents.models import Document, DocumentChunk


class ChunkData(TypedDict):
    position: int
    content: str
    content_hash: str
    token_count: int | None
    page_number: int | None
    metadata: dict


class DocumentRepository:
    @staticmethod
    def create(**values: object) -> Document:
        return Document.objects.create(**values)

    @staticmethod
    def get_by_content_hash(content_hash: str) -> Document | None:
        return Document.objects.filter(content_hash=content_hash).first()

    @staticmethod
    @transaction.atomic
    def replace_chunks(
        document: Document,
        chunks: Iterable[ChunkData],
    ) -> list[DocumentChunk]:
        document.chunks.all().delete()
        return DocumentChunk.objects.bulk_create(
            DocumentChunk(document=document, **chunk) for chunk in chunks
        )
