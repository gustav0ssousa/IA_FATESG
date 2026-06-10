import hashlib
from dataclasses import dataclass
from pathlib import Path

from django.conf import settings
from django.core.files.uploadedfile import UploadedFile

from apps.documents.chunking import ChunkingConfig, LangChainTextChunker
from apps.documents.extractors import DocumentExtractionError, ExtractorRegistry
from apps.documents.models import Document, DocumentStatus
from apps.documents.repositories import DocumentRepository
from apps.documents.technical import (
    enrich_technical_sections,
    infer_technical_document_metadata,
)


class DocumentIngestionError(ValueError):
    pass


@dataclass(frozen=True)
class IngestionResult:
    document: Document
    created: bool


class DocumentIngestionService:
    def ingest(
        self,
        uploaded_file: UploadedFile,
        title: str = "",
        metadata: dict | None = None,
    ) -> IngestionResult:
        content = uploaded_file.read()
        content_hash = hashlib.sha256(content).hexdigest()
        existing = DocumentRepository.get_by_content_hash(content_hash)
        if existing:
            return IngestionResult(document=existing, created=False)

        source_name = Path(uploaded_file.name).name
        document = DocumentRepository.create(
            title=title.strip() or Path(source_name).stem,
            source_name=source_name,
            source_type=Path(source_name).suffix.lower().lstrip("."),
            content_hash=content_hash,
            status=DocumentStatus.PROCESSING,
            metadata={"file_size": len(content), **(metadata or {})},
        )

        try:
            sections = ExtractorRegistry.get_for_filename(source_name).extract(content)
            inferred_metadata = infer_technical_document_metadata(sections, source_name)
            document.metadata = {
                **inferred_metadata,
                **document.metadata,
            }
            if not title.strip() and inferred_metadata["suggested_title"]:
                document.title = inferred_metadata["suggested_title"]
            sections = enrich_technical_sections(sections, document.metadata)
            chunker = LangChainTextChunker(
                ChunkingConfig(
                    chunk_size=settings.RAG_CHUNK_SIZE,
                    chunk_overlap=settings.RAG_CHUNK_OVERLAP,
                )
            )
            chunks = chunker.split(sections)
            if not chunks:
                raise DocumentExtractionError("O documento nao possui texto utilizavel.")
            DocumentRepository.replace_chunks(document, chunks)
            document.status = DocumentStatus.CHUNKED
            document.error_message = ""
            document.save(
                update_fields=["title", "metadata", "status", "error_message", "updated_at"]
            )
        except (DocumentExtractionError, ValueError) as error:
            document.status = DocumentStatus.FAILED
            document.error_message = str(error)
            document.save(update_fields=["status", "error_message", "updated_at"])
            raise DocumentIngestionError(str(error)) from error

        return IngestionResult(document=document, created=True)
