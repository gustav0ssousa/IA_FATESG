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
    def stage(
        self,
        uploaded_file: UploadedFile,
        title: str = "",
        metadata: dict | None = None,
    ) -> IngestionResult:
        digest = hashlib.sha256()
        for chunk in uploaded_file.chunks():
            digest.update(chunk)
        content_hash = digest.hexdigest()
        uploaded_file.seek(0)
        existing = DocumentRepository.get_by_content_hash(content_hash)
        if existing:
            return IngestionResult(document=existing, created=False)

        source_name = Path(uploaded_file.name).name
        document = DocumentRepository.create(
            title=title.strip() or Path(source_name).stem,
            source_name=source_name,
            source_type=Path(source_name).suffix.lower().lstrip("."),
            content_hash=content_hash,
            file=uploaded_file,
            status=DocumentStatus.PENDING,
            metadata={"file_size": uploaded_file.size, **(metadata or {})},
        )
        return IngestionResult(document=document, created=True)

    def process(self, document: Document) -> Document:
        if not document.file:
            raise DocumentIngestionError("O arquivo original nao esta disponivel.")
        document.status = DocumentStatus.PROCESSING
        document.error_message = ""
        document.save(update_fields=["status", "error_message", "updated_at"])
        try:
            with document.file.open("rb") as stored_file:
                content = stored_file.read()
            sections = ExtractorRegistry.get_for_filename(document.source_name).extract(content)
            inferred_metadata = infer_technical_document_metadata(
                sections, document.source_name
            )
            document.metadata = {
                **inferred_metadata,
                **document.metadata,
            }
            if document.title == Path(document.source_name).stem and inferred_metadata["suggested_title"]:
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
        return document

    def ingest(
        self,
        uploaded_file: UploadedFile,
        title: str = "",
        metadata: dict | None = None,
    ) -> IngestionResult:
        result = self.stage(uploaded_file, title=title, metadata=metadata)
        if result.created:
            self.process(result.document)
        return result


class DocumentMetadataService:
    technical_fields = (
        "domain",
        "manufacturer",
        "models",
        "equipment_type",
        "manual_type",
        "language",
    )

    def update(self, document: Document, *, title: str | None, metadata: dict) -> Document:
        if title is not None:
            document.title = title
        document.metadata = {**document.metadata, **metadata}
        for chunk in document.chunks.all():
            chunk.metadata = {
                **chunk.metadata,
                **{
                    key: document.metadata[key]
                    for key in self.technical_fields
                    if key in document.metadata
                },
            }
            chunk.save(update_fields=["metadata"])
        if document.chunks.exists():
            document.status = DocumentStatus.CHUNKED
        document.save(update_fields=["title", "metadata", "status", "updated_at"])
        return document
