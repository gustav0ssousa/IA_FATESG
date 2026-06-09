from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Protocol

from pypdf import PdfReader


class DocumentExtractionError(ValueError):
    pass


@dataclass(frozen=True)
class ExtractedSection:
    text: str
    page_number: int | None = None
    metadata: dict = field(default_factory=dict)


class TextExtractor(Protocol):
    def extract(self, content: bytes) -> list[ExtractedSection]: ...


class PlainTextExtractor:
    def extract(self, content: bytes) -> list[ExtractedSection]:
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise DocumentExtractionError("O arquivo deve usar codificacao UTF-8.") from error
        return [ExtractedSection(text=text)]


class PdfTextExtractor:
    def extract(self, content: bytes) -> list[ExtractedSection]:
        try:
            reader = PdfReader(BytesIO(content))
            sections = [
                ExtractedSection(
                    text=page.extract_text() or "",
                    page_number=index,
                    metadata={"page_number": index},
                )
                for index, page in enumerate(reader.pages, start=1)
            ]
        except Exception as error:
            raise DocumentExtractionError("Nao foi possivel extrair o PDF.") from error

        if not any(section.text.strip() for section in sections):
            raise DocumentExtractionError(
                "O PDF nao possui texto extraivel. OCR ainda nao e suportado."
            )
        return sections


class ExtractorRegistry:
    _extractors: dict[str, TextExtractor] = {
        ".txt": PlainTextExtractor(),
        ".md": PlainTextExtractor(),
        ".pdf": PdfTextExtractor(),
    }

    @classmethod
    def get_for_filename(cls, filename: str) -> TextExtractor:
        extension = Path(filename).suffix.lower()
        try:
            return cls._extractors[extension]
        except KeyError as error:
            raise DocumentExtractionError(
                f"Formato nao suportado: {extension or 'sem extensao'}."
            ) from error

    @classmethod
    def supported_extensions(cls) -> tuple[str, ...]:
        return tuple(cls._extractors)
