from io import BytesIO

import pytest
from pypdf import PdfWriter

from apps.documents.extractors import (
    DocumentExtractionError,
    ExtractorRegistry,
    PdfTextExtractor,
    PlainTextExtractor,
)


def test_plain_text_extractor_decodes_utf8_with_bom() -> None:
    sections = PlainTextExtractor().extract(b"\xef\xbb\xbfConteudo em portugues.")

    assert len(sections) == 1
    assert sections[0].text == "Conteudo em portugues."


def test_registry_rejects_unsupported_extension() -> None:
    with pytest.raises(DocumentExtractionError, match="Formato nao suportado"):
        ExtractorRegistry.get_for_filename("dados.csv")


def test_pdf_without_extractable_text_reports_ocr_limitation() -> None:
    stream = BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.write(stream)

    with pytest.raises(DocumentExtractionError, match="OCR"):
        PdfTextExtractor().extract(stream.getvalue())
