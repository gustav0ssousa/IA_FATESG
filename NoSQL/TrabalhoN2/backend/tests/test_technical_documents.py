from apps.documents.extractors import ExtractedSection
from apps.documents.technical import (
    classify_technical_chunk,
    enrich_technical_sections,
    infer_technical_document_metadata,
    normalize_manufacturer,
    normalize_models,
)


def manual_sections() -> list[ExtractedSection]:
    return [
        ExtractedSection(
            text=(
                "Brother Laser MFC\nSERVICE MANUAL\n"
                "DCP- L5510DN/L5512DN\nMFC-L5710DN/L5715DW\nEX910/EX915DW\n"
                "Read this manual thoroughly before maintenance work."
            ),
            page_number=1,
        ),
        ExtractedSection(
            text=(
                "CHAPTER 2 ERROR INDICATION AND TROUBLESHOOTING\n"
                "Error codes Description\n0501 Fuser error\n"
            ),
            page_number=20,
        ),
    ]


def test_infers_printer_scanner_manual_metadata_and_models() -> None:
    metadata = infer_technical_document_metadata(manual_sections(), "manual.pdf")

    assert metadata["manufacturer"] == "Brother"
    assert metadata["equipment_type"] == "multifunction"
    assert metadata["manual_type"] == "service_manual"
    assert metadata["models"] == [
        "DCP-L5510DN",
        "DCP-L5512DN",
        "EX910",
        "EX915DW",
        "MFC-L5710DN",
        "MFC-L5715DW",
    ]


def test_enriches_sections_with_chapter_and_error_reference() -> None:
    metadata = infer_technical_document_metadata(manual_sections(), "manual.pdf")
    enriched = enrich_technical_sections(manual_sections(), metadata)
    classification = classify_technical_chunk(enriched[1].text)

    assert enriched[1].metadata["chapter"].startswith("CHAPTER 2")
    assert classification["content_type"] == "troubleshooting"
    assert classification["error_codes"] == ["0501"]


def test_classifies_safety_warning() -> None:
    classification = classify_technical_chunk(
        "WARNING Disconnect the machine before performing maintenance."
    )

    assert classification["content_type"] == "safety"
    assert classification["safety_level"] == "warning"


def test_normalizes_filter_metadata() -> None:
    assert normalize_manufacturer("brother") == "Brother"
    assert normalize_models(["mfc-l5710dn", " MFC-L5710DN "]) == ["MFC-L5710DN"]
