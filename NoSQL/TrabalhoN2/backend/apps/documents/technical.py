import re
from dataclasses import replace

from apps.documents.extractors import ExtractedSection

KNOWN_MANUFACTURERS = (
    "Brother",
    "Canon",
    "Epson",
    "HP",
    "Kyocera",
    "Lexmark",
    "Ricoh",
    "Samsung",
    "Xerox",
)
MODEL_GROUP_PATTERN = re.compile(
    r"\b(DCP|MFC|HL|ADS|DS|WF)-?\s*([A-Z0-9]{3,12}(?:/[A-Z0-9]{3,12})*)",
    re.IGNORECASE,
)
SHORT_MODEL_GROUP_PATTERN = re.compile(r"\b(EX)(\d{3,5}[A-Z]*(?:/\d{3,5}[A-Z]*)*)\b")
ERROR_CODE_GROUP_PATTERN = re.compile(
    r"error codes?\s*:?\s*((?:[0-9A-F]{4}(?:\s*,\s*)?)+)",
    re.IGNORECASE,
)
HEADING_PATTERN = re.compile(r"^(?:\d+(?:\.\d+)*\s+|CHAPTER\s+\d+\s+)(.+)", re.IGNORECASE)


def infer_technical_document_metadata(
    sections: list[ExtractedSection],
    source_name: str,
) -> dict:
    sample = "\n".join(section.text for section in sections[:8])
    upper = sample.upper()
    manufacturer = next(
        (name for name in KNOWN_MANUFACTURERS if name.upper() in upper),
        "",
    )
    models = extract_models(sample)
    if "SERVICE MANUAL" in upper:
        manual_type = "service_manual"
    elif "USER" in upper and "MANUAL" in upper:
        manual_type = "user_manual"
    elif "INSTALLATION" in upper:
        manual_type = "installation_manual"
    else:
        manual_type = "technical_document"

    has_printer = any(term in upper for term in ("PRINTER", "PRINTING", "LASER MFC"))
    has_scanner = any(term in upper for term in ("MFC", "SCANNER", "SCANNING", "ADF"))
    equipment_type = (
        "multifunction"
        if has_printer and has_scanner
        else "printer"
        if has_printer
        else "scanner"
        if has_scanner
        else "other"
    )
    language = "en" if "THE " in upper and "MANUAL" in upper else "und"
    model_summary = (
        f"{models[0]} + {len(models) - 1} models" if len(models) > 1 else " ".join(models)
    )
    title_parts = [
        part
        for part in (manufacturer, model_summary, manual_type.replace("_", " ").title())
        if part
    ]

    return {
        "domain": "technical_support",
        "manufacturer": manufacturer,
        "models": models,
        "equipment_type": equipment_type,
        "manual_type": manual_type,
        "language": language,
        "page_count": len(sections),
        "source_name": source_name,
        "suggested_title": " - ".join(title_parts),
    }


def enrich_technical_sections(
    sections: list[ExtractedSection],
    document_metadata: dict,
) -> list[ExtractedSection]:
    chapter = ""
    section_heading = ""
    enriched: list[ExtractedSection] = []
    inherited = {
        key: document_metadata[key]
        for key in ("domain", "manufacturer", "models", "equipment_type", "manual_type", "language")
        if document_metadata.get(key)
    }
    for section in sections:
        heading = detect_heading(section.text)
        if heading.upper().startswith("CHAPTER "):
            chapter = heading
        elif heading:
            section_heading = heading
        metadata = {
            **section.metadata,
            **inherited,
            "chapter": chapter,
            "section_heading": heading or section_heading,
        }
        enriched.append(replace(section, metadata=metadata))
    return enriched


def classify_technical_chunk(content: str) -> dict:
    upper = content.upper()
    normalized_upper = " ".join(upper.split())
    error_codes = []
    for group in ERROR_CODE_GROUP_PATTERN.findall(normalized_upper):
        error_codes.extend(re.findall(r"\b[0-9A-F]{4}\b", group.upper()))
    if any(
        term in normalized_upper
        for term in ("ERROR CODES", "ERROR INDICATION", "ERROR MESSAGE")
    ):
        error_codes.extend(
            code
            for code in re.findall(r"\b[0-9A-F]{4}\b", normalized_upper)
            if any(character.isdigit() for character in code)
        )

    if "WARNING" in upper or "CAUTION" in upper:
        content_type = "safety"
    elif "TROUBLESHOOT" in upper or ("CAUSE" in upper and "REMEDY" in upper):
        content_type = "troubleshooting"
    elif error_codes or "ERROR MESSAGE" in upper or "ERROR INDICATION" in upper:
        content_type = "error_reference"
    elif any(term in upper for term in ("DISASSEMBLY", "REASSEMBLY", "PROCEDURE", "FUNCTION CODE")):
        content_type = "procedure"
    elif any(term in upper for term in ("SPECIFICATION", "DIMENSIONS", "WEIGHTS")):
        content_type = "specification"
    elif "MAINTENANCE" in upper or "CLEANING" in upper:
        content_type = "maintenance"
    else:
        content_type = "technical_reference"

    safety_level = "warning" if "WARNING" in upper else "caution" if "CAUTION" in upper else ""
    return {
        "content_type": content_type,
        "error_codes": sorted(set(error_codes)),
        "safety_level": safety_level,
    }


def detect_heading(text: str) -> str:
    for line in text.splitlines()[:12]:
        candidate = " ".join(line.split()).strip(" .")
        if not candidate or candidate.lower() == "confidential":
            continue
        if re.match(r"^[0-9A-F]{4}\s", candidate, re.IGNORECASE):
            continue
        match = HEADING_PATTERN.match(candidate)
        if match:
            return candidate[:240]
        if len(candidate) <= 100 and candidate.isupper() and len(candidate.split()) >= 2:
            return candidate[:240]
    return ""


def extract_models(text: str) -> list[str]:
    models: set[str] = set()
    model_block = re.search(r"\bMODEL\b(.*?)\bOPTION\b", text, re.IGNORECASE | re.DOTALL)
    current_prefix = ""
    if model_block:
        for line in model_block.group(1).splitlines():
            value = line.strip().rstrip("/")
            prefix_match = re.match(r"^(DCP|MFC|HL|ADS|DS|WF)-?\s*(.*)", value, re.IGNORECASE)
            if prefix_match:
                current_prefix = prefix_match.group(1).upper()
                value = prefix_match.group(2)
            for model in value.split("/"):
                model = model.strip().upper()
                if not any(character.isdigit() for character in model):
                    continue
                if re.match(r"^EX\d", model):
                    models.add(model)
                elif current_prefix:
                    models.add(f"{current_prefix}-{model}")
        return sorted(models)

    for prefix, values in MODEL_GROUP_PATTERN.findall(text):
        models.update(
            f"{prefix.upper()}-{value.upper()}"
            for value in values.split("/")
            if any(character.isdigit() for character in value)
        )
    for prefix, values in SHORT_MODEL_GROUP_PATTERN.findall(text):
        models.update(f"{prefix.upper()}{value.upper()}" for value in values.split("/"))
    return sorted(models)


def normalize_manufacturer(value: str) -> str:
    stripped = value.strip()
    return next(
        (name for name in KNOWN_MANUFACTURERS if name.casefold() == stripped.casefold()),
        stripped,
    )


def normalize_models(values: list[str]) -> list[str]:
    return sorted({value.strip().upper() for value in values if value.strip()})
