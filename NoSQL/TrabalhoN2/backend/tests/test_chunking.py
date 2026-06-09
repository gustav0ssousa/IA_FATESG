import pytest

from apps.documents.chunking import ChunkingConfig, LangChainTextChunker, normalize_text
from apps.documents.extractors import ExtractedSection


def test_normalize_text_preserves_paragraphs_and_removes_noise() -> None:
    text = "  Primeiro   paragrafo. \r\n\r\n\r\n Segundo\tparagrafo.  "

    assert normalize_text(text) == "Primeiro paragrafo.\n\nSegundo paragrafo."


def test_chunker_preserves_page_metadata_and_sequential_positions() -> None:
    chunker = LangChainTextChunker(ChunkingConfig(chunk_size=30, chunk_overlap=5))
    sections = [
        ExtractedSection(
            text="Primeiro paragrafo com bastante conteudo para dividir. Segundo bloco.",
            page_number=2,
            metadata={"section": "intro"},
        )
    ]

    chunks = chunker.split(sections)

    assert len(chunks) > 1
    assert [chunk["position"] for chunk in chunks] == list(range(len(chunks)))
    assert all(chunk["page_number"] == 2 for chunk in chunks)
    assert all(chunk["metadata"] == {"section": "intro"} for chunk in chunks)


def test_chunking_config_rejects_overlap_greater_than_size() -> None:
    with pytest.raises(ValueError, match="chunk_overlap"):
        ChunkingConfig(chunk_size=100, chunk_overlap=100)
