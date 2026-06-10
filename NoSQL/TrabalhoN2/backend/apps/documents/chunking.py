import hashlib
import re
from dataclasses import dataclass

from langchain_text_splitters import RecursiveCharacterTextSplitter

from apps.documents.extractors import ExtractedSection
from apps.documents.repositories import ChunkData
from apps.documents.technical import classify_technical_chunk


def normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(normalized_lines)).strip()


@dataclass(frozen=True)
class ChunkingConfig:
    chunk_size: int
    chunk_overlap: int

    def __post_init__(self) -> None:
        if self.chunk_size <= 0:
            raise ValueError("chunk_size deve ser maior que zero.")
        if self.chunk_overlap < 0 or self.chunk_overlap >= self.chunk_size:
            raise ValueError("chunk_overlap deve ser menor que chunk_size.")


class LangChainTextChunker:
    def __init__(self, config: ChunkingConfig) -> None:
        self._splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap,
            separators=["\n\n", "\n", ". ", " ", ""],
        )

    def split(self, sections: list[ExtractedSection]) -> list[ChunkData]:
        chunks: list[ChunkData] = []
        for section in sections:
            normalized = normalize_text(section.text)
            if not normalized:
                continue
            for content in self._splitter.split_text(normalized):
                chunks.append(
                    {
                        "position": len(chunks),
                        "content": content,
                        "content_hash": hashlib.sha256(content.encode()).hexdigest(),
                        "token_count": None,
                        "page_number": section.page_number,
                        "metadata": {
                            **section.metadata,
                            **classify_technical_chunk(
                                "\n".join(
                                    filter(
                                        None,
                                        (
                                            section.metadata.get("chapter", ""),
                                            section.metadata.get("section_heading", ""),
                                            content,
                                        ),
                                    )
                                )
                            ),
                        },
                    }
                )
        return chunks
