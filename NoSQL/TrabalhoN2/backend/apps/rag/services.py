import re
import uuid
from functools import lru_cache

from django.conf import settings
from django.db.models import Q
from qdrant_client import QdrantClient

from apps.documents.models import Document, DocumentChunk, DocumentStatus
from apps.documents.technical import KNOWN_MANUFACTURERS, extract_models
from apps.rag.embeddings import EmbeddingProvider, FastEmbedProvider, RemoteEmbeddingProvider
from apps.rag.generation import LLMProvider, MaritacaProvider
from apps.rag.prompting import PromptBuilder
from apps.rag.vector_store import QdrantVectorStore, SearchResult, VectorStore


class DocumentIndexingError(ValueError):
    pass


class DocumentIndexingService:
    def __init__(self, embeddings: EmbeddingProvider, vector_store: VectorStore) -> None:
        self._embeddings = embeddings
        self._vector_store = vector_store

    def index(self, document: Document) -> int:
        chunks = list(document.chunks.select_related("document").all())
        if not chunks:
            raise DocumentIndexingError("O documento nao possui chunks para indexar.")
        try:
            vectors = self._embeddings.embed_documents([chunk.content for chunk in chunks])
            self._vector_store.index_chunks(chunks, vectors)
        except Exception as error:
            document.status = DocumentStatus.FAILED
            document.error_message = f"Falha na indexacao vetorial: {error}"
            document.save(update_fields=["status", "error_message", "updated_at"])
            raise DocumentIndexingError(document.error_message) from error
        document.status = DocumentStatus.INDEXED
        document.error_message = ""
        document.save(update_fields=["status", "error_message", "updated_at"])
        return len(chunks)


class SemanticSearchService:
    def __init__(
        self,
        embeddings: EmbeddingProvider,
        vector_store: VectorStore,
        min_relevance_score: float = 0.0,
    ) -> None:
        if not -1.0 <= min_relevance_score <= 1.0:
            raise ValueError("O score minimo de relevancia deve estar entre -1 e 1.")
        self._embeddings = embeddings
        self._vector_store = vector_store
        self._min_relevance_score = min_relevance_score

    def search(
        self,
        query: str,
        top_k: int,
        filters: dict | None = None,
    ) -> list[SearchResult]:
        vector = self._embeddings.embed_query(query)
        results = (
            self._vector_store.search(vector, top_k, filters=filters)
            if filters
            else self._vector_store.search(vector, top_k)
        )
        return [
            result for result in results if result.score >= self._min_relevance_score
        ]


class RAGQueryError(RuntimeError):
    pass


def _matches_explicit_equipment_scope(
    question: str,
    results: list[SearchResult],
) -> bool:
    requested_manufacturers = {
        manufacturer.casefold()
        for manufacturer in KNOWN_MANUFACTURERS
        if re.search(rf"\b{re.escape(manufacturer)}\b", question, re.IGNORECASE)
    }
    available_manufacturers = {
        str(result.metadata.get("manufacturer", "")).casefold()
        for result in results
        if result.metadata.get("manufacturer")
    }
    if requested_manufacturers and requested_manufacturers.isdisjoint(
        available_manufacturers
    ):
        return False

    requested_models = set(extract_models(question))
    available_models = {
        str(model).upper()
        for result in results
        for model in result.metadata.get("models", [])
    }
    return not requested_models or not requested_models.isdisjoint(available_models)


def _expand_adjacent_context(results: list[SearchResult]) -> list[SearchResult]:
    valid_ids = []
    for result in results:
        try:
            valid_ids.append(uuid.UUID(result.chunk_id))
        except (TypeError, ValueError, AttributeError):
            continue
    anchors = {
        str(chunk.id): chunk
        for chunk in DocumentChunk.objects.select_related("document").filter(id__in=valid_ids)
    }
    neighbor_filter = Q()
    for chunk in anchors.values():
        positions = [position for position in (chunk.position - 1, chunk.position + 1) if position >= 0]
        if positions:
            neighbor_filter |= Q(
                document_id=chunk.document_id,
                page_number=chunk.page_number,
                position__in=positions,
            )
    neighbors_by_anchor: dict[str, list[DocumentChunk]] = {}
    if neighbor_filter:
        neighbors = DocumentChunk.objects.select_related("document").filter(neighbor_filter)
        for anchor_id, anchor in anchors.items():
            neighbors_by_anchor[anchor_id] = [
                chunk
                for chunk in neighbors
                if chunk.document_id == anchor.document_id
                and chunk.page_number == anchor.page_number
                and abs(chunk.position - anchor.position) == 1
            ]

    expanded: list[SearchResult] = []
    seen = set()
    for result in results:
        if result.chunk_id not in seen:
            expanded.append(result)
            seen.add(result.chunk_id)
        for chunk in sorted(
            neighbors_by_anchor.get(result.chunk_id, []),
            key=lambda item: item.position,
        ):
            chunk_id = str(chunk.id)
            if chunk_id in seen:
                continue
            expanded.append(
                SearchResult(
                    chunk_id=chunk_id,
                    document_id=str(chunk.document_id),
                    score=result.score,
                    content=chunk.content,
                    source_name=chunk.document.source_name,
                    page_number=chunk.page_number,
                    metadata=chunk.metadata,
                )
            )
            seen.add(chunk_id)
    return expanded


def _normalize_citations(answer: str, source_count: int) -> str:
    def replace(match: re.Match) -> str:
        numbers = [
            int(value)
            for value in re.findall(r"Fonte\s+(\d+)", match.group(0), re.IGNORECASE)
        ]
        valid_numbers = list(dict.fromkeys(
            number for number in numbers if 1 <= number <= source_count
        ))
        return "".join(f"[Fonte {number}]" for number in valid_numbers) or match.group(0)

    return re.sub(r"\[[^\]]*Fonte\s+\d+[^\]]*\]", replace, answer, flags=re.IGNORECASE)


class RAGQueryService:
    def __init__(
        self,
        search_service: SemanticSearchService,
        prompt_builder: PromptBuilder,
        llm: LLMProvider,
    ) -> None:
        self._search_service = search_service
        self._prompt_builder = prompt_builder
        self._llm = llm

    def answer(self, question: str, top_k: int, filters: dict | None = None) -> dict:
        try:
            results = (
                self._search_service.search(question, top_k, filters=filters)
                if filters
                else self._search_service.search(question, top_k)
            )
            if not results or not _matches_explicit_equipment_scope(question, results):
                return {
                    "answer": "Nao encontrei informacao suficiente nos documentos indexados.",
                    "sources": [],
                    "model": None,
                    "usage": {},
                }

            results = _expand_adjacent_context(results)
            prompt = self._prompt_builder.build(question, results)
            generation = self._llm.generate(
                prompt.system_instruction,
                prompt.user_prompt,
            )
        except Exception as error:
            raise RAGQueryError(f"Falha ao consultar o RAG: {error}") from error

        return {
            "answer": _normalize_citations(generation.text, len(prompt.used_sources)),
            "sources": [
                {
                    "number": index,
                    "chunk_id": source.chunk_id,
                    "document_id": source.document_id,
                    "score": source.score,
                    "content": source.content,
                    "source_name": source.source_name,
                    "page_number": source.page_number,
                    "metadata": source.metadata,
                }
                for index, source in enumerate(prompt.used_sources, start=1)
            ],
            "model": generation.model,
            "usage": generation.usage,
        }


@lru_cache(maxsize=1)
def build_services() -> tuple[DocumentIndexingService, SemanticSearchService]:
    if settings.EMBEDDING_SERVICE_URL:
        embeddings = RemoteEmbeddingProvider(
            url=settings.EMBEDDING_SERVICE_URL,
            dimension=settings.EMBEDDING_DIMENSION,
            timeout_seconds=settings.EMBEDDING_SERVICE_TIMEOUT_SECONDS,
        )
    else:
        embeddings = FastEmbedProvider(
            model_name=settings.EMBEDDING_MODEL,
            dimension=settings.EMBEDDING_DIMENSION,
            cache_dir=settings.EMBEDDING_CACHE_DIR,
            threads=settings.EMBEDDING_THREADS,
            enable_cpu_mem_arena=settings.EMBEDDING_ENABLE_CPU_MEM_ARENA,
        )
    vector_store = QdrantVectorStore(
        client=QdrantClient(url=settings.QDRANT_URL),
        collection_name=settings.QDRANT_COLLECTION,
        vector_size=embeddings.dimension,
    )
    return (
        DocumentIndexingService(embeddings, vector_store),
        SemanticSearchService(
            embeddings,
            vector_store,
            min_relevance_score=settings.RAG_MIN_RELEVANCE_SCORE,
        ),
    )


@lru_cache(maxsize=1)
def build_rag_query_service() -> RAGQueryService:
    _, search_service = build_services()
    llm = MaritacaProvider(
        api_key=settings.MARITACA_API_KEY,
        base_url=settings.MARITACA_BASE_URL,
        model=settings.MARITACA_MODEL,
        temperature=settings.MARITACA_TEMPERATURE,
        max_output_tokens=settings.MARITACA_MAX_OUTPUT_TOKENS,
        timeout_seconds=settings.MARITACA_TIMEOUT_SECONDS,
        max_retries=settings.MARITACA_MAX_RETRIES,
    )
    return RAGQueryService(
        search_service=search_service,
        prompt_builder=PromptBuilder(settings.RAG_MAX_CONTEXT_CHARS),
        llm=llm,
    )
