from functools import lru_cache

from django.conf import settings
from qdrant_client import QdrantClient

from apps.documents.models import Document, DocumentStatus
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
    def __init__(self, embeddings: EmbeddingProvider, vector_store: VectorStore) -> None:
        self._embeddings = embeddings
        self._vector_store = vector_store

    def search(
        self,
        query: str,
        top_k: int,
        filters: dict | None = None,
    ) -> list[SearchResult]:
        vector = self._embeddings.embed_query(query)
        if filters:
            return self._vector_store.search(vector, top_k, filters=filters)
        return self._vector_store.search(vector, top_k)


class RAGQueryError(RuntimeError):
    pass


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
            if not results:
                return {
                    "answer": "Nao encontrei informacao suficiente nos documentos indexados.",
                    "sources": [],
                    "model": None,
                    "usage": {},
                }

            prompt = self._prompt_builder.build(question, results)
            generation = self._llm.generate(
                prompt.system_instruction,
                prompt.user_prompt,
            )
        except Exception as error:
            raise RAGQueryError(f"Falha ao consultar o RAG: {error}") from error

        return {
            "answer": generation.text,
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
        SemanticSearchService(embeddings, vector_store),
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
