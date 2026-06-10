import uuid
from functools import lru_cache

from django.conf import settings
from django.http import Http404
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.documents.models import Document
from apps.rag.async_indexing import AsyncIndexingError, AsyncIndexingService
from apps.rag.embeddings import FastEmbedProvider
from apps.rag.models import IndexingJob
from apps.rag.serializers import (
    IndexingJobSerializer,
    EmbeddingRequestSerializer,
    RAGQuerySerializer,
    SemanticSearchSerializer,
)
from apps.rag.services import (
    DocumentIndexingError,
    RAGQueryError,
    build_rag_query_service,
    build_services,
)


@lru_cache(maxsize=1)
def build_local_embedding_provider() -> FastEmbedProvider:
    return FastEmbedProvider(
        model_name=settings.EMBEDDING_MODEL,
        dimension=settings.EMBEDDING_DIMENSION,
        cache_dir=settings.EMBEDDING_CACHE_DIR,
        threads=settings.EMBEDDING_THREADS,
        enable_cpu_mem_arena=settings.EMBEDDING_ENABLE_CPU_MEM_ARENA,
    )


class InternalEmbeddingView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request: Request) -> Response:
        if not settings.EMBEDDING_SERVICE_ENABLED:
            raise Http404
        serializer = EmbeddingRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        provider = build_local_embedding_provider()
        texts = serializer.validated_data["texts"]
        if serializer.validated_data["mode"] == "query":
            vectors = [provider.embed_query(texts[0])]
        else:
            vectors = provider.embed_documents(texts)
        return Response({"vectors": vectors, "dimension": provider.dimension})


class DocumentIndexView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request: Request, document_id: str) -> Response:
        document = get_object_or_404(Document, id=document_id)
        indexing_service, _ = build_services()
        try:
            chunk_count = indexing_service.index(document)
        except DocumentIndexingError as error:
            return Response(
                {"detail": str(error)},
                status=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )
        return Response(
            {
                "document_id": str(document.id),
                "status": document.status,
                "indexed_chunks": chunk_count,
            }
        )


class AsyncDocumentIndexView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request: Request, document_id: str) -> Response:
        document = get_object_or_404(Document, id=document_id)
        try:
            job = AsyncIndexingService().enqueue(document)
        except AsyncIndexingError as error:
            return Response(
                {"detail": str(error)},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return Response(
            IndexingJobSerializer(job).data,
            status=status.HTTP_202_ACCEPTED,
        )


class IndexingJobDetailView(APIView):
    authentication_classes = []
    permission_classes = []

    def get(self, request: Request, job_id: str) -> Response:
        job = get_object_or_404(IndexingJob, id=job_id)
        return Response(IndexingJobSerializer(job).data)


class SemanticSearchView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request: Request) -> Response:
        serializer = SemanticSearchSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        _, search_service = build_services()
        results = search_service.search(
            query=serializer.validated_data["query"],
            top_k=serializer.validated_data.get("top_k", settings.RAG_TOP_K),
        )
        return Response(
            {
                "query": serializer.validated_data["query"],
                "results": [
                    {
                        "chunk_id": result.chunk_id,
                        "document_id": result.document_id,
                        "score": result.score,
                        "content": result.content,
                        "source_name": result.source_name,
                        "page_number": result.page_number,
                        "metadata": result.metadata,
                    }
                    for result in results
                ],
            }
        )


class RAGQueryView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request: Request) -> Response:
        serializer = RAGQuerySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        request_id = str(uuid.uuid4())
        try:
            result = build_rag_query_service().answer(
                question=serializer.validated_data["question"],
                top_k=serializer.validated_data.get("top_k", settings.RAG_TOP_K),
            )
        except (RAGQueryError, ValueError) as error:
            return Response(
                {"detail": str(error), "request_id": request_id},
                status=status.HTTP_502_BAD_GATEWAY,
            )
        return Response({**result, "request_id": request_id})
