import logging
import hashlib
import time
import uuid
from functools import lru_cache

from django.conf import settings
from django.db import transaction
from django.http import Http404
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.common.permissions import AuthenticatedOrAPIKey, StaffOrAPIKey, has_valid_api_key
from apps.documents.models import Document
from apps.rag.async_indexing import AsyncIndexingError, AsyncIndexingService
from apps.rag.embeddings import FastEmbedProvider
from apps.rag.models import IndexingJob
from apps.rag.models import QueryStatus, RAGQueryRecord, RAGQuerySource
from apps.rag.observability import build_kpi_summary
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

logger = logging.getLogger("adaptive_rag.rag")
TECHNICAL_FILTER_FIELDS = (
    "manufacturer",
    "models",
    "equipment_type",
    "manual_type",
    "content_type",
)


def technical_filters(validated_data: dict) -> dict:
    return {
        key: validated_data[key]
        for key in TECHNICAL_FILTER_FIELDS
        if validated_data.get(key)
    }


def query_audit_fields(request: Request, question: str, filters: dict) -> dict:
    if request.user.is_authenticated:
        authentication_method = "token"
        user = request.user
    elif has_valid_api_key(request):
        authentication_method = "api_key"
        user = None
    else:
        authentication_method = "local_anonymous"
        user = None
    return {
        "user": user,
        "authentication_method": authentication_method,
        "question": question if settings.AUDIT_STORE_QUESTION_TEXT else "",
        "question_hash": hashlib.sha256(question.encode("utf-8")).hexdigest(),
        "filters": filters,
    }


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
    permission_classes = [StaffOrAPIKey]

    def post(self, request: Request, document_id: str) -> Response:
        document = get_object_or_404(Document, id=document_id)
        indexing_service, _ = build_services()
        try:
            chunk_count = indexing_service.index(document)
        except DocumentIndexingError as error:
            logger.warning(
                "document_indexing_failed",
                extra={
                    "event": "document_indexing_failed",
                    "request_id": request._request.request_id,
                    "document_id": str(document.id),
                    "error_type": type(error).__name__,
                },
            )
            return Response(
                {
                    "detail": "Nao foi possivel indexar o documento.",
                    "request_id": request._request.request_id,
                },
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
    permission_classes = [StaffOrAPIKey]

    def post(self, request: Request, document_id: str) -> Response:
        document = get_object_or_404(Document, id=document_id)
        try:
            job = AsyncIndexingService().enqueue(document)
        except AsyncIndexingError as error:
            logger.warning(
                "document_indexing_enqueue_failed",
                extra={
                    "event": "document_indexing_enqueue_failed",
                    "request_id": request._request.request_id,
                    "document_id": str(document.id),
                    "error_type": type(error).__name__,
                },
            )
            return Response(
                {
                    "detail": "O servico de indexacao esta temporariamente indisponivel.",
                    "request_id": request._request.request_id,
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return Response(
            IndexingJobSerializer(job).data,
            status=status.HTTP_202_ACCEPTED,
        )


class IndexingJobDetailView(APIView):
    permission_classes = [AuthenticatedOrAPIKey]

    def get(self, request: Request, job_id: str) -> Response:
        job = get_object_or_404(IndexingJob, id=job_id)
        return Response(IndexingJobSerializer(job).data)


class SemanticSearchView(APIView):
    permission_classes = [AuthenticatedOrAPIKey]

    def post(self, request: Request) -> Response:
        serializer = SemanticSearchSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        _, search_service = build_services()
        query = serializer.validated_data["query"]
        top_k = serializer.validated_data.get("top_k", settings.RAG_TOP_K)
        filters = technical_filters(serializer.validated_data)
        results = (
            search_service.search(query=query, top_k=top_k, filters=filters)
            if filters
            else search_service.search(query=query, top_k=top_k)
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
    permission_classes = [AuthenticatedOrAPIKey]

    def post(self, request: Request) -> Response:
        serializer = RAGQuerySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        request_id = str(uuid.uuid4())
        request._request.request_id = request_id
        question = serializer.validated_data["question"]
        top_k = serializer.validated_data.get("top_k", settings.RAG_TOP_K)
        filters = technical_filters(serializer.validated_data)
        audit_fields = query_audit_fields(request, question, filters)
        started_at = time.perf_counter()
        try:
            query_service = build_rag_query_service()
            result = (
                query_service.answer(question=question, top_k=top_k, filters=filters)
                if filters
                else query_service.answer(question=question, top_k=top_k)
            )
        except (RAGQueryError, ValueError) as error:
            duration_ms = round((time.perf_counter() - started_at) * 1000)
            RAGQueryRecord.objects.create(
                request_id=request_id,
                **audit_fields,
                status=QueryStatus.ERROR,
                top_k=top_k,
                duration_ms=duration_ms,
                error_message=str(error)[:2000],
            )
            logger.warning(
                "rag_query_failed",
                extra={
                    "event": "rag_query_failed",
                    "request_id": request_id,
                    "duration_ms": duration_ms,
                    "question_length": len(question),
                },
            )
            return Response(
                {
                    "detail": "Nao foi possivel concluir a consulta RAG.",
                    "request_id": request_id,
                },
                status=status.HTTP_502_BAD_GATEWAY,
            )
        duration_ms = round((time.perf_counter() - started_at) * 1000)
        with transaction.atomic():
            record = RAGQueryRecord.objects.create(
                request_id=request_id,
                **audit_fields,
                status=QueryStatus.SUCCESS,
                model=result["model"] or "",
                top_k=top_k,
                source_count=len(result["sources"]),
                duration_ms=duration_ms,
                usage=result["usage"],
            )
            RAGQuerySource.objects.bulk_create(
                [
                    RAGQuerySource(
                        query=record,
                        document_id=source["document_id"],
                        chunk_id=source.get("chunk_id", ""),
                        source_name=source["source_name"],
                        page_number=source.get("page_number"),
                        metadata=source.get("metadata", {}),
                        rank=source["number"],
                        score=source["score"],
                    )
                    for source in result["sources"]
                ]
            )
        logger.info(
            "rag_query_completed",
            extra={
                "event": "rag_query_completed",
                "request_id": request_id,
                "duration_ms": duration_ms,
                "source_count": len(result["sources"]),
                "model": result["model"],
                "question_length": len(question),
            },
        )
        return Response({**result, "request_id": request_id})


class KPIOverviewView(APIView):
    permission_classes = [StaffOrAPIKey]

    def get(self, request: Request) -> Response:
        return Response(build_kpi_summary())
