from rest_framework import status
from django.db.models import Count
from django.shortcuts import get_object_or_404
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.common.permissions import AuthenticatedOrAPIKey, StaffOrAPIKey
from apps.documents.models import Document, DocumentStatus
from apps.documents.serializers import (
    DocumentIngestionSerializer,
    DocumentListQuerySerializer,
    DocumentMetadataSerializer,
    DocumentSummarySerializer,
)
from apps.documents.services import DocumentIngestionService, DocumentMetadataService
from apps.documents.technical import normalize_manufacturer, normalize_models
from apps.rag.async_indexing import AsyncIndexingError, AsyncIndexingService
from apps.rag.serializers import IndexingJobSerializer


class DocumentIngestionView(APIView):
    permission_classes = [StaffOrAPIKey]
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request: Request) -> Response:
        serializer = DocumentIngestionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            metadata = {
                key: value
                for key in (
                    "manufacturer",
                    "equipment_type",
                    "manual_type",
                    "language",
                )
                if (value := serializer.validated_data.get(key))
            }
            if models := serializer.validated_data.get("models"):
                metadata["models"] = normalize_models(models.split(","))
            if manufacturer := metadata.get("manufacturer"):
                metadata["manufacturer"] = normalize_manufacturer(manufacturer)
            result = DocumentIngestionService().stage(
                uploaded_file=serializer.validated_data["file"],
                title=serializer.validated_data.get("title", ""),
                metadata=metadata,
            )
            job = None
            if result.created or result.document.status != DocumentStatus.INDEXED:
                job = AsyncIndexingService().enqueue(result.document)
        except AsyncIndexingError:
            return Response(
                {"detail": "O documento foi salvo, mas o processamento nao pode ser enfileirado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        document = result.document
        return Response(
            {
                "document": DocumentSummarySerializer(
                    document,
                    context={"request": request},
                ).data,
                "job": IndexingJobSerializer(job).data if job else None,
                "duplicate": not result.created,
            },
            status=status.HTTP_202_ACCEPTED if job else status.HTTP_200_OK,
        )


class DocumentListView(APIView):
    permission_classes = [AuthenticatedOrAPIKey]

    def get(self, request: Request) -> Response:
        query = DocumentListQuerySerializer(data=request.query_params)
        query.is_valid(raise_exception=True)
        page = query.validated_data["page"]
        page_size = query.validated_data["page_size"]
        documents = Document.objects.annotate(chunk_count=Count("chunks"))
        total = documents.count()
        start = (page - 1) * page_size
        results = documents[start : start + page_size]
        metadata = Document.objects.values_list("metadata", flat=True)
        manufacturers = sorted(
            {
                item.get("manufacturer")
                for item in metadata
                if item.get("manufacturer")
            }
        )
        models = sorted(
            {
                model
                for item in metadata
                for model in item.get("models", [])
                if model
            }
        )
        return Response(
            {
                "results": DocumentSummarySerializer(results, many=True).data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": max(1, (total + page_size - 1) // page_size),
                },
                "facets": {
                    "manufacturers": manufacturers,
                    "models": models,
                },
            }
        )


class DocumentDetailView(APIView):
    permission_classes = [StaffOrAPIKey]

    def patch(self, request: Request, document_id: str) -> Response:
        document = get_object_or_404(Document, id=document_id)
        serializer = DocumentMetadataSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        values = serializer.validated_data
        metadata = {
            key: value
            for key, value in values.items()
            if key not in ("title", "models", "manufacturer")
        }
        if "models" in values:
            metadata["models"] = normalize_models(values["models"])
        if "manufacturer" in values:
            metadata["manufacturer"] = normalize_manufacturer(values["manufacturer"])
        DocumentMetadataService().update(
            document,
            title=values.get("title"),
            metadata=metadata,
        )
        try:
            job = AsyncIndexingService().enqueue(document) if document.chunks.exists() else None
        except AsyncIndexingError:
            return Response(
                {
                    "detail": (
                        "Os metadados foram salvos, mas a reindexacao nao pode "
                        "ser enfileirada."
                    )
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return Response(
            {
                "document": DocumentSummarySerializer(document).data,
                "job": IndexingJobSerializer(job).data if job else None,
            }
        )


class DocumentReprocessView(APIView):
    permission_classes = [StaffOrAPIKey]

    def post(self, request: Request, document_id: str) -> Response:
        document = get_object_or_404(Document, id=document_id)
        if not document.file:
            return Response(
                {"detail": "O arquivo original nao esta disponivel para reprocessamento."},
                status=status.HTTP_409_CONFLICT,
            )
        document.status = DocumentStatus.PENDING
        document.error_message = ""
        document.save(update_fields=["status", "error_message", "updated_at"])
        try:
            job = AsyncIndexingService().enqueue(document)
        except AsyncIndexingError:
            return Response(
                {"detail": "O reprocessamento nao pode ser enfileirado."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return Response(IndexingJobSerializer(job).data, status=status.HTTP_202_ACCEPTED)
