from rest_framework import status
from django.db.models import Count
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.common.permissions import AuthenticatedOrAPIKey, StaffOrAPIKey
from apps.documents.models import Document
from apps.documents.serializers import DocumentIngestionSerializer, DocumentSummarySerializer
from apps.documents.services import DocumentIngestionError, DocumentIngestionService
from apps.documents.technical import normalize_manufacturer, normalize_models


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
            result = DocumentIngestionService().ingest(
                uploaded_file=serializer.validated_data["file"],
                title=serializer.validated_data.get("title", ""),
                metadata=metadata,
            )
        except DocumentIngestionError as error:
            return Response(
                {"detail": str(error)},
                status=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )

        document = result.document
        return Response(
            {
                "id": str(document.id),
                "title": document.title,
                "source_name": document.source_name,
                "source_type": document.source_type,
                "status": document.status,
                "chunk_count": document.chunks.count(),
                "metadata": document.metadata,
                "duplicate": not result.created,
            },
            status=status.HTTP_201_CREATED if result.created else status.HTTP_200_OK,
        )


class DocumentListView(APIView):
    permission_classes = [AuthenticatedOrAPIKey]

    def get(self, request: Request) -> Response:
        documents = Document.objects.annotate(chunk_count=Count("chunks"))[:100]
        return Response(DocumentSummarySerializer(documents, many=True).data)
