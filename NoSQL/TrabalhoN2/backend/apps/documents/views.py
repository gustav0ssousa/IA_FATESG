from rest_framework import status
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.documents.serializers import DocumentIngestionSerializer
from apps.documents.services import DocumentIngestionError, DocumentIngestionService


class DocumentIngestionView(APIView):
    authentication_classes = []
    permission_classes = []
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request: Request) -> Response:
        serializer = DocumentIngestionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            result = DocumentIngestionService().ingest(
                uploaded_file=serializer.validated_data["file"],
                title=serializer.validated_data.get("title", ""),
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
                "duplicate": not result.created,
            },
            status=status.HTTP_201_CREATED if result.created else status.HTTP_200_OK,
        )
