from pathlib import Path

from django.conf import settings
from rest_framework import serializers

from apps.documents.extractors import ExtractorRegistry
from apps.documents.models import Document


class DocumentIngestionSerializer(serializers.Serializer):
    file = serializers.FileField()
    title = serializers.CharField(required=False, allow_blank=True, max_length=255)
    manufacturer = serializers.CharField(required=False, allow_blank=True, max_length=100)
    models = serializers.CharField(required=False, allow_blank=True, max_length=1000)
    equipment_type = serializers.ChoiceField(
        required=False,
        choices=("printer", "scanner", "multifunction", "other"),
    )
    manual_type = serializers.ChoiceField(
        required=False,
        choices=(
            "service_manual",
            "user_manual",
            "installation_manual",
            "parts_catalog",
            "technical_document",
        ),
    )
    language = serializers.CharField(required=False, allow_blank=True, max_length=20)

    def validate_file(self, uploaded_file):
        extension = Path(uploaded_file.name).suffix.lower()
        if extension not in ExtractorRegistry.supported_extensions():
            raise serializers.ValidationError(
                "Formato nao suportado. Use arquivos .txt, .md ou .pdf."
            )
        if uploaded_file.size > settings.DOCUMENT_MAX_UPLOAD_SIZE:
            raise serializers.ValidationError("Arquivo excede o tamanho maximo permitido.")
        if uploaded_file.size == 0:
            raise serializers.ValidationError("O arquivo esta vazio.")
        return uploaded_file


class DocumentSummarySerializer(serializers.ModelSerializer):
    chunk_count = serializers.SerializerMethodField()

    def get_chunk_count(self, document: Document) -> int:
        annotated_count = getattr(document, "chunk_count", None)
        return annotated_count if annotated_count is not None else document.chunks.count()

    class Meta:
        model = Document
        fields = (
            "id",
            "title",
            "source_name",
            "source_type",
            "status",
            "metadata",
            "chunk_count",
            "created_at",
            "updated_at",
        )
        read_only_fields = fields


class DocumentListQuerySerializer(serializers.Serializer):
    page = serializers.IntegerField(required=False, min_value=1, default=1)
    page_size = serializers.IntegerField(
        required=False,
        min_value=1,
        max_value=100,
        default=25,
    )


class DocumentMetadataSerializer(serializers.Serializer):
    title = serializers.CharField(required=False, max_length=255)
    manufacturer = serializers.CharField(required=False, allow_blank=True, max_length=100)
    models = serializers.ListField(
        required=False,
        child=serializers.CharField(max_length=100),
        max_length=100,
    )
    equipment_type = serializers.ChoiceField(
        required=False,
        choices=("printer", "scanner", "multifunction", "other"),
    )
    manual_type = serializers.ChoiceField(
        required=False,
        choices=(
            "service_manual",
            "user_manual",
            "installation_manual",
            "parts_catalog",
            "technical_document",
        ),
    )
    language = serializers.CharField(required=False, allow_blank=True, max_length=20)
