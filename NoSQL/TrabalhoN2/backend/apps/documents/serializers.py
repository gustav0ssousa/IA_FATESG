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
    chunk_count = serializers.IntegerField(read_only=True)

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
