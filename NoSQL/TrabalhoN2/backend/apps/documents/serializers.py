from pathlib import Path

from django.conf import settings
from rest_framework import serializers

from apps.documents.extractors import ExtractorRegistry


class DocumentIngestionSerializer(serializers.Serializer):
    file = serializers.FileField()
    title = serializers.CharField(required=False, allow_blank=True, max_length=255)

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
