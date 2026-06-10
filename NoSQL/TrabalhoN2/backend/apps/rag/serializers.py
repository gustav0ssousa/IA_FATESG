from rest_framework import serializers

from apps.rag.models import IndexingJob
from apps.documents.technical import normalize_manufacturer


class TechnicalFiltersSerializer(serializers.Serializer):
    manufacturer = serializers.CharField(required=False, max_length=100)
    model = serializers.CharField(required=False, max_length=100, source="models")
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
    content_type = serializers.ChoiceField(
        required=False,
        choices=(
            "safety",
            "troubleshooting",
            "error_reference",
            "procedure",
            "specification",
            "maintenance",
            "technical_reference",
        ),
    )

    def validate_manufacturer(self, value: str) -> str:
        return normalize_manufacturer(value)

    def validate_model(self, value: str) -> str:
        return value.strip().upper()


class SemanticSearchSerializer(TechnicalFiltersSerializer):
    query = serializers.CharField(max_length=2000)
    top_k = serializers.IntegerField(required=False, min_value=1, max_value=20)


class RAGQuerySerializer(TechnicalFiltersSerializer):
    question = serializers.CharField(max_length=4000)
    top_k = serializers.IntegerField(required=False, min_value=1, max_value=20)


class EmbeddingRequestSerializer(serializers.Serializer):
    mode = serializers.ChoiceField(choices=("documents", "query"))
    texts = serializers.ListField(
        child=serializers.CharField(max_length=20000),
        min_length=1,
        max_length=100,
    )

    def validate(self, attrs):
        if attrs["mode"] == "query" and len(attrs["texts"]) != 1:
            raise serializers.ValidationError("O modo query aceita exatamente um texto.")
        return attrs


class IndexingJobSerializer(serializers.ModelSerializer):
    document_id = serializers.UUIDField(read_only=True)

    class Meta:
        model = IndexingJob
        fields = (
            "id",
            "document_id",
            "celery_task_id",
            "status",
            "attempts",
            "indexed_chunks",
            "error_message",
            "created_at",
            "started_at",
            "finished_at",
            "updated_at",
        )
        read_only_fields = fields
