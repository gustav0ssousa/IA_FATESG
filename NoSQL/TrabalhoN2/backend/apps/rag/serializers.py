from rest_framework import serializers

from apps.rag.models import IndexingJob


class SemanticSearchSerializer(serializers.Serializer):
    query = serializers.CharField(max_length=2000)
    top_k = serializers.IntegerField(required=False, min_value=1, max_value=20)


class RAGQuerySerializer(serializers.Serializer):
    question = serializers.CharField(max_length=4000)
    top_k = serializers.IntegerField(required=False, min_value=1, max_value=20)


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
