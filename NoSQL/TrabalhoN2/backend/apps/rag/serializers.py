from rest_framework import serializers


class SemanticSearchSerializer(serializers.Serializer):
    query = serializers.CharField(max_length=2000)
    top_k = serializers.IntegerField(required=False, min_value=1, max_value=20)


class RAGQuerySerializer(serializers.Serializer):
    question = serializers.CharField(max_length=4000)
    top_k = serializers.IntegerField(required=False, min_value=1, max_value=20)
