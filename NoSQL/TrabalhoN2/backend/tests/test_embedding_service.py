from unittest.mock import patch

from django.test import override_settings
from rest_framework import status
from rest_framework.test import APIClient


class FakeEmbeddingProvider:
    dimension = 2

    def embed_documents(self, texts):
        return [[float(index), 1.0] for index, _ in enumerate(texts)]

    def embed_query(self, text):
        return [0.5, 1.0]


def test_internal_embedding_endpoint_is_disabled_by_default() -> None:
    response = APIClient().post(
        "/api/rag/internal/embeddings",
        {"mode": "query", "texts": ["RAG"]},
        format="json",
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND


@override_settings(EMBEDDING_SERVICE_ENABLED=True)
@patch(
    "apps.rag.views.build_local_embedding_provider",
    return_value=FakeEmbeddingProvider(),
)
def test_internal_embedding_endpoint_returns_vectors(provider_mock) -> None:
    response = APIClient().post(
        "/api/rag/internal/embeddings",
        {"mode": "documents", "texts": ["primeiro", "segundo"]},
        format="json",
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "vectors": [[0.0, 1.0], [1.0, 1.0]],
        "dimension": 2,
    }
