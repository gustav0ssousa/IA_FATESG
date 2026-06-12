import json
from unittest.mock import patch

from apps.rag.embeddings import RemoteEmbeddingProvider


class JsonResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def request_texts(request) -> list[str]:
    payload = json.loads(request.data.decode())
    return payload["texts"]


def test_remote_embedding_provider_splits_document_batches() -> None:
    calls = []

    def fake_urlopen(request, timeout):
        texts = request_texts(request)
        calls.append((texts, timeout))
        return JsonResponse({"vectors": [[float(len(text))] for text in texts]})

    provider = RemoteEmbeddingProvider(
        url="http://embeddings/api",
        dimension=1,
        timeout_seconds=12,
        batch_size=2,
        max_retries=0,
    )

    with patch("apps.rag.embeddings.urlopen", side_effect=fake_urlopen):
        vectors = provider.embed_documents(["a", "bb", "ccc", "dddd", "eeeee"])

    assert vectors == [[1.0], [2.0], [3.0], [4.0], [5.0]]
    assert calls == [
        (["a", "bb"], 12),
        (["ccc", "dddd"], 12),
        (["eeeee"], 12),
    ]


def test_remote_embedding_provider_retries_timeout_once() -> None:
    attempts = 0

    def fake_urlopen(request, timeout):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("timed out")
        return JsonResponse({"vectors": [[1.0]]})

    provider = RemoteEmbeddingProvider(
        url="http://embeddings/api",
        dimension=1,
        timeout_seconds=12,
        batch_size=2,
        max_retries=1,
        retry_base_delay_seconds=0.5,
    )

    with (
        patch("apps.rag.embeddings.urlopen", side_effect=fake_urlopen),
        patch("apps.rag.embeddings.time.sleep") as sleep_mock,
    ):
        vectors = provider.embed_query("manual")

    assert vectors == [1.0]
    assert attempts == 2
    sleep_mock.assert_called_once_with(0.5)
