from unittest.mock import patch

from rest_framework import status
from rest_framework.test import APIClient

from apps.rag.generation import GenerationResult, MaritacaProvider
from apps.rag.prompting import PromptBuilder
from apps.rag.services import RAGQueryService
from apps.rag.vector_store import SearchResult


def source(content: str = "RAG recupera contexto relevante.") -> SearchResult:
    return SearchResult(
        chunk_id="chunk-1",
        document_id="document-1",
        score=0.98,
        content=content,
        source_name="guia.md",
        page_number=2,
        metadata={"section": "retrieval"},
    )


class FakeSearchService:
    def __init__(self, results):
        self.results = results

    def search(self, query, top_k):
        return self.results[:top_k]


class FakeLLM:
    model_name = "fake-model"

    def __init__(self):
        self.calls = []

    def generate(self, system_instruction, user_prompt):
        self.calls.append((system_instruction, user_prompt))
        return GenerationResult(
            text="O RAG recupera contexto antes da resposta [Fonte 1].",
            model=self.model_name,
            usage={"input_tokens": 50, "output_tokens": 12},
        )


class FakeUsage:
    def model_dump(self):
        return {"input_tokens": 10, "output_tokens": 4}


class FakeResponse:
    output_text = "Resposta fundamentada."
    usage = FakeUsage()


class FakeResponsesAPI:
    def __init__(self):
        self.params = None

    def create(self, **kwargs):
        self.params = kwargs
        return FakeResponse()


class FakeOpenAIClient:
    def __init__(self):
        self.responses = FakeResponsesAPI()


def test_prompt_builder_includes_sources_and_limits_context() -> None:
    prompt = PromptBuilder(max_context_chars=60).build(
        "Como funciona?",
        [source("A" * 100), source("B" * 100)],
    )

    assert "[Fonte 1] guia.md, pagina 2" in prompt.user_prompt
    assert "Como funciona?" in prompt.user_prompt
    assert len(prompt.used_sources) == 1


def test_rag_query_service_returns_answer_and_sources() -> None:
    llm = FakeLLM()
    service = RAGQueryService(
        search_service=FakeSearchService([source()]),
        prompt_builder=PromptBuilder(max_context_chars=1000),
        llm=llm,
    )

    result = service.answer("Como funciona RAG?", top_k=5)

    assert result["model"] == "fake-model"
    assert result["sources"][0]["number"] == 1
    assert result["sources"][0]["source_name"] == "guia.md"
    assert "[Fonte 1]" in result["answer"]
    assert len(llm.calls) == 1


def test_rag_query_service_does_not_call_llm_without_context() -> None:
    llm = FakeLLM()
    service = RAGQueryService(
        search_service=FakeSearchService([]),
        prompt_builder=PromptBuilder(max_context_chars=1000),
        llm=llm,
    )

    result = service.answer("Pergunta sem resposta", top_k=5)

    assert result["model"] is None
    assert result["sources"] == []
    assert llm.calls == []


def test_maritaca_provider_uses_responses_api_parameters() -> None:
    client = FakeOpenAIClient()
    provider = MaritacaProvider(
        api_key="",
        base_url="https://chat.maritaca.ai/api",
        model="sabia-4",
        temperature=0.1,
        max_output_tokens=512,
        timeout_seconds=30,
        max_retries=2,
        client=client,
    )

    result = provider.generate("Instrucao", "Pergunta")

    assert result.text == "Resposta fundamentada."
    assert result.usage["output_tokens"] == 4
    assert client.responses.params == {
        "model": "sabia-4",
        "instructions": "Instrucao",
        "input": "Pergunta",
        "temperature": 0.1,
        "max_output_tokens": 512,
    }


@patch("apps.rag.views.build_rag_query_service")
def test_rag_query_endpoint_returns_request_id_and_sources(build_service_mock) -> None:
    build_service_mock.return_value.answer.return_value = {
        "answer": "Resposta [Fonte 1].",
        "sources": [{"number": 1, "source_name": "guia.md"}],
        "model": "sabia-4",
        "usage": {},
    }

    response = APIClient().post(
        "/api/rag/query",
        {"question": "Como funciona RAG?", "top_k": 3},
        format="json",
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["model"] == "sabia-4"
    assert response.json()["request_id"]


def test_rag_query_endpoint_validates_question() -> None:
    response = APIClient().post("/api/rag/query", {"question": ""}, format="json")

    assert response.status_code == status.HTTP_400_BAD_REQUEST
