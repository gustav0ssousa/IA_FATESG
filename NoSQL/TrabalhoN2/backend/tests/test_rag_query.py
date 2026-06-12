from unittest.mock import patch

import pytest
from rest_framework import status
from rest_framework.test import APIClient

from apps.documents.models import Document, DocumentChunk
from apps.rag.generation import GenerationResult, MaritacaProvider
from apps.rag.prompting import PromptBuilder
from apps.rag.services import RAGQueryService, _normalize_citations
from apps.rag.vector_store import SearchResult

pytestmark = pytest.mark.django_db


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


def test_prompt_builder_requires_exact_citations_and_technical_terms() -> None:
    prompt = PromptBuilder(max_context_chars=1000).build(
        "O que verificar?",
        [source("Use papel recomendado e verifique se esta umido.")],
    )

    assert "exatamente o formato [Fonte N]" in prompt.system_instruction
    assert "Preserve os termos tecnicos essenciais" in prompt.system_instruction


def test_citation_normalization_enforces_public_contract() -> None:
    answer = (
        "Use o procedimento [Fonte 1, Fonte 2]. "
        "Consulte a pagina [Fonte 2, pagina 31] e ignore [Fonte 9]."
    )

    assert _normalize_citations(answer, source_count=2) == (
        "Use o procedimento [Fonte 1][Fonte 2]. "
        "Consulte a pagina [Fonte 2] e ignore [Fonte 9]."
    )


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


def test_rag_query_service_expands_adjacent_chunk_on_same_page() -> None:
    document = Document.objects.create(
        title="Manual",
        source_name="manual.pdf",
        source_type="pdf",
        content_hash="a" * 64,
    )
    anchor = DocumentChunk.objects.create(
        document=document,
        position=0,
        page_number=31,
        content="Checks before commencing troubleshooting.",
        content_hash="b" * 64,
    )
    adjacent = DocumentChunk.objects.create(
        document=document,
        position=1,
        page_number=31,
        content="A recommended type of paper is being used. The paper is not damp.",
        content_hash="c" * 64,
    )
    DocumentChunk.objects.create(
        document=document,
        position=2,
        page_number=32,
        content="Outra pagina.",
        content_hash="d" * 64,
    )
    llm = FakeLLM()
    search_result = SearchResult(
        chunk_id=str(anchor.id),
        document_id=str(document.id),
        score=0.9,
        content=anchor.content,
        source_name=document.source_name,
        page_number=anchor.page_number,
        metadata={},
    )
    service = RAGQueryService(
        search_service=FakeSearchService([search_result]),
        prompt_builder=PromptBuilder(max_context_chars=2000),
        llm=llm,
    )

    result = service.answer("O que verificar no papel?", top_k=5)

    assert str(adjacent.id) in [item["chunk_id"] for item in result["sources"]]
    assert "recommended type of paper" in llm.calls[0][1]
    assert "Outra pagina" not in llm.calls[0][1]


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


def test_rag_query_service_refuses_incompatible_explicit_manufacturer() -> None:
    llm = FakeLLM()
    brother_source = source()
    brother_source = SearchResult(
        **{
            **brother_source.__dict__,
            "metadata": {
                "manufacturer": "Brother",
                "models": ["MFC-L5710DN"],
            },
        }
    )
    service = RAGQueryService(
        search_service=FakeSearchService([brother_source]),
        prompt_builder=PromptBuilder(max_context_chars=1000),
        llm=llm,
    )

    result = service.answer(
        "Como substituir a cabeca de impressao da Epson L3250?",
        top_k=5,
    )

    assert result["sources"] == []
    assert result["model"] is None
    assert llm.calls == []


def test_rag_query_service_refuses_incompatible_explicit_model() -> None:
    llm = FakeLLM()
    brother_source = SearchResult(
        **{
            **source().__dict__,
            "metadata": {
                "manufacturer": "Brother",
                "models": ["MFC-L5710DN"],
            },
        }
    )
    service = RAGQueryService(
        search_service=FakeSearchService([brother_source]),
        prompt_builder=PromptBuilder(max_context_chars=1000),
        llm=llm,
    )

    result = service.answer("Como reparar a Brother MFC-L9999DW?", top_k=5)

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
        "sources": [
            {
                "number": 1,
                "document_id": "document-1",
                "source_name": "guia.md",
                "score": 0.98,
            }
        ],
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
