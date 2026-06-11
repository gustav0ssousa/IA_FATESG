import json

from apps.rag.evaluation import (
    EvaluationCase,
    RAGEvaluator,
    load_dataset,
    write_report,
)
from apps.rag.vector_store import SearchResult


def result(source_name: str, content: str = "conteudo") -> SearchResult:
    return SearchResult(
        chunk_id="chunk",
        document_id="document",
        score=0.9,
        content=content,
        source_name=source_name,
        page_number=None,
        metadata={},
    )


class FakeSearch:
    def search(self, query, top_k):
        return [result("irrelevante.md"), result("esperado.md")][:top_k]


class FakeQuery:
    def answer(self, question, top_k):
        return {
            "answer": "A resposta contem termo esperado [Fonte 1].",
            "sources": [{"source_name": "esperado.md"}],
        }


def case(answerable=True):
    return EvaluationCase(
        id="case-1",
        question="Pergunta?",
        expected_sources=["esperado.md"],
        expected_answer_terms=["termo esperado"],
        answerable=answerable,
    )


def test_evaluator_calculates_retrieval_and_generation_metrics() -> None:
    evaluation = RAGEvaluator(FakeSearch(), FakeQuery()).evaluate([case()], top_k=2)

    summary = evaluation["summary"]
    assert summary["retrieval_hit_rate"] == 1.0
    assert summary["mean_reciprocal_rank"] == 0.5
    assert summary["mean_precision_at_k"] == 0.5
    assert summary["citation_rate"] == 1.0
    assert summary["mean_answer_term_recall"] == 1.0
    assert evaluation["quality_gate"]["passed"] is False
    assert (
        evaluation["quality_gate"]["checks"]["mean_reciprocal_rank"]["passed"]
        is False
    )


def test_evaluator_skips_generation_metrics_when_not_requested() -> None:
    evaluation = RAGEvaluator(FakeSearch()).evaluate([case()], top_k=2)

    assert evaluation["summary"]["citation_rate"] is None
    assert evaluation["cases"][0]["answer"] is None


def test_evaluator_measures_refusal_for_unanswerable_case() -> None:
    class RefusingQuery:
        def answer(self, question, top_k):
            return {"answer": "Nao ha informacao suficiente.", "sources": []}

    evaluation = RAGEvaluator(FakeSearch(), RefusingQuery()).evaluate(
        [case(answerable=False)],
        top_k=2,
    )

    assert evaluation["summary"]["refusal_accuracy"] == 1.0
    assert evaluation["summary"]["retrieval_hit_rate"] is None


def test_evaluator_fails_gate_when_unanswerable_case_is_not_refused() -> None:
    evaluation = RAGEvaluator(FakeSearch(), FakeQuery()).evaluate(
        [case(answerable=False)],
        top_k=2,
    )

    assert evaluation["summary"]["refusal_accuracy"] == 0.0
    assert evaluation["quality_gate"]["checks"]["refusal_accuracy"]["passed"] is False
    assert evaluation["quality_gate"]["passed"] is False


def test_evaluator_records_generation_error_and_fails_gate() -> None:
    class FailingQuery:
        def answer(self, question, top_k):
            raise RuntimeError("provider indisponivel")

    evaluation = RAGEvaluator(FakeSearch(), FailingQuery()).evaluate(
        [case()],
        top_k=2,
    )

    assert evaluation["summary"]["generation_error_count"] == 1
    assert evaluation["quality_gate"]["passed"] is False
    assert "provider indisponivel" in evaluation["cases"][0]["generation_error"]


def test_evaluator_detects_duplicate_results() -> None:
    class DuplicateSearch:
        def search(self, query, top_k):
            return [result("esperado.md"), result("esperado.md")]

    evaluation = RAGEvaluator(DuplicateSearch()).evaluate([case()], top_k=2)

    assert evaluation["summary"]["mean_duplicate_result_rate"] == 0.5
    assert evaluation["quality_gate"]["passed"] is False


def test_dataset_loader_and_report_writer(tmp_path) -> None:
    dataset_path = tmp_path / "dataset.json"
    dataset_path.write_text(
        json.dumps({"cases": [case().__dict__]}),
        encoding="utf-8",
    )
    loaded = load_dataset(dataset_path)
    evaluation = RAGEvaluator(FakeSearch()).evaluate(loaded, top_k=2)

    write_report(evaluation, tmp_path / "report")

    assert loaded[0].id == "case-1"
    assert (tmp_path / "report" / "evaluation.json").exists()
    assert "Retrieval Hit Rate: 1.000" in (
        tmp_path / "report" / "evaluation.md"
    ).read_text()
