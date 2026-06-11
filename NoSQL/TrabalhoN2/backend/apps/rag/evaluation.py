import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Protocol

from apps.rag.vector_store import SearchResult

DEFAULT_THRESHOLDS = {
    "retrieval_hit_rate": 0.80,
    "mean_reciprocal_rank": 0.70,
    "citation_rate": 0.80,
    "mean_answer_term_recall": 0.60,
    "refusal_accuracy": 1.0,
}


@dataclass(frozen=True)
class EvaluationCase:
    id: str
    question: str
    expected_sources: list[str]
    expected_answer_terms: list[str]
    answerable: bool = True


@dataclass(frozen=True)
class CaseResult:
    id: str
    question: str
    retrieved_sources: list[str]
    retrieval_hit: bool | None
    reciprocal_rank: float | None
    precision_at_k: float | None
    duplicate_result_rate: float
    answer: str | None
    citation_present: bool | None
    answer_term_recall: float | None
    refusal_correct: bool | None
    generation_error: str | None


class EvaluationSearchService(Protocol):
    def search(self, query: str, top_k: int) -> list[SearchResult]: ...


class EvaluationQueryService(Protocol):
    def answer(self, question: str, top_k: int) -> dict: ...


def load_dataset(path: Path) -> list[EvaluationCase]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [EvaluationCase(**item) for item in payload["cases"]]


def _average(values: list[float | bool | None]) -> float | None:
    numeric = [float(value) for value in values if value is not None]
    return mean(numeric) if numeric else None


class RAGEvaluator:
    def __init__(
        self,
        search_service: EvaluationSearchService,
        query_service: EvaluationQueryService | None = None,
    ) -> None:
        self._search_service = search_service
        self._query_service = query_service

    def evaluate(self, cases: list[EvaluationCase], top_k: int) -> dict:
        results = [self._evaluate_case(case, top_k) for case in cases]
        summary = {
            "case_count": len(results),
            "top_k": top_k,
            "retrieval_hit_rate": _average(
                [result.retrieval_hit for result in results]
            ),
            "mean_reciprocal_rank": _average(
                [result.reciprocal_rank for result in results]
            ),
            "mean_precision_at_k": _average(
                [result.precision_at_k for result in results]
            ),
            "mean_duplicate_result_rate": _average(
                [result.duplicate_result_rate for result in results]
            ),
            "citation_rate": _average(
                [result.citation_present for result in results]
            ),
            "mean_answer_term_recall": _average(
                [result.answer_term_recall for result in results]
            ),
            "refusal_accuracy": _average(
                [result.refusal_correct for result in results]
            ),
            "generation_error_count": sum(
                result.generation_error is not None for result in results
            ),
        }
        return {
            "summary": summary,
            "quality_gate": _quality_gate(summary),
            "cases": [asdict(result) for result in results],
        }

    def _evaluate_case(self, case: EvaluationCase, top_k: int) -> CaseResult:
        retrieved = self._search_service.search(case.question, top_k)
        source_names = [result.source_name for result in retrieved]
        signatures = [(result.source_name, result.content) for result in retrieved]
        duplicate_result_rate = (
            (len(signatures) - len(set(signatures))) / len(signatures)
            if signatures
            else 0.0
        )

        retrieval_hit = None
        reciprocal_rank = None
        precision_at_k = None
        if case.answerable:
            relevant_ranks = [
                index
                for index, source_name in enumerate(source_names, start=1)
                if source_name in case.expected_sources
            ]
            retrieval_hit = bool(relevant_ranks)
            reciprocal_rank = 1 / relevant_ranks[0] if relevant_ranks else 0.0
            precision_at_k = len(relevant_ranks) / len(source_names) if source_names else 0.0

        answer = None
        citation_present = None
        answer_term_recall = None
        refusal_correct = None
        generation_error = None
        if self._query_service:
            try:
                generated = self._query_service.answer(case.question, top_k)
                answer = generated["answer"]
                if case.answerable:
                    citation_present = bool(re.search(r"\[Fonte \d+\]", answer))
                    normalized_answer = answer.casefold()
                    matches = sum(
                        term.casefold() in normalized_answer
                        for term in case.expected_answer_terms
                    )
                    answer_term_recall = (
                        matches / len(case.expected_answer_terms)
                        if case.expected_answer_terms
                        else None
                    )
                else:
                    refusal_correct = not generated["sources"]
            except Exception as error:
                generation_error = str(error)

        return CaseResult(
            id=case.id,
            question=case.question,
            retrieved_sources=source_names,
            retrieval_hit=retrieval_hit,
            reciprocal_rank=reciprocal_rank,
            precision_at_k=precision_at_k,
            duplicate_result_rate=duplicate_result_rate,
            answer=answer,
            citation_present=citation_present,
            answer_term_recall=answer_term_recall,
            refusal_correct=refusal_correct,
            generation_error=generation_error,
        )


def write_report(result: dict, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "evaluation.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary = result["summary"]
    gate = result["quality_gate"]
    lines = [
        "# Relatorio de Avaliacao RAG",
        "",
        f"- Quality Gate: {'APROVADO' if gate['passed'] else 'REPROVADO'}",
        f"- Casos: {summary['case_count']}",
        f"- Top-k: {summary['top_k']}",
        f"- Retrieval Hit Rate: {_format_metric(summary['retrieval_hit_rate'])}",
        f"- Mean Reciprocal Rank: {_format_metric(summary['mean_reciprocal_rank'])}",
        f"- Mean Precision@k: {_format_metric(summary['mean_precision_at_k'])}",
        f"- Mean Duplicate Result Rate: {_format_metric(summary['mean_duplicate_result_rate'])}",
        f"- Citation Rate: {_format_metric(summary['citation_rate'])}",
        f"- Answer Term Recall: {_format_metric(summary['mean_answer_term_recall'])}",
        f"- Refusal Accuracy: {_format_metric(summary['refusal_accuracy'])}",
        f"- Generation Errors: {summary['generation_error_count']}",
        "",
        "## Criterios",
        "",
        *[
            (
                f"- {name}: {_format_metric(check['value'])} "
                f"{check['operator']} {check['threshold']:.3f} "
                f"({'OK' if check['passed'] else 'FALHOU'})"
            )
            for name, check in gate["checks"].items()
        ],
    ]
    (output_dir / "evaluation.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _format_metric(value: float | None) -> str:
    return "nao avaliado" if value is None else f"{value:.3f}"


def _quality_gate(summary: dict) -> dict:
    checks = {
        name: {
            "value": summary[name],
            "threshold": threshold,
            "operator": ">=",
            "passed": summary[name] is None or summary[name] >= threshold,
        }
        for name, threshold in DEFAULT_THRESHOLDS.items()
    }
    checks["generation_error_count"] = {
        "value": summary["generation_error_count"],
        "threshold": 0,
        "operator": "==",
        "passed": summary["generation_error_count"] == 0,
    }
    checks["mean_duplicate_result_rate"] = {
        "value": summary["mean_duplicate_result_rate"],
        "threshold": 0,
        "operator": "==",
        "passed": summary["mean_duplicate_result_rate"] == 0,
    }
    return {
        "passed": all(check["passed"] for check in checks.values()),
        "checks": checks,
    }
