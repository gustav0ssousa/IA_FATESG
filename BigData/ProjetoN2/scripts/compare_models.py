"""Compare trained ML models and select the best one."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = PROJECT_ROOT / "outputs" / "ml" / "models"
COMPARISON_DIR = PROJECT_ROOT / "outputs" / "ml" / "comparison"

MODEL_PATHS = {
    "Regressao Logistica Multiclasse": MODELS_DIR / "logistic_regression",
    "Random Forest": MODELS_DIR / "random_forest",
}


def load_metrics(model_name: str, model_dir: Path) -> dict:
    metrics_path = model_dir / "metrics.json"
    if not metrics_path.exists():
        raise FileNotFoundError(f"Metrics not found for {model_name}: {metrics_path}")
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    metrics["model_name"] = model_name
    return metrics


def interpretation(row: pd.Series) -> str:
    if row["model_name"] == "Regressao Logistica Multiclasse":
        return (
            "Melhor equilibrio geral nas metricas macro e melhor recall para High; "
            "boa escolha como baseline interpretavel."
        )
    return (
        "Melhor recall para Severe, mas desempenho geral inferior e recall fraco para High."
    )


def main() -> None:
    COMPARISON_DIR.mkdir(parents=True, exist_ok=True)

    rows = [load_metrics(name, path) for name, path in MODEL_PATHS.items()]
    comparison = pd.DataFrame(rows)
    selected_columns = [
        "model_name",
        "algorithm",
        "accuracy",
        "balanced_accuracy",
        "precision_macro",
        "recall_macro",
        "f1_macro",
        "f1_weighted",
        "recall_high",
        "recall_severe",
        "train_rows",
        "test_rows",
        "feature_count",
    ]
    comparison = comparison[selected_columns].sort_values(
        ["f1_macro", "balanced_accuracy", "recall_high"],
        ascending=False,
    )
    comparison["vantagens"] = comparison["model_name"].map(
        {
            "Regressao Logistica Multiclasse": (
                "Mais interpretavel, leve, melhor F1 macro e melhor recall para High."
            ),
            "Random Forest": (
                "Captura nao linearidades, fornece importancia de features e teve maior recall para Severe."
            ),
        }
    )
    comparison["desvantagens"] = comparison["model_name"].map(
        {
            "Regressao Logistica Multiclasse": (
                "Modelo linear, pode perder interacoes complexas e ainda confunde classes intermediarias."
            ),
            "Random Forest": (
                "Mais pesado, menos interpretavel e com pior desempenho geral neste baseline."
            ),
        }
    )
    comparison["interpretacao_desempenho"] = comparison.apply(interpretation, axis=1)

    best = comparison.iloc[0].to_dict()
    best_model_key = (
        "logistic_regression"
        if best["model_name"] == "Regressao Logistica Multiclasse"
        else "random_forest"
    )

    comparison.to_csv(COMPARISON_DIR / "model_comparison.csv", index=False, encoding="utf-8")
    (COMPARISON_DIR / "best_model.json").write_text(
        json.dumps(
            {
                "best_model_name": best["model_name"],
                "best_model_key": best_model_key,
                "selection_criteria": [
                    "f1_macro",
                    "balanced_accuracy",
                    "recall_high",
                    "interpretability",
                    "operational_complexity",
                ],
                "reason": (
                    "A Regressao Logistica Multiclasse teve melhor F1 macro, melhor balanced accuracy, "
                    "melhor acuracia geral e recall superior para a classe High."
                ),
                "metrics": {
                    "accuracy": best["accuracy"],
                    "balanced_accuracy": best["balanced_accuracy"],
                    "f1_macro": best["f1_macro"],
                    "recall_high": best["recall_high"],
                    "recall_severe": best["recall_severe"],
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    best_source = MODELS_DIR / best_model_key / "model.joblib"
    if best_source.exists():
        shutil.copy2(best_source, COMPARISON_DIR / "best_model.joblib")

    summary = [
        "# Comparacao dos algoritmos",
        "",
        f"- Melhor modelo: {best['model_name']}",
        f"- F1 macro: {best['f1_macro']:.4f}",
        f"- Balanced accuracy: {best['balanced_accuracy']:.4f}",
        f"- Recall High: {best['recall_high']:.4f}",
        f"- Recall Severe: {best['recall_severe']:.4f}",
    ]
    (COMPARISON_DIR / "comparison_summary.md").write_text("\n".join(summary), encoding="utf-8")

    print("Comparacao de modelos concluida.")
    print(f"Melhor modelo: {best['model_name']}")
    print(f"F1 macro: {best['f1_macro']:.4f}")


if __name__ == "__main__":
    main()
