"""Define the Machine Learning problem and export reusable metadata."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATISTICS_DIR = PROJECT_ROOT / "outputs" / "statistics"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "ml"

TARGET_COLUMN = "burnout_level"
PROBLEM_TYPE = "classification_multiclass"

PRIMARY_PREDICTORS = [
    "job_role",
    "seniority_level",
    "years_experience",
    "years_at_company",
    "company_size",
    "industry",
    "work_mode",
    "salary_usd",
    "work_hours_per_week",
    "meetings_per_day",
    "team_size",
    "sleep_hours_per_night",
    "exercise_days_per_week",
    "vacation_days_taken",
    "therapy_access",
    "uses_therapy",
    "ai_tools_daily",
    "manager_support_score",
    "work_life_balance_score",
    "job_satisfaction_score",
    "social_support_score",
    "deadline_pressure_score",
    "autonomy_score",
]

FAIRNESS_AUDIT_COLUMNS = [
    "age",
    "gender",
    "country",
]

LEAKAGE_COLUMNS = [
    "employee_id",
    "burnout_score",
    "stress_score",
    "phq9_score",
    "phq9_category",
    "gad7_score",
    "gad7_category",
    "seeks_mental_health_support",
    "job_change_intention",
]

FUTURE_SCENARIO_COLUMNS = [
    "stress_score",
    "phq9_score",
    "gad7_score",
]


def load_target_distribution() -> pd.DataFrame:
    frequencies = pd.read_csv(STATISTICS_DIR / "categorical_frequencies.csv")
    target = frequencies[frequencies["coluna"] == TARGET_COLUMN].copy()
    target = target[
        [
            "categoria",
            "frequencia_absoluta",
            "frequencia_relativa_pct",
            "ranking",
        ]
    ].rename(
        columns={
            "categoria": "classe",
            "frequencia_absoluta": "quantidade",
            "frequencia_relativa_pct": "percentual",
        }
    )
    return target.sort_values("ranking")


def load_relevant_correlations() -> pd.DataFrame:
    correlations = pd.read_csv(STATISTICS_DIR / "correlation_long.csv")
    relevant = correlations[
        correlations["variavel_1"].isin(["burnout_score", "stress_score"])
        | correlations["variavel_2"].isin(["burnout_score", "stress_score"])
    ].copy()
    relevant["abs_correlacao"] = relevant["correlacao"].abs()
    relevant["par"] = relevant.apply(
        lambda row: " | ".join(sorted([row["variavel_1"], row["variavel_2"]])),
        axis=1,
    )
    relevant = relevant.drop_duplicates("par").sort_values("abs_correlacao", ascending=False)
    return relevant.head(15)


def write_csv(items: list[str], filename: str, item_column: str) -> None:
    df = pd.DataFrame({item_column: items})
    df.to_csv(OUTPUT_DIR / filename, index=False, encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    target_distribution = load_target_distribution()
    relevant_correlations = load_relevant_correlations()

    predictors_df = pd.DataFrame(
        {
            "coluna": PRIMARY_PREDICTORS,
            "uso": "preditor_principal",
        }
    )
    fairness_df = pd.DataFrame(
        {
            "coluna": FAIRNESS_AUDIT_COLUMNS,
            "uso": "auditoria_fairness_e_segmentacao",
        }
    )
    leakage_df = pd.DataFrame(
        {
            "coluna": LEAKAGE_COLUMNS,
            "motivo": [
                "Identificador unico, sem valor preditivo generalizavel.",
                "Deriva diretamente o nivel de burnout e causaria vazamento.",
                "Resultado psicologico muito proximo do alvo; usar apenas em cenario alternativo.",
                "Instrumento clinico correlato; pode representar vazamento conceitual.",
                "Categoria derivada de phq9_score.",
                "Instrumento clinico correlato; pode representar vazamento conceitual.",
                "Categoria derivada de gad7_score.",
                "Pode ser consequencia do estado de saude mental.",
                "Pode ser consequencia de burnout e insatisfacao.",
            ],
        }
    )

    target_distribution.to_csv(OUTPUT_DIR / "target_distribution.csv", index=False, encoding="utf-8")
    relevant_correlations.to_csv(OUTPUT_DIR / "relevant_correlations_for_ml.csv", index=False, encoding="utf-8")
    predictors_df.to_csv(OUTPUT_DIR / "primary_predictors.csv", index=False, encoding="utf-8")
    fairness_df.to_csv(OUTPUT_DIR / "fairness_audit_columns.csv", index=False, encoding="utf-8")
    leakage_df.to_csv(OUTPUT_DIR / "leakage_columns.csv", index=False, encoding="utf-8")

    problem_definition = {
        "target_column": TARGET_COLUMN,
        "problem_type": PROBLEM_TYPE,
        "target_description": "Nivel categorico de burnout: Low, Moderate, High ou Severe.",
        "business_goal": "Antecipar risco de burnout para apoiar intervencoes de saude mental no ambiente tech.",
        "primary_predictors": PRIMARY_PREDICTORS,
        "fairness_audit_columns": FAIRNESS_AUDIT_COLUMNS,
        "leakage_columns": LEAKAGE_COLUMNS,
        "future_scenario_columns": FUTURE_SCENARIO_COLUMNS,
        "recommended_metric_focus": [
            "macro_f1",
            "balanced_accuracy",
            "recall_for_high_and_severe_classes",
        ],
        "notes": [
            "A versao primaria evita variaveis que medem diretamente sintomas psicologicos proximos do alvo.",
            "As colunas de auditoria podem ser usadas para avaliar vieses e diferencas de desempenho entre grupos.",
            "Um segundo cenario pode ser criado futuramente incluindo stress_score, phq9_score e gad7_score se esses dados forem considerados disponiveis antes da predicao.",
        ],
    }

    (OUTPUT_DIR / "ml_problem_definition.json").write_text(
        json.dumps(problem_definition, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = [
        "# Definicao do problema de Machine Learning",
        "",
        f"- Variavel-alvo: `{TARGET_COLUMN}`",
        "- Tipo de problema: classificacao multiclasse",
        "- Preditores principais: variaveis organizacionais, rotina de trabalho, bem-estar e suporte.",
        "- Colunas removidas: identificadores, variaveis derivadas do alvo e possiveis consequencias do burnout.",
        "",
        "## Arquivos gerados",
        "",
        "| Arquivo | Conteudo |",
        "|---|---|",
        "| `ml_problem_definition.json` | Definicao estruturada do problema. |",
        "| `target_distribution.csv` | Distribuicao da variavel-alvo. |",
        "| `primary_predictors.csv` | Preditores principais da versao baseline. |",
        "| `fairness_audit_columns.csv` | Colunas para auditoria de vies e segmentacao. |",
        "| `leakage_columns.csv` | Colunas removidas por vazamento ou baixa adequacao. |",
        "| `relevant_correlations_for_ml.csv` | Correlacoes uteis para justificar decisoes. |",
    ]
    (OUTPUT_DIR / "ml_problem_summary.md").write_text("\n".join(summary), encoding="utf-8")

    print("Definicao de ML exportada em outputs/ml.")


if __name__ == "__main__":
    main()
