"""Generate statistical datasets for reports, dashboards, BI and KPIs.

The script reads from the clean PostgreSQL table and exports reusable files to
`outputs/statistics`. It keeps the statistical analysis reproducible and gives
the dashboard/BI steps ready-to-consume datasets.

Usage:
    python scripts/statistical_analysis.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from db import env, read_sql, wait_for_database


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "statistics"
CLEAN_TABLE = env("CLEAN_TABLE", "clean_mental_health_burnout_tech_2026")

NUMERIC_COLUMNS = [
    "age",
    "years_experience",
    "years_at_company",
    "salary_usd",
    "work_hours_per_week",
    "meetings_per_day",
    "team_size",
    "sleep_hours_per_night",
    "exercise_days_per_week",
    "vacation_days_taken",
    "manager_support_score",
    "work_life_balance_score",
    "job_satisfaction_score",
    "social_support_score",
    "deadline_pressure_score",
    "autonomy_score",
    "stress_score",
    "burnout_score",
    "phq9_score",
    "gad7_score",
]

BINARY_COLUMNS = [
    "therapy_access",
    "uses_therapy",
    "ai_tools_daily",
    "seeks_mental_health_support",
    "job_change_intention",
]

CATEGORICAL_COLUMNS = [
    "gender",
    "country",
    "job_role",
    "seniority_level",
    "company_size",
    "industry",
    "work_mode",
    "phq9_category",
    "gad7_category",
    "burnout_level",
]

GROUPING_COLUMNS = [
    "work_mode",
    "country",
    "job_role",
    "seniority_level",
    "company_size",
    "industry",
    "gender",
]

def save_dataframe(df: pd.DataFrame, filename: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / filename
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Arquivo gerado: {path.relative_to(PROJECT_ROOT)}")
    return path


def numeric_values_cte() -> str:
    return "\nUNION ALL\n".join(
        f"SELECT '{column}' AS coluna, {column}::double precision AS valor FROM {CLEAN_TABLE}"
        for column in NUMERIC_COLUMNS
    )


def get_numeric_statistics() -> pd.DataFrame:
    sql = f"""
        WITH numeric_values AS (
            {numeric_values_cte()}
        ), stats AS (
            SELECT
                coluna,
                COUNT(*) AS n,
                AVG(valor) AS media,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY valor) AS mediana,
                mode() WITHIN GROUP (ORDER BY valor) AS moda,
                STDDEV_SAMP(valor) AS desvio_padrao,
                VAR_SAMP(valor) AS variancia,
                MIN(valor) AS minimo,
                MAX(valor) AS maximo,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY valor) AS q1,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY valor) AS q2,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY valor) AS q3
            FROM numeric_values
            GROUP BY coluna
        ), outliers AS (
            SELECT
                n.coluna,
                SUM(CASE
                    WHEN n.valor < (s.q1 - 1.5 * (s.q3 - s.q1))
                      OR n.valor > (s.q3 + 1.5 * (s.q3 - s.q1))
                    THEN 1 ELSE 0 END) AS outliers_iqr
            FROM numeric_values n
            JOIN stats s ON s.coluna = n.coluna
            GROUP BY n.coluna
        )
        SELECT
            s.coluna,
            s.n,
            ROUND(s.media::numeric, 4) AS media,
            ROUND(s.mediana::numeric, 4) AS mediana,
            ROUND(s.moda::numeric, 4) AS moda,
            ROUND(s.desvio_padrao::numeric, 4) AS desvio_padrao,
            ROUND(s.variancia::numeric, 4) AS variancia,
            ROUND(s.minimo::numeric, 4) AS minimo,
            ROUND(s.maximo::numeric, 4) AS maximo,
            ROUND(s.q1::numeric, 4) AS q1,
            ROUND(s.q2::numeric, 4) AS q2,
            ROUND(s.q3::numeric, 4) AS q3,
            ROUND((s.q3 - s.q1)::numeric, 4) AS iqr,
            ROUND((s.q1 - 1.5 * (s.q3 - s.q1))::numeric, 4) AS limite_inferior_iqr,
            ROUND((s.q3 + 1.5 * (s.q3 - s.q1))::numeric, 4) AS limite_superior_iqr,
            o.outliers_iqr,
            ROUND((o.outliers_iqr * 100.0 / s.n)::numeric, 4) AS outliers_iqr_pct
        FROM stats s
        JOIN outliers o ON o.coluna = s.coluna
        ORDER BY s.coluna;
    """
    return read_sql(sql)


def get_categorical_frequencies() -> pd.DataFrame:
    categorical_union = "\nUNION ALL\n".join(
        f"SELECT '{column}' AS coluna, {column}::text AS categoria FROM {CLEAN_TABLE}"
        for column in CATEGORICAL_COLUMNS
    )
    sql = f"""
        WITH categorical_values AS (
            {categorical_union}
        ), totals AS (
            SELECT coluna, COUNT(*) AS total, COUNT(DISTINCT categoria) AS categorias_unicas
            FROM categorical_values
            GROUP BY coluna
        ), frequencies AS (
            SELECT
                v.coluna,
                v.categoria,
                COUNT(*) AS frequencia_absoluta,
                ROUND((COUNT(*) * 100.0 / t.total)::numeric, 4) AS frequencia_relativa_pct,
                t.categorias_unicas,
                ROW_NUMBER() OVER (PARTITION BY v.coluna ORDER BY COUNT(*) DESC, v.categoria) AS ranking
            FROM categorical_values v
            JOIN totals t ON t.coluna = v.coluna
            GROUP BY v.coluna, v.categoria, t.total, t.categorias_unicas
        )
        SELECT *
        FROM frequencies
        ORDER BY coluna, ranking;
    """
    return read_sql(sql)


def get_binary_frequencies() -> pd.DataFrame:
    binary_union = "\nUNION ALL\n".join(
        f"SELECT '{column}' AS coluna, {column}::text AS categoria FROM {CLEAN_TABLE}"
        for column in BINARY_COLUMNS
    )
    sql = f"""
        WITH binary_values AS (
            {binary_union}
        ), totals AS (
            SELECT coluna, COUNT(*) AS total
            FROM binary_values
            GROUP BY coluna
        )
        SELECT
            v.coluna,
            v.categoria,
            COUNT(*) AS frequencia_absoluta,
            ROUND((COUNT(*) * 100.0 / t.total)::numeric, 4) AS frequencia_relativa_pct
        FROM binary_values v
        JOIN totals t ON t.coluna = v.coluna
        GROUP BY v.coluna, v.categoria, t.total
        ORDER BY v.coluna, v.categoria;
    """
    return read_sql(sql)


def get_kpi_summary() -> pd.DataFrame:
    sql = f"""
        SELECT
            COUNT(*) AS total_registros,
            ROUND(AVG(age)::numeric, 4) AS idade_media,
            ROUND(AVG(years_experience)::numeric, 4) AS experiencia_media,
            ROUND(AVG(salary_usd)::numeric, 4) AS salario_medio_usd,
            ROUND(AVG(work_hours_per_week)::numeric, 4) AS horas_semanais_media,
            ROUND(AVG(meetings_per_day)::numeric, 4) AS reunioes_dia_media,
            ROUND(AVG(sleep_hours_per_night)::numeric, 4) AS sono_medio_horas,
            ROUND(AVG(stress_score)::numeric, 4) AS stress_score_medio,
            ROUND(AVG(burnout_score)::numeric, 4) AS burnout_score_medio,
            ROUND(AVG(job_satisfaction_score)::numeric, 4) AS satisfacao_media,
            ROUND(AVG(work_life_balance_score)::numeric, 4) AS equilibrio_vida_trabalho_medio,
            ROUND(AVG(manager_support_score)::numeric, 4) AS apoio_gestor_medio,
            ROUND(AVG(CASE WHEN burnout_level = 'Severe' THEN 1 ELSE 0 END) * 100, 4) AS burnout_severe_pct,
            ROUND(AVG(CASE WHEN burnout_level IN ('High', 'Severe') THEN 1 ELSE 0 END) * 100, 4) AS burnout_high_or_severe_pct,
            ROUND(AVG(CASE WHEN work_hours_per_week > 50 THEN 1 ELSE 0 END) * 100, 4) AS mais_50h_semanais_pct,
            ROUND(AVG(CASE WHEN sleep_hours_per_night < 6 THEN 1 ELSE 0 END) * 100, 4) AS sono_menor_6h_pct,
            ROUND(AVG(CASE WHEN phq9_score >= 10 THEN 1 ELSE 0 END) * 100, 4) AS phq9_moderado_ou_maior_pct,
            ROUND(AVG(CASE WHEN gad7_score >= 10 THEN 1 ELSE 0 END) * 100, 4) AS gad7_moderado_ou_maior_pct,
            ROUND(AVG(therapy_access) * 100, 4) AS acesso_terapia_pct,
            ROUND(AVG(uses_therapy) * 100, 4) AS usa_terapia_pct,
            ROUND(AVG(seeks_mental_health_support) * 100, 4) AS busca_suporte_mental_pct,
            ROUND(AVG(job_change_intention) * 100, 4) AS intencao_troca_emprego_pct,
            ROUND(AVG(ai_tools_daily) * 100, 4) AS uso_diario_ia_pct
        FROM {CLEAN_TABLE};
    """
    return read_sql(sql)


def get_grouped_kpis() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for group_column in GROUPING_COLUMNS:
        sql = f"""
            SELECT
                '{group_column}' AS dimensao,
                {group_column}::text AS categoria,
                COUNT(*) AS total_registros,
                ROUND(AVG(stress_score)::numeric, 4) AS stress_score_medio,
                ROUND(AVG(burnout_score)::numeric, 4) AS burnout_score_medio,
                ROUND(AVG(job_satisfaction_score)::numeric, 4) AS satisfacao_media,
                ROUND(AVG(work_life_balance_score)::numeric, 4) AS equilibrio_vida_trabalho_medio,
                ROUND(AVG(manager_support_score)::numeric, 4) AS apoio_gestor_medio,
                ROUND(AVG(work_hours_per_week)::numeric, 4) AS horas_semanais_media,
                ROUND(AVG(sleep_hours_per_night)::numeric, 4) AS sono_medio_horas,
                ROUND(AVG(CASE WHEN burnout_level = 'Severe' THEN 1 ELSE 0 END) * 100, 4) AS burnout_severe_pct,
                ROUND(AVG(CASE WHEN burnout_level IN ('High', 'Severe') THEN 1 ELSE 0 END) * 100, 4) AS burnout_high_or_severe_pct,
                ROUND(AVG(seeks_mental_health_support) * 100, 4) AS busca_suporte_mental_pct,
                ROUND(AVG(job_change_intention) * 100, 4) AS intencao_troca_emprego_pct,
                ROUND(AVG(uses_therapy) * 100, 4) AS usa_terapia_pct,
                ROUND(AVG(ai_tools_daily) * 100, 4) AS uso_diario_ia_pct
            FROM {CLEAN_TABLE}
            GROUP BY {group_column}
            ORDER BY {group_column};
        """
        frames.append(read_sql(sql))

    return pd.concat(frames, ignore_index=True)


def get_correlation_outputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    selected_columns = NUMERIC_COLUMNS + BINARY_COLUMNS
    sql = f"SELECT {', '.join(selected_columns)} FROM {CLEAN_TABLE};"
    numeric_data = read_sql(sql)
    correlation_matrix = numeric_data.corr(numeric_only=True).round(6)
    correlation_long = (
        correlation_matrix.reset_index()
        .melt(id_vars="index", var_name="variavel_2", value_name="correlacao")
        .rename(columns={"index": "variavel_1"})
    )
    correlation_long = correlation_long[
        correlation_long["variavel_1"] != correlation_long["variavel_2"]
    ].sort_values("correlacao", key=lambda series: series.abs(), ascending=False)
    return correlation_matrix.reset_index().rename(columns={"index": "variavel"}), correlation_long


def write_summary(
    numeric_stats: pd.DataFrame,
    categorical_freq: pd.DataFrame,
    binary_freq: pd.DataFrame,
    kpi_summary: pd.DataFrame,
) -> None:
    summary_path = OUTPUT_DIR / "statistical_analysis_summary.md"
    kpis = kpi_summary.iloc[0].to_dict()
    top_burnout = categorical_freq[
        (categorical_freq["coluna"] == "burnout_level") & (categorical_freq["ranking"] == 1)
    ].iloc[0]
    top_work_mode = categorical_freq[
        (categorical_freq["coluna"] == "work_mode") & (categorical_freq["ranking"] == 1)
    ].iloc[0]
    top_outliers = numeric_stats.sort_values("outliers_iqr", ascending=False).head(8)

    lines = [
        "# Resumo da analise estatistica",
        "",
        "## KPIs principais",
        "",
        "| KPI | Valor |",
        "|---|---:|",
        f"| Total de registros | {int(kpis['total_registros']):,} |".replace(",", "."),
        f"| Stress score medio | {kpis['stress_score_medio']:.2f} |",
        f"| Burnout score medio | {kpis['burnout_score_medio']:.2f} |",
        f"| Burnout severo | {kpis['burnout_severe_pct']:.2f}% |",
        f"| Burnout alto ou severo | {kpis['burnout_high_or_severe_pct']:.2f}% |",
        f"| Media de horas semanais | {kpis['horas_semanais_media']:.2f} |",
        f"| Sono medio em horas | {kpis['sono_medio_horas']:.2f} |",
        f"| Usa terapia | {kpis['usa_terapia_pct']:.2f}% |",
        f"| Busca suporte mental | {kpis['busca_suporte_mental_pct']:.2f}% |",
        f"| Intencao de troca de emprego | {kpis['intencao_troca_emprego_pct']:.2f}% |",
        "",
        "## Categorias dominantes",
        "",
        "| Variavel | Moda | Frequencia relativa |",
        "|---|---|---:|",
        f"| burnout_level | {top_burnout['categoria']} | {top_burnout['frequencia_relativa_pct']:.2f}% |",
        f"| work_mode | {top_work_mode['categoria']} | {top_work_mode['frequencia_relativa_pct']:.2f}% |",
        "",
        "## Principais outliers",
        "",
        "| Variavel | Outliers IQR | Percentual |",
        "|---|---:|---:|",
    ]

    for row in top_outliers.itertuples(index=False):
        lines.append(f"| `{row.coluna}` | {int(row.outliers_iqr):,} | {row.outliers_iqr_pct:.2f}% |".replace(",", "."))

    lines.extend(
        [
            "",
            "## Arquivos gerados",
            "",
            "| Arquivo | Uso recomendado |",
            "|---|---|",
            "| `numeric_statistics.csv` | Tabelas estatisticas, boxplots, histogramas e KPIs numericos. |",
            "| `categorical_frequencies.csv` | Graficos de barras, filtros e segmentacoes categoricas. |",
            "| `binary_frequencies.csv` | KPIs percentuais para indicadores 0/1. |",
            "| `kpi_summary.csv` | Cards de indicadores gerais. |",
            "| `kpi_summary.json` | Consumo por aplicacoes, dashboards ou APIs. |",
            "| `grouped_kpis.csv` | Comparacoes por pais, cargo, senioridade, setor e modo de trabalho. |",
            "| `correlation_matrix.csv` | Heatmap de correlacao. |",
            "| `correlation_long.csv` | Ranking de correlacoes para analises e filtros. |",
        ]
    )

    summary_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Arquivo gerado: {summary_path.relative_to(PROJECT_ROOT)}")


def write_kpi_json(kpi_summary: pd.DataFrame) -> None:
    path = OUTPUT_DIR / "kpi_summary.json"
    payload = kpi_summary.iloc[0].to_dict()
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Arquivo gerado: {path.relative_to(PROJECT_ROOT)}")


def main() -> None:
    wait_for_database()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Gerando estatisticas numericas...")
    numeric_stats = get_numeric_statistics()
    save_dataframe(numeric_stats, "numeric_statistics.csv")

    print("Gerando frequencias categoricas...")
    categorical_freq = get_categorical_frequencies()
    save_dataframe(categorical_freq, "categorical_frequencies.csv")

    print("Gerando frequencias binarias...")
    binary_freq = get_binary_frequencies()
    save_dataframe(binary_freq, "binary_frequencies.csv")

    print("Gerando KPIs gerais...")
    kpi_summary = get_kpi_summary()
    save_dataframe(kpi_summary, "kpi_summary.csv")
    write_kpi_json(kpi_summary)

    print("Gerando KPIs agrupados...")
    grouped_kpis = get_grouped_kpis()
    save_dataframe(grouped_kpis, "grouped_kpis.csv")

    print("Gerando matriz de correlacao...")
    correlation_matrix, correlation_long = get_correlation_outputs()
    save_dataframe(correlation_matrix, "correlation_matrix.csv")
    save_dataframe(correlation_long, "correlation_long.csv")

    write_summary(numeric_stats, categorical_freq, binary_freq, kpi_summary)
    print("\nAnalise estatistica exportada com sucesso.")


if __name__ == "__main__":
    main()
