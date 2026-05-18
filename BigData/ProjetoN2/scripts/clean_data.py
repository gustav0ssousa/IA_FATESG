"""Recreate and validate the clean PostgreSQL table.

Usage:
    python scripts/clean_data.py
    python scripts/clean_data.py --skip-rebuild
"""

from __future__ import annotations

import argparse

from db import env, execute_sql_file, read_sql, wait_for_database


RAW_TABLE = env("RAW_TABLE", "raw_mental_health_burnout_tech_2026")
CLEAN_TABLE = env("CLEAN_TABLE", "clean_mental_health_burnout_tech_2026")

ALL_COLUMNS = [
    "employee_id",
    "age",
    "gender",
    "country",
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
    "stress_score",
    "burnout_score",
    "phq9_score",
    "phq9_category",
    "gad7_score",
    "gad7_category",
    "burnout_level",
    "seeks_mental_health_support",
    "job_change_intention",
]

NUMERIC_ANALYSIS_COLUMNS = [
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


def print_section(title: str) -> None:
    print("\n" + "=" * len(title))
    print(title)
    print("=" * len(title))


def print_df(sql: str) -> None:
    print(read_sql(sql).to_string(index=False))


def rebuild_clean_table() -> None:
    print("Recriando tabela tratada...")
    execute_sql_file("sql/02_create_clean_table.sql")
    print("Tabela tratada recriada.")


def validate_counts() -> None:
    print_section("Contagem de registros")
    print_df(
        f"""
        SELECT '{RAW_TABLE}' AS tabela, COUNT(*) AS linhas FROM {RAW_TABLE}
        UNION ALL
        SELECT '{CLEAN_TABLE}' AS tabela, COUNT(*) AS linhas FROM {CLEAN_TABLE}
        ORDER BY tabela;
        """
    )


def validate_nulls() -> None:
    print_section("Valores nulos")
    null_expression = " + ".join(
        f"SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)" for column in ALL_COLUMNS
    )
    print_df(f"SELECT {null_expression} AS total_nulos FROM {CLEAN_TABLE};")


def validate_duplicates() -> None:
    print_section("Duplicatas")
    full_duplicate_group = ", ".join(ALL_COLUMNS)
    print_df(
        f"""
        WITH duplicated_rows AS (
            SELECT COUNT(*) AS frequency
            FROM {RAW_TABLE}
            GROUP BY {full_duplicate_group}
            HAVING COUNT(*) > 1
        )
        SELECT
            (SELECT COUNT(*) - COUNT(DISTINCT employee_id) FROM {RAW_TABLE})
                AS employee_id_duplicados,
            COALESCE(SUM(frequency - 1), 0)
                AS duplicatas_completas
        FROM duplicated_rows;
        """
    )


def validate_categorical_cardinality() -> None:
    print_section("Categorias unicas")
    union_sql = "\nUNION ALL\n".join(
        f"SELECT '{column}' AS coluna, COUNT(DISTINCT {column}) AS categorias FROM {CLEAN_TABLE}"
        for column in CATEGORICAL_COLUMNS
    )
    print_df(f"{union_sql}\nORDER BY coluna;")


def validate_outliers() -> None:
    print_section("Outliers por IQR")
    numeric_union = "\nUNION ALL\n".join(
        f"SELECT '{column}' AS coluna, {column}::double precision AS valor FROM {CLEAN_TABLE}"
        for column in NUMERIC_ANALYSIS_COLUMNS
    )
    print_df(
        f"""
        WITH numeric_values AS (
            {numeric_union}
        ), quartiles AS (
            SELECT
                coluna,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY valor) AS q1,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY valor) AS q3
            FROM numeric_values
            GROUP BY coluna
        ), bounds AS (
            SELECT
                coluna,
                q1,
                q3,
                q3 - q1 AS iqr,
                q1 - 1.5 * (q3 - q1) AS limite_inferior,
                q3 + 1.5 * (q3 - q1) AS limite_superior
            FROM quartiles
        )
        SELECT
            n.coluna,
            ROUND(b.q1::numeric, 2) AS q1,
            ROUND(b.q3::numeric, 2) AS q3,
            ROUND(b.iqr::numeric, 2) AS iqr,
            ROUND(b.limite_inferior::numeric, 2) AS limite_inferior,
            ROUND(b.limite_superior::numeric, 2) AS limite_superior,
            SUM(CASE WHEN n.valor < b.limite_inferior OR n.valor > b.limite_superior THEN 1 ELSE 0 END) AS outliers_iqr
        FROM numeric_values n
        JOIN bounds b ON n.coluna = b.coluna
        GROUP BY n.coluna, b.q1, b.q3, b.iqr, b.limite_inferior, b.limite_superior
        ORDER BY outliers_iqr DESC, n.coluna
        LIMIT 12;
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run and validate the cleaning pipeline.")
    parser.add_argument(
        "--skip-rebuild",
        action="store_true",
        help="Only run validations without recreating the clean table.",
    )
    args = parser.parse_args()

    wait_for_database()

    if not args.skip_rebuild:
        rebuild_clean_table()

    validate_counts()
    validate_nulls()
    validate_duplicates()
    validate_categorical_cardinality()
    validate_outliers()

    print("\nLimpeza e validacao concluidas.")


if __name__ == "__main__":
    main()
