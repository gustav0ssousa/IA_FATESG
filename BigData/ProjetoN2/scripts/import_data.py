"""Import the source CSV into the PostgreSQL raw table."""

from __future__ import annotations

from pathlib import Path

from db import env, execute_sql_file, read_sql, wait_for_database, copy_csv_to_table


RAW_TABLE = env("RAW_TABLE", "raw_mental_health_burnout_tech_2026")
CSV_PATH = Path(env("CSV_PATH", "data/mental_health_burnout_tech_2026.csv"))


def main() -> None:
    wait_for_database()
    print("Criando tabela bruta...")
    execute_sql_file("sql/01_create_raw_table.sql")

    print(f"Importando CSV para {RAW_TABLE}...")
    copy_csv_to_table(CSV_PATH, RAW_TABLE)

    result = read_sql(f"SELECT COUNT(*) AS linhas_importadas FROM {RAW_TABLE};")
    print(result.to_string(index=False))
    print("Importacao concluida.")


if __name__ == "__main__":
    main()
