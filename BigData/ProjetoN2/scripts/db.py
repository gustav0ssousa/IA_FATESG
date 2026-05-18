"""Database helpers shared by ETL, statistics, dashboard and Streamlit scripts."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv
from sqlalchemy import create_engine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def connection_params() -> dict[str, str | int]:
    return {
        "host": env("POSTGRES_HOST", "localhost"),
        "port": int(env("POSTGRES_PORT", "5432")),
        "dbname": env("POSTGRES_DB", "mental_health_tech"),
        "user": env("POSTGRES_USER", "mental_health_user"),
        "password": env("POSTGRES_PASSWORD", "mental_health_password"),
    }


def connect() -> psycopg.Connection:
    return psycopg.connect(**connection_params())


def sqlalchemy_url() -> str:
    params = connection_params()
    return (
        f"postgresql+psycopg://{params['user']}:{params['password']}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )


def wait_for_database(timeout_seconds: int = 90) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None

    while time.time() < deadline:
        try:
            with connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            return
        except Exception as exc:  # noqa: BLE001 - surface the last DB error below.
            last_error = exc
            time.sleep(2)

    raise RuntimeError(f"Database unavailable after {timeout_seconds}s: {last_error}")


def read_sql(sql: str) -> pd.DataFrame:
    engine = create_engine(sqlalchemy_url())
    with engine.connect() as conn:
        return pd.read_sql_query(sql, conn)


def execute_sql(sql: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def execute_sql_file(path: str | Path) -> None:
    sql_path = Path(path)
    if not sql_path.is_absolute():
        sql_path = PROJECT_ROOT / sql_path
    execute_sql(sql_path.read_text(encoding="utf-8"))


def copy_csv_to_table(csv_path: str | Path, table_name: str) -> None:
    source = Path(csv_path)
    if not source.is_absolute():
        source = PROJECT_ROOT / source

    with connect() as conn:
        with conn.cursor() as cur:
            with cur.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as copy:
                with source.open("r", encoding="utf-8", newline="") as file:
                    while chunk := file.read(1024 * 1024):
                        copy.write(chunk)
        conn.commit()
