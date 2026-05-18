"""Prepare train/test datasets for Machine Learning.

The script reads the clean PostgreSQL table, applies the ML problem definition,
splits data with stratification, fits preprocessing only on training data and
exports reusable artifacts for model training.
"""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from scipy import sparse
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from db import env, read_sql, wait_for_database


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ML_DIR = PROJECT_ROOT / "outputs" / "ml"
PREPARED_DIR = ML_DIR / "prepared"
CLEAN_TABLE = env("CLEAN_TABLE", "clean_mental_health_burnout_tech_2026")
RANDOM_STATE = 42
TEST_SIZE = 0.20


def load_problem_definition() -> dict:
    path = ML_DIR / "ml_problem_definition.json"
    if not path.exists():
        raise FileNotFoundError(
            "ML problem definition not found. Run scripts/define_ml_problem.py first."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def load_dataset(problem: dict) -> pd.DataFrame:
    columns = (
        problem["primary_predictors"]
        + problem["fairness_audit_columns"]
        + [problem["target_column"]]
    )
    column_sql = ", ".join(columns)
    return read_sql(f"SELECT {column_sql} FROM {CLEAN_TABLE};")


def classify_columns(x: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    categorical_columns = x.select_dtypes(include=["object", "category"]).columns.tolist()
    binary_columns: list[str] = []
    numeric_columns: list[str] = []

    for column in x.select_dtypes(exclude=["object", "category"]).columns:
        values = set(x[column].dropna().unique().tolist())
        if values.issubset({0, 1}):
            binary_columns.append(column)
        else:
            numeric_columns.append(column)

    return numeric_columns, binary_columns, categorical_columns


def save_json(payload: dict, path: Path) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    wait_for_database()
    PREPARED_DIR.mkdir(parents=True, exist_ok=True)

    problem = load_problem_definition()
    target_column = problem["target_column"]
    predictors = problem["primary_predictors"]
    fairness_columns = problem["fairness_audit_columns"]

    data = load_dataset(problem)
    x = data[predictors]
    y = data[target_column]
    audit = data[fairness_columns]

    numeric_columns, binary_columns, categorical_columns = classify_columns(x)

    (
        x_train,
        x_test,
        y_train,
        y_test,
        audit_train,
        audit_test,
    ) = train_test_split(
        x,
        y,
        audit,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", StandardScaler(), numeric_columns),
            ("binary", "passthrough", binary_columns),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=True),
                categorical_columns,
            ),
        ],
        sparse_threshold=1.0,
        remainder="drop",
    )

    x_train_prepared = preprocessor.fit_transform(x_train)
    x_test_prepared = preprocessor.transform(x_test)

    feature_names = preprocessor.get_feature_names_out().tolist()
    target_distribution_train = y_train.value_counts(normalize=False).rename_axis("classe").reset_index(name="quantidade")
    target_distribution_test = y_test.value_counts(normalize=False).rename_axis("classe").reset_index(name="quantidade")
    target_distribution_train["percentual"] = (
        target_distribution_train["quantidade"] * 100 / len(y_train)
    ).round(4)
    target_distribution_test["percentual"] = (
        target_distribution_test["quantidade"] * 100 / len(y_test)
    ).round(4)

    sparse.save_npz(PREPARED_DIR / "X_train_prepared.npz", x_train_prepared)
    sparse.save_npz(PREPARED_DIR / "X_test_prepared.npz", x_test_prepared)
    joblib.dump(preprocessor, PREPARED_DIR / "preprocessor.joblib")

    x_train.to_csv(PREPARED_DIR / "X_train_raw.csv", index=False, encoding="utf-8")
    x_test.to_csv(PREPARED_DIR / "X_test_raw.csv", index=False, encoding="utf-8")
    y_train.to_frame(name=target_column).to_csv(PREPARED_DIR / "y_train.csv", index=False, encoding="utf-8")
    y_test.to_frame(name=target_column).to_csv(PREPARED_DIR / "y_test.csv", index=False, encoding="utf-8")
    audit_train.to_csv(PREPARED_DIR / "audit_train.csv", index=False, encoding="utf-8")
    audit_test.to_csv(PREPARED_DIR / "audit_test.csv", index=False, encoding="utf-8")
    target_distribution_train.to_csv(
        PREPARED_DIR / "target_distribution_train.csv", index=False, encoding="utf-8"
    )
    target_distribution_test.to_csv(
        PREPARED_DIR / "target_distribution_test.csv", index=False, encoding="utf-8"
    )

    save_json(
        {
            "target_column": target_column,
            "problem_type": problem["problem_type"],
            "random_state": RANDOM_STATE,
            "test_size": TEST_SIZE,
            "train_rows": int(x_train.shape[0]),
            "test_rows": int(x_test.shape[0]),
            "raw_predictor_count": int(x.shape[1]),
            "prepared_feature_count": int(len(feature_names)),
            "numeric_columns": numeric_columns,
            "binary_columns": binary_columns,
            "categorical_columns": categorical_columns,
            "fairness_audit_columns": fairness_columns,
            "leakage_columns_removed": problem["leakage_columns"],
            "prepared_files": {
                "X_train_prepared": "outputs/ml/prepared/X_train_prepared.npz",
                "X_test_prepared": "outputs/ml/prepared/X_test_prepared.npz",
                "preprocessor": "outputs/ml/prepared/preprocessor.joblib",
                "y_train": "outputs/ml/prepared/y_train.csv",
                "y_test": "outputs/ml/prepared/y_test.csv",
            },
        },
        PREPARED_DIR / "split_metadata.json",
    )
    save_json({"feature_names": feature_names}, PREPARED_DIR / "feature_names.json")

    summary = [
        "# Preparacao dos dados para Machine Learning",
        "",
        f"- Linhas de treino: {x_train.shape[0]}",
        f"- Linhas de teste: {x_test.shape[0]}",
        f"- Preditores crus: {x.shape[1]}",
        f"- Features apos preprocessamento: {len(feature_names)}",
        f"- Variavel-alvo: `{target_column}`",
        "- Divisao estratificada para preservar a proporcao das classes.",
        "- Preprocessador ajustado somente no treino para evitar vazamento.",
    ]
    (PREPARED_DIR / "preparation_summary.md").write_text("\n".join(summary), encoding="utf-8")

    print("Preparacao de ML concluida.")
    print(f"Treino: {x_train.shape[0]} linhas")
    print(f"Teste: {x_test.shape[0]} linhas")
    print(f"Features preparadas: {len(feature_names)}")


if __name__ == "__main__":
    main()
