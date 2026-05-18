"""Train the second ML algorithm: Random Forest classifier."""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from scipy import sparse
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PREPARED_DIR = PROJECT_ROOT / "outputs" / "ml" / "prepared"
MODEL_DIR = PROJECT_ROOT / "outputs" / "ml" / "models" / "random_forest"
TARGET_COLUMN = "burnout_level"
RANDOM_STATE = 42


def load_data():
    x_train = sparse.load_npz(PREPARED_DIR / "X_train_prepared.npz")
    x_test = sparse.load_npz(PREPARED_DIR / "X_test_prepared.npz")
    y_train = pd.read_csv(PREPARED_DIR / "y_train.csv")[TARGET_COLUMN]
    y_test = pd.read_csv(PREPARED_DIR / "y_test.csv")[TARGET_COLUMN]
    return x_train, x_test, y_train, y_test


def save_json(payload: dict, path: Path) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    x_train, x_test, y_train, y_test = load_data()

    model = RandomForestClassifier(
        n_estimators=250,
        max_depth=18,
        min_samples_leaf=8,
        class_weight="balanced_subsample",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(x_train, y_train)

    y_pred = model.predict(x_test)
    y_proba = model.predict_proba(x_test)

    labels = list(model.classes_)
    metrics = {
        "algorithm": "Random Forest Classifier",
        "accuracy": accuracy_score(y_test, y_pred),
        "balanced_accuracy": balanced_accuracy_score(y_test, y_pred),
        "precision_macro": precision_score(y_test, y_pred, average="macro", zero_division=0),
        "recall_macro": recall_score(y_test, y_pred, average="macro", zero_division=0),
        "f1_macro": f1_score(y_test, y_pred, average="macro", zero_division=0),
        "precision_weighted": precision_score(y_test, y_pred, average="weighted", zero_division=0),
        "recall_weighted": recall_score(y_test, y_pred, average="weighted", zero_division=0),
        "f1_weighted": f1_score(y_test, y_pred, average="weighted", zero_division=0),
        "recall_high": recall_score(y_test, y_pred, labels=["High"], average="macro", zero_division=0),
        "recall_severe": recall_score(y_test, y_pred, labels=["Severe"], average="macro", zero_division=0),
        "train_rows": int(x_train.shape[0]),
        "test_rows": int(x_test.shape[0]),
        "feature_count": int(x_train.shape[1]),
        "classes": labels,
    }

    report_df = (
        pd.DataFrame(classification_report(y_test, y_pred, output_dict=True, zero_division=0))
        .transpose()
        .reset_index()
        .rename(columns={"index": "classe"})
    )
    confusion_df = pd.DataFrame(
        confusion_matrix(y_test, y_pred, labels=labels),
        index=[f"real_{label}" for label in labels],
        columns=[f"pred_{label}" for label in labels],
    )
    predictions_df = pd.DataFrame({"y_true": y_test, "y_pred": y_pred})
    for index, label in enumerate(labels):
        predictions_df[f"prob_{label}"] = y_proba[:, index]

    feature_names = json.loads((PREPARED_DIR / "feature_names.json").read_text(encoding="utf-8"))[
        "feature_names"
    ]
    importance_df = pd.DataFrame(
        {
            "feature": feature_names,
            "importance": model.feature_importances_,
        }
    ).sort_values("importance", ascending=False)

    joblib.dump(model, MODEL_DIR / "model.joblib")
    save_json(metrics, MODEL_DIR / "metrics.json")
    report_df.to_csv(MODEL_DIR / "classification_report.csv", index=False, encoding="utf-8")
    confusion_df.to_csv(MODEL_DIR / "confusion_matrix.csv", encoding="utf-8")
    predictions_df.to_csv(MODEL_DIR / "test_predictions.csv", index=False, encoding="utf-8")
    importance_df.to_csv(MODEL_DIR / "feature_importance.csv", index=False, encoding="utf-8")

    summary = [
        "# Segundo algoritmo - Random Forest",
        "",
        f"- Accuracy: {metrics['accuracy']:.4f}",
        f"- Balanced accuracy: {metrics['balanced_accuracy']:.4f}",
        f"- F1 macro: {metrics['f1_macro']:.4f}",
        f"- Recall High: {metrics['recall_high']:.4f}",
        f"- Recall Severe: {metrics['recall_severe']:.4f}",
    ]
    (MODEL_DIR / "model_summary.md").write_text("\n".join(summary), encoding="utf-8")

    print("Treinamento do segundo modelo concluido.")
    print(f"Accuracy: {metrics['accuracy']:.4f}")
    print(f"Balanced accuracy: {metrics['balanced_accuracy']:.4f}")
    print(f"F1 macro: {metrics['f1_macro']:.4f}")


if __name__ == "__main__":
    main()
