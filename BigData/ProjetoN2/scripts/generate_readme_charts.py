"""
Gera graficos estaticos (PNG) para uso no README do projeto.
Saida: docs/images/
"""

import os
import sys
import numpy as np

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
except ImportError:
    print("matplotlib nao encontrado. Instalando...")
    os.system(f"{sys.executable} -m pip install matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

# ---------- configuracao visual ----------
plt.rcParams.update({
    "figure.facecolor": "#0d1117",
    "axes.facecolor": "#161b22",
    "axes.edgecolor": "#30363d",
    "axes.labelcolor": "#c9d1d9",
    "xtick.color": "#8b949e",
    "ytick.color": "#8b949e",
    "text.color": "#c9d1d9",
    "grid.color": "#21262d",
    "grid.linestyle": "--",
    "grid.alpha": 0.6,
    "font.family": "sans-serif",
    "font.size": 11,
})

PALETTE = ["#58a6ff", "#3fb950", "#d29922", "#f85149"]
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "images")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save(fig, name):
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  -> {path}")


# =====================================================================
# 1. Distribuicao de burnout_level
# =====================================================================
def chart_burnout_distribution():
    labels = ["Severe", "Moderate", "Low", "High"]
    values = [28576, 26255, 25807, 19362]
    colors = ["#f85149", "#d29922", "#3fb950", "#58a6ff"]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.barh(labels[::-1], values[::-1], color=colors[::-1], edgecolor="#30363d", height=0.6)
    ax.set_xlabel("Quantidade de profissionais")
    ax.set_title("Distribuição do Nível de Burnout", fontsize=14, fontweight="bold", pad=12)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k"))
    ax.grid(axis="x")

    for bar, val in zip(bars, values[::-1]):
        pct = val / 100000 * 100
        ax.text(bar.get_width() + 800, bar.get_y() + bar.get_height()/2,
                f"{val:,}  ({pct:.1f}%)", va="center", fontsize=10, color="#c9d1d9")

    ax.set_xlim(0, 38000)
    save(fig, "01_burnout_distribution.png")


# =====================================================================
# 2. Top 10 feature importances (Random Forest)
# =====================================================================
def chart_feature_importance():
    features = [
        ("work_hours_per_week", 0.1423),
        ("meetings_per_day", 0.1235),
        ("sleep_hours_per_night", 0.1089),
        ("vacation_days_taken", 0.0787),
        ("social_support_score", 0.0642),
        ("manager_support_score", 0.0493),
        ("deadline_pressure_score", 0.0427),
        ("exercise_days_per_week", 0.0405),
        ("salary_usd", 0.0353),
        ("autonomy_score", 0.0351),
    ]
    names = [f[0].replace("_", " ").title() for f in features][::-1]
    importances = [f[1] for f in features][::-1]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(names, importances, color="#58a6ff", edgecolor="#30363d", height=0.6)
    ax.set_xlabel("Importância relativa")
    ax.set_title("Top 10 Features — Random Forest", fontsize=14, fontweight="bold", pad=12)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0%}"))
    ax.grid(axis="x")

    for bar, val in zip(bars, importances):
        ax.text(bar.get_width() + 0.003, bar.get_y() + bar.get_height()/2,
                f"{val:.1%}", va="center", fontsize=9, color="#8b949e")

    ax.set_xlim(0, 0.18)
    save(fig, "02_feature_importance.png")


# =====================================================================
# 3. Matriz de confusao — Regressao Logistica
# =====================================================================
def chart_confusion_matrix():
    classes = ["High", "Low", "Moderate", "Severe"]
    cm = np.array([
        [1638, 212, 1080, 943],
        [220, 3794, 1137, 10],
        [1417, 1348, 2187, 299],
        [1328, 17, 270, 4100],
    ])

    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    im = ax.imshow(cm, cmap="Blues", aspect="auto")

    ax.set_xticks(range(4))
    ax.set_yticks(range(4))
    ax.set_xticklabels(classes, fontsize=10)
    ax.set_yticklabels(classes, fontsize=10)
    ax.set_xlabel("Previsto", fontsize=12)
    ax.set_ylabel("Real", fontsize=12)
    ax.set_title("Matriz de Confusão — Regressão Logística", fontsize=13, fontweight="bold", pad=12)

    for i in range(4):
        for j in range(4):
            val = cm[i, j]
            color = "white" if val > cm.max() * 0.5 else "#c9d1d9"
            ax.text(j, i, f"{val:,}", ha="center", va="center", fontsize=11, color=color, fontweight="bold")

    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.ax.yaxis.set_tick_params(color="#8b949e")
    cbar.outline.set_edgecolor("#30363d")
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="#8b949e")

    save(fig, "03_confusion_matrix_lr.png")


# =====================================================================
# 4. Comparacao de metricas entre modelos
# =====================================================================
def chart_model_comparison():
    metrics = ["Acurácia", "Balanced\nAccuracy", "F1 Macro", "Recall\nHigh", "Recall\nSevere"]
    lr_vals = [0.5859, 0.5730, 0.5722, 0.4229, 0.7174]
    rf_vals = [0.5583, 0.5337, 0.5228, 0.2355, 0.7522]

    x = np.arange(len(metrics))
    width = 0.32

    fig, ax = plt.subplots(figsize=(9, 5))
    bars1 = ax.bar(x - width/2, lr_vals, width, label="Regressão Logística", color="#58a6ff", edgecolor="#30363d")
    bars2 = ax.bar(x + width/2, rf_vals, width, label="Random Forest", color="#3fb950", edgecolor="#30363d")

    ax.set_ylabel("Score")
    ax.set_title("Comparação de Modelos", fontsize=14, fontweight="bold", pad=12)
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, fontsize=10)
    ax.set_ylim(0, 0.95)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:.0%}"))
    ax.legend(loc="upper right", framealpha=0.3, edgecolor="#30363d")
    ax.grid(axis="y")

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.015,
                f"{bar.get_height():.1%}", ha="center", fontsize=8, color="#8b949e")
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.015,
                f"{bar.get_height():.1%}", ha="center", fontsize=8, color="#8b949e")

    save(fig, "04_model_comparison.png")


# =====================================================================
# 5. KPIs de alerta — indicadores criticos
# =====================================================================
def chart_kpi_alerts():
    labels = [
        "Burnout\nalto ou severo",
        "Sono\n< 6h/noite",
        "Jornada\n> 50h/semana",
        "Busca suporte\nem saúde mental",
        "Uso de\nterapia",
    ]
    values = [47.94, 43.99, 25.63, 49.79, 15.19]
    colors = ["#f85149", "#d29922", "#d29922", "#58a6ff", "#3fb950"]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.bar(labels, values, color=colors, edgecolor="#30363d", width=0.55)
    ax.set_ylabel("% dos profissionais")
    ax.set_title("Indicadores Críticos de Saúde Mental", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylim(0, 65)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:.0f}%"))
    ax.grid(axis="y")

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1.2,
                f"{val:.1f}%", ha="center", fontsize=11, color="#c9d1d9", fontweight="bold")

    save(fig, "05_kpi_alerts.png")


# =====================================================================
if __name__ == "__main__":
    print("Gerando graficos para o README...")
    chart_burnout_distribution()
    chart_feature_importance()
    chart_confusion_matrix()
    chart_model_comparison()
    chart_kpi_alerts()
    print("Concluido!")
