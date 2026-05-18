"""Streamlit dashboard for the mental health tech project."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATISTICS_DIR = PROJECT_ROOT / "outputs" / "statistics"
DASHBOARD_DATA_DIR = PROJECT_ROOT / "outputs" / "dashboard" / "data"

PRIMARY = "#2f6f73"
BLUE = "#4f7cac"
RED = "#c94b4b"
GOLD = "#d99645"
GREEN = "#537a5a"
PLUM = "#6f5b8c"
TEXT = "#263238"
MUTED = "#65747b"
PANEL = "#ffffff"
BG = "#f6f7f4"
LINE = "#d8ded8"

CHART_TEMPLATE = "plotly_white"
WORK_MODE_COLORS = {
    "Hybrid": GREEN,
    "On-site": GOLD,
    "Remote": BLUE,
}

st.set_page_config(page_title="Saude mental em tecnologia", layout="wide")

PIPELINE_STEPS = [
    {
        "name": "Importar dados",
        "script": "scripts/import_data.py",
        "outputs": [],
        "description": "Cria a tabela bruta no PostgreSQL e importa o CSV.",
    },
    {
        "name": "Limpar dados",
        "script": "scripts/clean_data.py",
        "outputs": [],
        "description": "Recria a tabela tratada e executa validacoes de qualidade.",
    },
    {
        "name": "Gerar estatisticas e KPIs",
        "script": "scripts/statistical_analysis.py",
        "outputs": [
            STATISTICS_DIR / "kpi_summary.json",
            STATISTICS_DIR / "grouped_kpis.csv",
            STATISTICS_DIR / "correlation_matrix.csv",
        ],
        "description": "Exporta estatisticas, frequencias, KPIs e correlacoes.",
    },
    {
        "name": "Exportar dashboard HTML",
        "script": "scripts/dashboard_visual.py",
        "outputs": [
            DASHBOARD_DATA_DIR / "stress_score_values.csv",
            DASHBOARD_DATA_DIR / "burnout_score_by_work_mode.csv",
            DASHBOARD_DATA_DIR / "stress_burnout_sample.csv",
        ],
        "description": "Gera datasets visuais, graficos Plotly e dashboard HTML.",
    },
    {
        "name": "Definir problema de ML",
        "script": "scripts/define_ml_problem.py",
        "outputs": [
            PROJECT_ROOT / "outputs" / "ml" / "ml_problem_definition.json",
            PROJECT_ROOT / "outputs" / "ml" / "primary_predictors.csv",
        ],
        "description": "Exporta alvo, preditores e colunas removidas por vazamento.",
    },
    {
        "name": "Preparar dados de ML",
        "script": "scripts/prepare_ml_data.py",
        "outputs": [
            PROJECT_ROOT / "outputs" / "ml" / "prepared" / "X_train_prepared.npz",
            PROJECT_ROOT / "outputs" / "ml" / "prepared" / "X_test_prepared.npz",
            PROJECT_ROOT / "outputs" / "ml" / "prepared" / "preprocessor.joblib",
        ],
        "description": "Gera treino/teste, encoding, scaling e artefatos para modelagem.",
    },
    {
        "name": "Treinar primeiro modelo",
        "script": "scripts/train_first_model.py",
        "outputs": [
            PROJECT_ROOT / "outputs" / "ml" / "models" / "logistic_regression" / "model.joblib",
            PROJECT_ROOT / "outputs" / "ml" / "models" / "logistic_regression" / "metrics.json",
        ],
        "description": "Treina Regressao Logistica Multiclasse e exporta metricas.",
    },
    {
        "name": "Treinar segundo modelo",
        "script": "scripts/train_second_model.py",
        "outputs": [
            PROJECT_ROOT / "outputs" / "ml" / "models" / "random_forest" / "model.joblib",
            PROJECT_ROOT / "outputs" / "ml" / "models" / "random_forest" / "metrics.json",
        ],
        "description": "Treina Random Forest e exporta metricas e importancia das features.",
    },
    {
        "name": "Comparar modelos",
        "script": "scripts/compare_models.py",
        "outputs": [
            PROJECT_ROOT / "outputs" / "ml" / "comparison" / "model_comparison.csv",
            PROJECT_ROOT / "outputs" / "ml" / "comparison" / "best_model.json",
        ],
        "description": "Compara os modelos treinados e seleciona o melhor baseline.",
    },
]


@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


@st.cache_data
def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def required_outputs() -> list[Path]:
    return [
        STATISTICS_DIR / "kpi_summary.json",
        STATISTICS_DIR / "categorical_frequencies.csv",
        STATISTICS_DIR / "binary_frequencies.csv",
        STATISTICS_DIR / "numeric_statistics.csv",
        STATISTICS_DIR / "grouped_kpis.csv",
        STATISTICS_DIR / "correlation_matrix.csv",
        STATISTICS_DIR / "correlation_long.csv",
        DASHBOARD_DATA_DIR / "stress_score_values.csv",
        DASHBOARD_DATA_DIR / "burnout_score_by_work_mode.csv",
        DASHBOARD_DATA_DIR / "stress_burnout_sample.csv",
    ]


def missing_outputs() -> list[Path]:
    return [path for path in required_outputs() if not path.exists()]


def pipeline_file_status(path: Path) -> str:
    if not path.exists():
        return "ausente"
    return "gerado"


def run_pipeline_script(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, script],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def show_missing_outputs(missing: list[Path]) -> None:
    if not missing:
        return
    st.warning("Alguns arquivos analiticos ainda nao foram gerados. Use a aba Pipeline para executar os scripts.")
    with st.expander("Arquivos ausentes", expanded=False):
        for path in missing:
            st.write(f"- `{path.relative_to(PROJECT_ROOT)}`")


def render_pipeline_tab() -> None:
    st.subheader("Central de pipeline")
    st.markdown(
        '<p class="section-note">Execute os scripts sem sair do dashboard. '
        "Use os jobs em ordem quando quiser reconstruir tudo a partir do CSV.</p>",
        unsafe_allow_html=True,
    )

    status_rows = []
    for step in PIPELINE_STEPS:
        outputs = step["outputs"]
        if outputs:
            status = "gerado" if all(Path(path).exists() for path in outputs) else "pendente"
            output_text = ", ".join(str(Path(path).relative_to(PROJECT_ROOT)) for path in outputs)
        else:
            status = "executavel"
            output_text = "Banco PostgreSQL"
        status_rows.append(
            {
                "etapa": step["name"],
                "script": step["script"],
                "status": status,
                "saida_principal": output_text,
            }
        )

    st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)

    left, right = st.columns([1, 1])
    with left:
        if st.button("Executar pipeline completo", type="primary", use_container_width=True):
            logs: list[str] = []
            progress = st.progress(0)
            for index, step in enumerate(PIPELINE_STEPS, start=1):
                with st.status(f"Executando: {step['name']}", expanded=True) as status:
                    result = run_pipeline_script(step["script"])
                    if result.stdout:
                        st.code(result.stdout, language="text")
                    if result.stderr:
                        st.code(result.stderr, language="text")
                    logs.append(f"$ python {step['script']}\n{result.stdout}\n{result.stderr}")
                    if result.returncode != 0:
                        status.update(label=f"Falha em: {step['name']}", state="error")
                        st.error("Pipeline interrompido. Verifique o log acima.")
                        break
                    status.update(label=f"Concluido: {step['name']}", state="complete")
                progress.progress(index / len(PIPELINE_STEPS))
            else:
                st.cache_data.clear()
                st.success("Pipeline completo executado com sucesso. Recarregue a pagina se quiser atualizar todos os cards.")
            st.download_button(
                "Baixar log da execucao",
                data="\n\n".join(logs).encode("utf-8"),
                file_name="pipeline_execution_log.txt",
                mime="text/plain",
                use_container_width=True,
            )

    with right:
        st.info(
            "Para rotinas longas ou automatizadas, prefira os jobs Docker. "
            "Para uso exploratorio, os botoes abaixo sao mais praticos."
        )
        st.code(
            "docker compose up dashboard-export --abort-on-container-exit --exit-code-from dashboard-export\n"
            "docker compose up ml-problem-definition --abort-on-container-exit --exit-code-from ml-problem-definition",
            language="powershell",
        )

    st.divider()
    st.subheader("Executar etapa individual")
    for step in PIPELINE_STEPS:
        with st.container(border=True):
            col1, col2 = st.columns([0.72, 0.28])
            with col1:
                st.markdown(f"**{step['name']}**")
                st.caption(step["description"])
                st.code(f"python {step['script']}", language="powershell")
            with col2:
                if st.button("Executar", key=f"run-{step['script']}", use_container_width=True):
                    with st.status(f"Executando {step['name']}", expanded=True) as status:
                        result = run_pipeline_script(step["script"])
                        if result.stdout:
                            st.code(result.stdout, language="text")
                        if result.stderr:
                            st.code(result.stderr, language="text")
                        if result.returncode == 0:
                            st.cache_data.clear()
                            status.update(label="Etapa concluida", state="complete")
                        else:
                            status.update(label="Etapa falhou", state="error")
                            st.error(f"Codigo de saida: {result.returncode}")


def require_outputs() -> bool:
    required = required_outputs()
    missing = [path for path in required if not path.exists()]
    if missing:
        st.error("Arquivos analiticos ainda nao foram gerados.")
        st.code(
            "docker compose up dashboard-export --abort-on-container-exit --exit-code-from dashboard-export\n"
            "docker compose up -d streamlit",
            language="powershell",
        )
        for path in missing:
            st.write(f"- `{path.relative_to(PROJECT_ROOT)}`")
    return not missing


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
          .stApp {{
            background: {BG};
            color: {TEXT};
          }}
          .block-container {{
            padding-top: 1.5rem;
            padding-bottom: 2rem;
            max-width: 1480px;
          }}
          [data-testid="stSidebar"] {{
            background: #ffffff;
            border-right: 1px solid {LINE};
          }}
          h1, h2, h3 {{
            letter-spacing: 0;
            color: {TEXT};
          }}
          .page-header {{
            background: {PANEL};
            border: 1px solid {LINE};
            border-radius: 8px;
            padding: 20px 22px;
            margin-bottom: 18px;
          }}
          .page-header h1 {{
            margin: 0 0 6px 0;
            font-size: 30px;
            line-height: 1.15;
          }}
          .page-header p {{
            margin: 0;
            color: {MUTED};
            font-size: 14px;
          }}
          [data-testid="stMetric"] {{
            background: {PANEL};
            border: 1px solid {LINE};
            border-radius: 8px;
            padding: 14px 14px 12px;
            min-height: 96px;
          }}
          [data-testid="stMetricLabel"] {{
            color: {MUTED};
          }}
          [data-testid="stMetricValue"] {{
            color: {PRIMARY};
            font-size: 26px;
          }}
          div[data-testid="stTabs"] button p {{
            font-size: 14px;
          }}
          .section-note {{
            color: {MUTED};
            font-size: 13px;
            margin-top: -6px;
            margin-bottom: 10px;
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_layout(fig: go.Figure, height: int = 420) -> go.Figure:
    fig.update_layout(
        template=CHART_TEMPLATE,
        height=height,
        margin=dict(l=40, r=24, t=70, b=48),
        font=dict(family="Arial, sans-serif", size=13, color=TEXT),
        title_font=dict(size=18, color=TEXT),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
    )
    return fig


def number_pt(value: float | int, decimals: int = 2) -> str:
    if isinstance(value, int) or float(value).is_integer():
        return f"{int(value):,}".replace(",", ".")
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def percent(value: float) -> str:
    return f"{value:.2f}%".replace(".", ",")


def dataframe_download(df: pd.DataFrame, label: str, filename: str) -> None:
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def categorical_slice(df: pd.DataFrame, column: str) -> pd.DataFrame:
    return df[df["coluna"] == column].copy()


def filtered_work_modes(df: pd.DataFrame, selected_modes: list[str]) -> pd.DataFrame:
    if "work_mode" not in df.columns or not selected_modes:
        return df
    return df[df["work_mode"].isin(selected_modes)].copy()


inject_css()

outputs_missing = missing_outputs()

st.markdown(
    """
    <section class="page-header">
      <h1>Saude mental em tecnologia</h1>
      <p>Dashboard analitico com foco em burnout, estresse, rotina de trabalho e segmentacao de risco.</p>
    </section>
    """,
    unsafe_allow_html=True,
)

if outputs_missing:
    show_missing_outputs(outputs_missing)
    pipeline_only_tab = st.tabs(["Pipeline"])[0]
    with pipeline_only_tab:
        render_pipeline_tab()
    st.stop()

kpis = load_json(STATISTICS_DIR / "kpi_summary.json")
categorical = load_csv(STATISTICS_DIR / "categorical_frequencies.csv")
binary = load_csv(STATISTICS_DIR / "binary_frequencies.csv")
numeric_stats = load_csv(STATISTICS_DIR / "numeric_statistics.csv")
grouped_kpis = load_csv(STATISTICS_DIR / "grouped_kpis.csv")
correlation = load_csv(STATISTICS_DIR / "correlation_matrix.csv").set_index("variavel")
correlation_long = load_csv(STATISTICS_DIR / "correlation_long.csv")
stress_values = load_csv(DASHBOARD_DATA_DIR / "stress_score_values.csv")
burnout_work_mode = load_csv(DASHBOARD_DATA_DIR / "burnout_score_by_work_mode.csv")
scatter_sample = load_csv(DASHBOARD_DATA_DIR / "stress_burnout_sample.csv")

categorical["categoria"] = categorical["categoria"].astype(str)
binary["categoria"] = binary["categoria"].astype(str)

work_modes = sorted(scatter_sample["work_mode"].dropna().unique().tolist())
dimensions = sorted(grouped_kpis["dimensao"].unique().tolist())
metric_options = {
    "Burnout alto ou severo": "burnout_high_or_severe_pct",
    "Burnout severo": "burnout_severe_pct",
    "Burnout medio": "burnout_score_medio",
    "Estresse medio": "stress_score_medio",
    "Intencao de troca": "intencao_troca_emprego_pct",
    "Usa terapia": "usa_terapia_pct",
    "Busca suporte mental": "busca_suporte_mental_pct",
}

with st.sidebar:
    st.header("Filtros")
    selected_work_modes = st.multiselect("Modelo de trabalho", work_modes, default=work_modes)
    selected_dimension = st.selectbox(
        "Dimensao de segmento",
        dimensions,
        index=dimensions.index("work_mode") if "work_mode" in dimensions else 0,
    )
    selected_metric_label = st.selectbox("Metrica de ranking", list(metric_options.keys()))
    top_n = st.slider("Top segmentos", min_value=5, max_value=15, value=8, step=1)
    st.divider()
    dataframe_download(grouped_kpis, "Baixar KPIs segmentados", "grouped_kpis.csv")
    dataframe_download(numeric_stats, "Baixar estatisticas", "numeric_statistics.csv")

metric_cols = st.columns(6)
metric_cols[0].metric("Registros", number_pt(kpis["total_registros"]))
metric_cols[1].metric("Burnout severo", percent(kpis["burnout_severe_pct"]))
metric_cols[2].metric("Burnout alto/severo", percent(kpis["burnout_high_or_severe_pct"]))
metric_cols[3].metric("Estresse medio", number_pt(kpis["stress_score_medio"]))
metric_cols[4].metric("Horas semanais", number_pt(kpis["horas_semanais_media"]))
metric_cols[5].metric("Usa terapia", percent(kpis["usa_terapia_pct"]))

overview_tab, burnout_tab, segments_tab, correlation_tab, data_tab, pipeline_tab = st.tabs(
    ["Visao geral", "Burnout e rotina", "Segmentos", "Correlacoes", "Dados", "Pipeline"]
)

with overview_tab:
    left, right = st.columns([1.05, 0.95])

    with left:
        burnout = categorical_slice(categorical, "burnout_level")
        order = ["Low", "Moderate", "High", "Severe"]
        burnout["categoria"] = pd.Categorical(burnout["categoria"], categories=order, ordered=True)
        burnout = burnout.sort_values("categoria")
        fig = px.bar(
            burnout,
            x="categoria",
            y="frequencia_absoluta",
            color="categoria",
            text="frequencia_relativa_pct",
            title="Distribuicao dos niveis de burnout",
            labels={
                "categoria": "Nivel de burnout",
                "frequencia_absoluta": "Quantidade",
                "frequencia_relativa_pct": "Percentual",
            },
            color_discrete_sequence=[GREEN, "#9ab85f", GOLD, RED],
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(showlegend=False)
        st.plotly_chart(apply_layout(fig), use_container_width=True)

    with right:
        indicators = binary[binary["categoria"] == "1"].copy()
        label_map = {
            "ai_tools_daily": "Usa IA diariamente",
            "job_change_intention": "Intencao de troca",
            "seeks_mental_health_support": "Busca suporte mental",
            "therapy_access": "Acesso a terapia",
            "uses_therapy": "Usa terapia",
        }
        indicators["indicador"] = indicators["coluna"].map(label_map)
        indicators = indicators.sort_values("frequencia_relativa_pct", ascending=True)
        fig = px.bar(
            indicators,
            x="frequencia_relativa_pct",
            y="indicador",
            orientation="h",
            text="frequencia_relativa_pct",
            title="Indicadores binarios positivos",
            labels={"frequencia_relativa_pct": "Percentual", "indicador": ""},
            color="frequencia_relativa_pct",
            color_continuous_scale=["#d8ded8", PRIMARY],
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(apply_layout(fig), use_container_width=True)

    left, right = st.columns(2)
    with left:
        fig = px.histogram(
            stress_values,
            x="stress_score",
            nbins=30,
            title="Distribuicao do score de estresse",
            labels={"stress_score": "Score de estresse"},
            color_discrete_sequence=[BLUE],
        )
        fig.update_layout(yaxis_title="Quantidade")
        st.plotly_chart(apply_layout(fig), use_container_width=True)

    with right:
        top_roles = categorical_slice(categorical, "job_role").sort_values(
            "frequencia_absoluta", ascending=True
        ).tail(8)
        fig = px.bar(
            top_roles,
            x="frequencia_absoluta",
            y="categoria",
            orientation="h",
            text="frequencia_relativa_pct",
            title="Principais cargos representados",
            labels={"frequencia_absoluta": "Quantidade", "categoria": "Cargo"},
            color_discrete_sequence=[PLUM],
        )
        fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
        st.plotly_chart(apply_layout(fig), use_container_width=True)

with burnout_tab:
    filtered_burnout = filtered_work_modes(burnout_work_mode, selected_work_modes)
    filtered_scatter = filtered_work_modes(scatter_sample, selected_work_modes)

    left, right = st.columns(2)
    with left:
        fig = px.box(
            filtered_burnout,
            x="work_mode",
            y="burnout_score",
            color="work_mode",
            title="Burnout por modelo de trabalho",
            labels={"work_mode": "Modelo de trabalho", "burnout_score": "Score de burnout"},
            color_discrete_map=WORK_MODE_COLORS,
            points=False,
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(apply_layout(fig), use_container_width=True)

    with right:
        work_mode = grouped_kpis[grouped_kpis["dimensao"] == "work_mode"].copy()
        if selected_work_modes:
            work_mode = work_mode[work_mode["categoria"].isin(selected_work_modes)]
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=work_mode["categoria"],
                y=work_mode["burnout_score_medio"],
                name="Burnout medio",
                marker_color=RED,
            )
        )
        fig.add_trace(
            go.Bar(
                x=work_mode["categoria"],
                y=work_mode["stress_score_medio"],
                name="Estresse medio",
                marker_color=BLUE,
            )
        )
        fig.update_layout(
            title="Burnout e estresse medio por modelo de trabalho",
            xaxis_title="Modelo de trabalho",
            yaxis_title="Score medio",
            barmode="group",
        )
        st.plotly_chart(apply_layout(fig), use_container_width=True)

    fig = px.scatter(
        filtered_scatter,
        x="stress_score",
        y="burnout_score",
        color="work_mode",
        size="work_hours_per_week",
        hover_data=["sleep_hours_per_night"],
        opacity=0.55,
        title="Relacao entre estresse e burnout",
        labels={
            "stress_score": "Score de estresse",
            "burnout_score": "Score de burnout",
            "work_mode": "Modelo de trabalho",
            "work_hours_per_week": "Horas semanais",
            "sleep_hours_per_night": "Horas de sono",
        },
        color_discrete_map=WORK_MODE_COLORS,
    )
    st.plotly_chart(apply_layout(fig, height=560), use_container_width=True)

with segments_tab:
    metric_column = metric_options[selected_metric_label]
    segment = grouped_kpis[grouped_kpis["dimensao"] == selected_dimension].copy()
    segment = segment.sort_values(metric_column, ascending=False).head(top_n)
    segment_for_chart = segment.sort_values(metric_column, ascending=True)

    left, right = st.columns([1.1, 0.9])
    with left:
        fig = px.bar(
            segment_for_chart,
            x=metric_column,
            y="categoria",
            orientation="h",
            text=metric_column,
            title=f"Ranking por {selected_metric_label.lower()}",
            labels={metric_column: selected_metric_label, "categoria": selected_dimension},
            color=metric_column,
            color_continuous_scale=["#d8ded8", RED],
        )
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(apply_layout(fig, height=520), use_container_width=True)

    with right:
        scatter_segment = px.scatter(
            segment,
            x="stress_score_medio",
            y="burnout_score_medio",
            size="total_registros",
            color=metric_column,
            hover_name="categoria",
            title="Estresse medio x burnout medio",
            labels={
                "stress_score_medio": "Estresse medio",
                "burnout_score_medio": "Burnout medio",
                "total_registros": "Registros",
            },
            color_continuous_scale=["#d8ded8", RED],
        )
        st.plotly_chart(apply_layout(scatter_segment, height=520), use_container_width=True)

    st.dataframe(segment, use_container_width=True, hide_index=True)

with correlation_tab:
    left, right = st.columns([1.2, 0.8])
    with left:
        fig = px.imshow(
            correlation,
            text_auto=".2f",
            aspect="auto",
            color_continuous_scale="RdBu_r",
            zmin=-1,
            zmax=1,
            title="Mapa de calor de correlacao",
            labels={"color": "Correlacao"},
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(apply_layout(fig, height=680), use_container_width=True)

    with right:
        ranked_corr = correlation_long.copy()
        pair_key = ranked_corr.apply(
            lambda row: " | ".join(sorted([row["variavel_1"], row["variavel_2"]])),
            axis=1,
        )
        ranked_corr = ranked_corr.assign(par=pair_key).drop_duplicates("par")
        ranked_corr["abs_correlacao"] = ranked_corr["correlacao"].abs()
        ranked_corr = ranked_corr.sort_values("abs_correlacao", ascending=False).head(15)
        ranked_corr["par_label"] = ranked_corr["variavel_1"] + " x " + ranked_corr["variavel_2"]
        fig = px.bar(
            ranked_corr.sort_values("abs_correlacao", ascending=True),
            x="abs_correlacao",
            y="par_label",
            orientation="h",
            text="correlacao",
            title="Principais correlacoes absolutas",
            labels={"abs_correlacao": "Correlacao absoluta", "par_label": ""},
            color="correlacao",
            color_continuous_scale="RdBu_r",
        )
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        st.plotly_chart(apply_layout(fig, height=680), use_container_width=True)

with data_tab:
    left, right = st.columns(2)
    with left:
        st.subheader("Estatisticas numericas")
        st.dataframe(numeric_stats, use_container_width=True, hide_index=True)
        dataframe_download(numeric_stats, "Baixar estatisticas numericas", "numeric_statistics.csv")
    with right:
        st.subheader("Frequencias categoricas")
        st.dataframe(categorical, use_container_width=True, hide_index=True)
        dataframe_download(categorical, "Baixar frequencias categoricas", "categorical_frequencies.csv")

    left, right = st.columns(2)
    with left:
        st.subheader("Indicadores binarios")
        st.dataframe(binary, use_container_width=True, hide_index=True)
        dataframe_download(binary, "Baixar indicadores binarios", "binary_frequencies.csv")
    with right:
        st.subheader("KPIs segmentados")
        st.dataframe(grouped_kpis, use_container_width=True, hide_index=True)
        dataframe_download(grouped_kpis, "Baixar KPIs segmentados", "grouped_kpis.csv")

with pipeline_tab:
    render_pipeline_tab()
