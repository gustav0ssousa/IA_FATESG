import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Configuração da página
st.set_page_config(
    page_title="Dashboard KPIs - Adequação Docente",
    page_icon="📊",
    layout="wide"
)

# Caminho do arquivo CSV
SCRIPT_DIR = Path(__file__).parent
CSV_FILE = SCRIPT_DIR / "consulta.csv"

# Carregamento dos dados
@st.cache_data
def load_data():
    data = pd.read_csv(CSV_FILE, sep=";")

    # Converter valores de texto para numérico
    anos = [str(ano) for ano in range(2014, 2025)]
    for ano in anos:
        data[ano] = data[ano].astype(str).str.replace('.', '', regex=False).astype(float)

    return data

data = load_data()

# Separar os dados
df_docentes = data[data['Variável'] == 'Docentes - Total (número)'].drop(columns=['Variável'])
df_salas = data[data['Variável'] == 'Salas de Aula Utilizadas - Total (número)'].drop(columns=['Variável'])

# Transformar formato wide para long
doc_melt = df_docentes.melt(id_vars='Localidade', var_name='Ano', value_name='Docentes')
sal_melt = df_salas.melt(id_vars='Localidade', var_name='Ano', value_name='Salas')

# Unificar
kpi_df = pd.merge(doc_melt, sal_melt, on=['Localidade', 'Ano'])
kpi_df['Ano'] = kpi_df['Ano'].astype(int)
kpi_df = kpi_df.sort_values(by=['Localidade', 'Ano']).reset_index(drop=True)

# Calcular KPIs
kpi_df['KPI_1_Docente_por_Sala'] = round(kpi_df['Docentes'] / kpi_df['Salas'], 2)
kpi_df['KPI_2_Cresc_Docentes_Percent'] = round(kpi_df.groupby('Localidade')['Docentes'].pct_change() * 100, 2)
kpi_df['Aux_Cresc_Salas_Percent'] = round(kpi_df.groupby('Localidade')['Salas'].pct_change() * 100, 2)
kpi_df['KPI_3_Descompasso_Docentes_vs_Salas_Percent'] = (
    kpi_df['KPI_2_Cresc_Docentes_Percent'] - kpi_df['Aux_Cresc_Salas_Percent']
)
kpi_df.fillna(0, inplace=True)

# Título
st.title("📊 Dashboard de KPIs - Adequação da Formação Docente")
st.markdown("---")

# Sidebar com filtros
st.sidebar.header("Filtros")
localidades = kpi_df['Localidade'].unique()
localidade_selecionada = st.sidebar.selectbox(
    "Localidade",
    ["Todas"] + list(localidades),
    index=0
)

# Filtrar dados
if localidade_selecionada != "Todas":
    dados_filtrados = kpi_df[kpi_df['Localidade'] == localidade_selecionada].copy()
else:
    dados_filtrados = kpi_df.copy()

# Métricas de resumo (último ano disponível)
ultimo_ano = dados_filtrados['Ano'].max()
dados_ultimo_ano = dados_filtrados[dados_filtrados['Ano'] == ultimo_ano]

st.subheader(f"📈 Visão Geral ({ultimo_ano})")
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_docentes = dados_ultimo_ano['Docentes'].sum()
    st.metric("Total de Docentes", f"{total_docentes:,.0f}")

with col2:
    total_salas = dados_ultimo_ano['Salas'].sum()
    st.metric("Total de Salas", f"{total_salas:,.0f}")

with col3:
    media_kpi1 = dados_ultimo_ano['KPI_1_Docente_por_Sala'].mean()
    st.metric("Média Docentes/Sala", f"{media_kpi1:.2f}")

with col4:
    cresc_docentes = dados_ultimo_ano['KPI_2_Cresc_Docentes_Percent'].mean()
    st.metric("Crescimento Médio Docentes", f"{cresc_docentes:.1f}%")

st.markdown("---")

# KPI 1: Densidade Docente
st.subheader("🎯 KPI 1: Densidade Docente (Docentes por Sala)")
st.markdown("*Relação entre número de docentes e salas de aula utilizadas*")

fig_kpi1 = px.line(
    dados_filtrados,
    x='Ano',
    y='KPI_1_Docente_por_Sala',
    color='Localidade',
    markers=True,
    title='Evolução da Densidade Docente por Ano',
    labels={'KPI_1_Docente_por_Sala': 'Docentes / Sala', 'Ano': 'Ano'}
)
fig_kpi1.update_layout(height=400)
st.plotly_chart(fig_kpi1, use_container_width=True)

# KPI 2: Crescimento Anual de Docentes
st.subheader("📈 KPI 2: Crescimento Anual de Docentes (YoY %)")
st.markdown("*Variação percentual anual no número de docentes*")

fig_kpi2 = px.bar(
    dados_filtrados,
    x='Ano',
    y='KPI_2_Cresc_Docentes_Percent',
    color='Localidade',
    title='Crescimento Anual de Docentes (%)',
    labels={'KPI_2_Cresc_Docentes_Percent': 'Crescimento (%)', 'Ano': 'Ano'}
)
fig_kpi2.add_hline(y=0, line_dash="solid", line_color="black")
fig_kpi2.update_layout(height=400)
st.plotly_chart(fig_kpi2, use_container_width=True)

# KPI 3: Descompasso
st.subheader("⚠️ KPI 3: Descompasso de Crescimento (Docentes vs Salas)")
st.markdown("*Diferença entre crescimento de docentes e crescimento de salas*")
st.info("""
**Como interpretar:**
- **Positivo (+)**: Docentes crescendo mais que salas (adequação)
- **Negativo (-)**: Salas crescendo mais que docentes (risco de déficit)
""")

fig_kpi3 = px.bar(
    dados_filtrados,
    x='Ano',
    y='KPI_3_Descompasso_Docentes_vs_Salas_Percent',
    color='Localidade',
    color_continuous_scale='RdYlGn',
    title='Descompasso de Crescimento (%)',
    labels={'KPI_3_Descompasso_Docentes_vs_Salas_Percent': 'Descompasso (%)', 'Ano': 'Ano'}
)
fig_kpi3.add_hline(y=0, line_dash="solid", line_color="black")
fig_kpi3.update_layout(height=400)
st.plotly_chart(fig_kpi3, use_container_width=True)

# Tabela de dados
st.markdown("---")
st.subheader("📋 Dados Detalhados")

with st.expander("Ver tabela completa dos KPIs"):
    display_cols = ['Localidade', 'Ano', 'Docentes', 'Salas',
                    'KPI_1_Docente_por_Sala', 'KPI_2_Cresc_Docentes_Percent',
                    'KPI_3_Descompasso_Docentes_vs_Salas_Percent']
    st.dataframe(
        dados_filtrados[display_cols].sort_values(['Localidade', 'Ano']),
        use_container_width=True,
        hide_index=True
    )

# Download dos dados
st.markdown("---")
csv_download = dados_filtrados.to_csv(index=False, sep=";").encode('utf-8')
st.download_button(
    label="📥 Baixar dados filtrados em CSV",
    data=csv_download,
    file_name=f"kpi_dashboard_{localidade_selecionada.lower()}.csv",
    mime="text/csv"
)
