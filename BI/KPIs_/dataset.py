import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

csv = r"C:\pr0jetos\IA_FATESG\BI\KPIs_\consulta.csv"

# Carregamento dos dados
data = pd.read_csv(csv, sep=";")


anos = [str(ano) for ano in range(2014, 2025)]
for ano in anos:
    data[ano] = data[ano].astype(str).str.replace('.', '', regex=False).astype(float)


df_docentes = data[data['Variável'] == 'Docentes - Total (número)'].drop(columns=['Variável'])
df_salas = data[data['Variável'] == 'Salas de Aula Utilizadas - Total (número)'].drop(columns=['Variável'])

# Transformar os anos de colunas para linhas (Melt)
doc_melt = df_docentes.melt(id_vars='Localidade', var_name='Ano', value_name='Docentes')
sal_melt = df_salas.melt(id_vars='Localidade', var_name='Ano', value_name='Salas')

# Unificar em uma única tabela
kpi_df = pd.merge(doc_melt, sal_melt, on=['Localidade', 'Ano'])
kpi_df['Ano'] = kpi_df['Ano'].astype(int)
kpi_df = kpi_df.sort_values(by=['Localidade', 'Ano']).reset_index(drop=True)

print("="*60)
print(" BASE MOLDADA PARA ANÁLISE ")
print("="*60)
print(kpi_df.head(), "\n")

# =====================================================================
# CÁLCULO DOS 3 KPIs ESTRATÉGICOS (Adequação da Formação Docente)
# =====================================================================

# KPI 1: Densidade Docente (Relação de Docentes por Sala de Aula)
kpi_df['KPI_1_Docente_por_Sala'] = round(kpi_df['Docentes'] / kpi_df['Salas'], 2)

# KPI 2: Crescimento Anual de Docentes (YoY %)
kpi_df['KPI_2_Cresc_Docentes_Percent'] = round(kpi_df.groupby('Localidade')['Docentes'].pct_change() * 100, 2)

# KPI auxiliar para cálculo (Crescimento Anual de Salas)
kpi_df['Aux_Cresc_Salas_Percent'] = round(kpi_df.groupby('Localidade')['Salas'].pct_change() * 100, 2)

# KPI 3: Descompasso de Crescimento (Crescimento de Docentes - Crescimento de Salas)
kpi_df['KPI_3_Descompasso_Docentes_vs_Salas_Percent'] = kpi_df['KPI_2_Cresc_Docentes_Percent'] - kpi_df['Aux_Cresc_Salas_Percent']

# Tratando valores NaN (primeiro ano não tem crescimento YoY)
kpi_df.fillna(0, inplace=True)

# Exibindo resultados formatados
print("="*80)
print(" RESULTADOS DOS INDICADORES ESTRATÉGICOS (KPIs) ")
print("="*80)

localidades = kpi_df['Localidade'].unique()
for local in localidades:
    print(f"\n---> LOCALIDADE: {local}")
    filtro = kpi_df[kpi_df['Localidade'] == local][['Ano', 'KPI_1_Docente_por_Sala', 'KPI_2_Cresc_Docentes_Percent', 'KPI_3_Descompasso_Docentes_vs_Salas_Percent']]
    print(filtro.to_string(index=False))

# Exportar os resultados caso queira usar os KPIs no PowerBI ou Metabase posteriormente
output_csv = r"C:\pr0jetos\IA_FATESG\BI\KPIs_\kpi_adequacao_docente_resultados.csv"
kpi_df.to_csv(output_csv, index=False, sep=";", decimal=",")
print(f"\n[!] Cópia exportada com sucesso para: {output_csv}")

# =====================================================================
# GERAÇÃO DE GRÁFICOS DOS KPIs
# =====================================================================
print("\nGerando gráficos visuais (Salvando Dashboard)...")

sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(3, 1, figsize=(10, 15))

# Gráfico KPI 1: Densidade Docente
sns.lineplot(data=kpi_df, x='Ano', y='KPI_1_Docente_por_Sala', hue='Localidade', marker='o', ax=axes[0])
axes[0].set_title('KPI 1: Relação de Docentes por Sala de Aula', fontsize=12, fontweight='bold')
axes[0].set_ylabel('Docentes / Sala')
axes[0].set_xticks(kpi_df['Ano'].unique())

# Gráfico KPI 2: Crescimento Anual Docente (YoY)
sns.barplot(data=kpi_df, x='Ano', y='KPI_2_Cresc_Docentes_Percent', hue='Localidade', ax=axes[1])
axes[1].set_title('KPI 2: Crescimento Anual de Docentes (YoY %)', fontsize=12, fontweight='bold')
axes[1].set_ylabel('Crescimento (%)')
axes[1].axhline(0, color='black', linewidth=1)

# Gráfico KPI 3: Descompasso de Crescimento
sns.barplot(data=kpi_df, x='Ano', y='KPI_3_Descompasso_Docentes_vs_Salas_Percent', hue='Localidade', palette='magma', ax=axes[2])
axes[2].set_title('KPI 3: Descompasso Docentes vs Salas (%)', fontsize=12, fontweight='bold')
axes[2].set_ylabel('Descompasso (%)')
axes[2].axhline(0, color='black', linewidth=1)

plt.tight_layout()

output_img = r"C:\pr0jetos\IA_FATESG\BI\KPIs_\kpi_dashboard.png"
plt.savefig(output_img, dpi=300, bbox_inches='tight')
print(f"[!] Dashboard gerado com sucesso em: {output_img}\n")

# Se você estiver chamando o script por linha de comando em IDE, o show() pode abrir a janela.
plt.show()