# Documentacao do projeto

Este diretorio reune os passos do projeto de ETL, analise estatistica, visualizacao de dados e Machine Learning usando a base `mental_health_burnout_tech_2026.csv`.

## Arquivos

| Arquivo | Conteudo |
|---|---|
| `00_ambiente_execucao.md` | Como executar o projeto com Docker, scripts e ambiente virtual Python. |
| `00_avaliacao_banco_dados.md` | Avaliacao entre PostgreSQL, MongoDB e outras opcoes para o projeto. |
| `00_importacao_banco.md` | Registro da importacao inicial em SQLite. Mantido apenas como historico, pois SQLite nao sera a escolha oficial. |
| `01_compreensao_dataset.md` | Passo 1 do projeto: compreensao inicial do dataset. |
| `02_etl.md` | Passo 2 do projeto: extracao, transformacao e carga usando PostgreSQL. |
| `03_analise_estatistica.md` | Passo 3 do projeto: analise estatistica descritiva das variaveis numericas e categoricas. |
| `04_dashboard_visual.md` | Passo 4 do projeto: dashboard visual com graficos interativos em Plotly. |
| `04_dashboard_melhorias.md` | Analise e melhorias aplicadas no dashboard Streamlit. |
| `05_definicao_ml.md` | Passo 5 do projeto: definicao do problema de Machine Learning. |
| `06_preparacao_ml.md` | Passo 6 do projeto: preparacao de dados para Machine Learning. |
| `07_primeiro_algoritmo.md` | Passo 7 do projeto: treinamento da Regressao Logistica Multiclasse. |
| `08_segundo_algoritmo.md` | Passo 8 do projeto: treinamento do Random Forest Classifier. |
| `09_comparacao_algoritmos.md` | Passo 9 do projeto: comparacao dos algoritmos treinados. |
| `10_conclusao_final.md` | Passo 10 do projeto: conclusao final e resumo geral. |

## Scripts

| Script | Finalidade |
|---|---|
| `scripts/clean_data.py` | Recria a tabela tratada a partir da tabela bruta e executa validacoes de qualidade. |
| `scripts/statistical_analysis.py` | Gera estatisticas, frequencias, KPIs e correlacoes para graficos, BI e proximas etapas analiticas. |
| `scripts/dashboard_visual.py` | Gera datasets visuais, graficos interativos e pagina HTML do dashboard. |
| `scripts/define_ml_problem.py` | Define alvo, tipo de problema, preditores e colunas removidas por vazamento. |
| `scripts/prepare_ml_data.py` | Prepara treino/teste, preprocessador e matrizes para Machine Learning. |
| `scripts/train_first_model.py` | Treina o primeiro algoritmo de ML e exporta metricas. |
| `scripts/train_second_model.py` | Treina o segundo algoritmo de ML e exporta metricas. |
| `scripts/compare_models.py` | Compara os modelos treinados e seleciona o melhor baseline. |
| `app/streamlit_app.py` | Dashboard Streamlit interativo. |

## Saidas analiticas

| Pasta | Conteudo |
|---|---|
| `outputs/statistics/` | Arquivos CSV, JSON e Markdown gerados pelo script de analise estatistica. |
| `outputs/dashboard/` | Dashboard HTML, graficos Plotly e datasets usados nas visualizacoes. |
| `outputs/ml/` | Definicoes e metadados usados nas etapas de Machine Learning. |

## Execucao rapida

```powershell
docker compose build
docker compose up dashboard-export --abort-on-container-exit --exit-code-from dashboard-export
docker compose up -d streamlit
```

Acesse:

```text
http://localhost:8501
```

## Banco de dados recomendado

O banco recomendado para este projeto e **PostgreSQL**.

## Motivo da escolha

A base `mental_health_burnout_tech_2026.csv` e tabular, estruturada, com colunas bem definidas e foco em ETL, estatistica, dashboard e Machine Learning. Para esse perfil, PostgreSQL oferece melhor aderencia que MongoDB porque trabalha naturalmente com SQL, tipos fortes, restricoes, indices, views e consultas analiticas.

MongoDB continua sendo uma alternativa robusta, mas e mais indicado quando os dados sao documentos flexiveis, aninhados ou com esquema variavel.

## Proximo passo tecnico

Definir o problema de Machine Learning no Passo 5, escolhendo a variavel-alvo e as variaveis preditoras.
