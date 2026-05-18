# Resumo da analise estatistica

## KPIs principais

| KPI | Valor |
|---|---:|
| Total de registros | 100.000 |
| Stress score medio | 7.06 |
| Burnout score medio | 5.40 |
| Burnout severo | 28.58% |
| Burnout alto ou severo | 47.94% |
| Media de horas semanais | 47.10 |
| Sono medio em horas | 6.10 |
| Usa terapia | 15.19% |
| Busca suporte mental | 49.79% |
| Intencao de troca de emprego | 32.00% |

## Categorias dominantes

| Variavel | Moda | Frequencia relativa |
|---|---|---:|
| burnout_level | Severe | 28.58% |
| work_mode | Hybrid | 39.86% |

## Principais outliers

| Variavel | Outliers IQR | Percentual |
|---|---:|---:|
| `years_at_company` | 4.600 | 4.60% |
| `team_size` | 3.437 | 3.44% |
| `meetings_per_day` | 653 | 0.65% |
| `sleep_hours_per_night` | 634 | 0.63% |
| `deadline_pressure_score` | 535 | 0.54% |
| `salary_usd` | 430 | 0.43% |
| `work_hours_per_week` | 353 | 0.35% |
| `vacation_days_taken` | 275 | 0.28% |

## Arquivos gerados

| Arquivo | Uso recomendado |
|---|---|
| `numeric_statistics.csv` | Tabelas estatisticas, boxplots, histogramas e KPIs numericos. |
| `categorical_frequencies.csv` | Graficos de barras, filtros e segmentacoes categoricas. |
| `binary_frequencies.csv` | KPIs percentuais para indicadores 0/1. |
| `kpi_summary.csv` | Cards de indicadores gerais. |
| `kpi_summary.json` | Consumo por aplicacoes, dashboards ou APIs. |
| `grouped_kpis.csv` | Comparacoes por pais, cargo, senioridade, setor e modo de trabalho. |
| `correlation_matrix.csv` | Heatmap de correlacao. |
| `correlation_long.csv` | Ranking de correlacoes para analises e filtros. |