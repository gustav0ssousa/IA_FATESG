# Definicao do problema de Machine Learning

- Variavel-alvo: `burnout_level`
- Tipo de problema: classificacao multiclasse
- Preditores principais: variaveis organizacionais, rotina de trabalho, bem-estar e suporte.
- Colunas removidas: identificadores, variaveis derivadas do alvo e possiveis consequencias do burnout.

## Arquivos gerados

| Arquivo | Conteudo |
|---|---|
| `ml_problem_definition.json` | Definicao estruturada do problema. |
| `target_distribution.csv` | Distribuicao da variavel-alvo. |
| `primary_predictors.csv` | Preditores principais da versao baseline. |
| `fairness_audit_columns.csv` | Colunas para auditoria de vies e segmentacao. |
| `leakage_columns.csv` | Colunas removidas por vazamento ou baixa adequacao. |
| `relevant_correlations_for_ml.csv` | Correlacoes uteis para justificar decisoes. |