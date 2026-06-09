# Sprint 6 - Avaliacao Minima e Testes de Fluxo

## Objetivo

Criar uma baseline reproduzivel para medir qualidade de retrieval e geracao,
detectar regressoes e definir criterios minimos para evolucao do RAG.

## Escopo

- Dataset JSON versionado.
- Casos com pergunta, fontes esperadas e termos esperados.
- Metricas de retrieval: Hit Rate, MRR e Precision@k.
- Metricas de geracao: Citation Rate e Answer Term Recall.
- Suporte a casos nao respondiveis e Refusal Accuracy.
- Quality gate com limites minimos.
- Comando Django `evaluate_rag`.
- Relatorios JSON e Markdown.
- Settings de teste isolados com SQLite em memoria.
- Testes automatizados do avaliador.
- Baseline real quando a infraestrutura estiver disponivel.

## Fora do escopo

- LLM-as-judge.
- Avaliacao humana estruturada em interface.
- Dataset amplo com documentos reais de producao.
- Metricas de latencia e custo.
- Reranking e busca hibrida.
- RabbitMQ.

## Decisoes tecnicas

- O dataset e versionado para permitir comparacao entre mudancas.
- Retrieval pode ser avaliado sem chamar a Maritaca.
- `--with-generation` habilita metricas de resposta e consome API.
- Casos respondiveis avaliam fonte esperada, ranking, citacao e termos.
- Casos nao respondiveis avaliam recusa sem fonte.
- O quality gate inicial exige:
  - Hit Rate >= `0.80`.
  - MRR >= `0.70`.
  - Citation Rate >= `0.80`, quando avaliada.
- Answer Term Recall >= `0.60`, quando avaliado.
- Erros de geracao = `0`.
- Duplicate Result Rate = `0`.
- Metricas nao executadas nao reprovam o quality gate.
- Pytest usa `config.test_settings` e SQLite em memoria, independente do `.env`.

## Arquivos criados ou alterados

- `pyproject.toml`
- `backend/config/test_settings.py`
- `backend/apps/rag/evaluation.py`
- `backend/apps/rag/management/commands/evaluate_rag.py`
- `backend/tests/test_evaluation.py`
- `data/evaluation/rag_cases.json`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_6.md`

## Implementacao

Avaliar retrieval:

```bash
.venv/bin/python backend/manage.py evaluate_rag
```

Avaliar retrieval e geracao:

```bash
.venv/bin/python backend/manage.py evaluate_rag --with-generation
```

Alterar parametros:

```bash
.venv/bin/python backend/manage.py evaluate_rag \
  --dataset data/evaluation/rag_cases.json \
  --output outputs/evaluation \
  --top-k 5
```

Artefatos gerados:

- `outputs/evaluation/evaluation.json`: metricas e resultados por caso.
- `outputs/evaluation/evaluation.md`: resumo legivel e quality gate.

## Como testar

```bash
.venv/bin/pytest
.venv/bin/python backend/manage.py check
.venv/bin/python backend/manage.py help evaluate_rag
docker compose config --quiet
```

Resultados obtidos:

- 39 testes automatizados aprovados.
- Suite isolada do PostgreSQL por settings de teste.
- Comando de avaliacao reconhecido pelo Django.
- Quality gate e relatorios validados com servicos falsos.
- PostgreSQL 17 e Qdrant 1.18.0 reais executados e validados.
- Documento de exemplo ingerido no PostgreSQL e indexado no Qdrant real.
- Baseline real de retrieval relevante:
  - Hit Rate: `1.000`.
  - MRR: `1.000`.
  - Precision@k: `1.000`.
- A validacao HTTP encontrou vetores duplicados da mesma fonte, deixados por uma
  base relacional anterior. O avaliador passou a medir e reprovar duplicacao.
- Duplicate Result Rate medido: `0.500`.
- Baseline real de geracao:
  - Citation Rate: `1.000`.
  - Answer Term Recall: `1.000`.
  - Generation Errors: `0`.
- Quality gate final reprovado somente pela duplicacao vetorial.
- O avaliador foi ajustado para concluir o relatorio mesmo com falha do provider.
- Cliente e servidor Qdrant alinhados na versao `1.18.0`.

## Documentacao atualizada

- README com execucao de avaliacao e quality gate.
- ADR da estrategia de avaliacao.
- Plano de sprints atualizado.
- Este relatorio da Sprint 6.

## Checklist de conclusao

- [x] Dataset inicial criado.
- [x] Metricas de retrieval implementadas.
- [x] Metricas de geracao implementadas.
- [x] Quality gate implementado.
- [x] Comando de avaliacao criado.
- [x] Relatorios JSON e Markdown implementados.
- [x] Testes isolados da infraestrutura externa.
- [x] Baseline real executada.

## Riscos e pendencias

- O dataset inicial possui poucos casos e uma unica fonte.
- Answer Term Recall nao mede equivalencia semantica.
- Citation Rate verifica formato, nao se a citacao realmente sustenta a frase.
- Avaliacao com geracao consome API e pode variar entre execucoes.
- A conta Maritaca precisa de creditos ativos para avaliar a geracao.
- O quality gate devera ser recalibrado com documentos reais.
- Casos nao respondiveis precisam ser adicionados ao dataset real.
- Falta uma rotina de reconciliacao para remover vetores orfaos do Qdrant.

## Proxima sprint prevista

Sprint 7 - Ingestao Assincrona com RabbitMQ: adicionar fila, worker, status de
jobs, retentativas e desacoplamento da indexacao.

Sprint finalizada. Aguardando o comando `continuar` para iniciar a proxima sprint.
