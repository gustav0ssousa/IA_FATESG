# Sprint 13 — Gate de Confiabilidade

## Objetivo

Impedir respostas sem contexto confiavel e fornecer reconciliacao idempotente
entre PostgreSQL e Qdrant.

## Escopo

- Score minimo configuravel para retrieval.
- Recusa antes da LLM para contexto vazio, fraco ou incompatível com
  fabricante/modelo explicitamente citado.
- Reconciliacao segura de vetores orfaos e chunks ausentes.
- Refusal Accuracy incorporada ao quality gate.
- Testes unitarios, de integracao e documentacao operacional.

## Fora do escopo

- Calibracao estatistica definitiva do score para novos acervos.
- Busca hibrida ou reranking.
- Correcao do E2E mobile.
- Ingestao integralmente assincrona.
- Auditoria por usuario e filtros.

## Decisões técnicas

- `RAG_MIN_RELEVANCE_SCORE=0.35` e o valor inicial, configuravel por ambiente.
- O filtro de score fica no `SemanticSearchService`, compartilhado por busca e
  consulta RAG.
- A compatibilidade explícita de fabricante/modelo e verificada antes da LLM.
- PostgreSQL e a fonte de verdade; somente chunks de documentos `indexed` sao
  esperados no Qdrant.
- `reconcile_qdrant` usa dry-run por padrao. Escritas exigem `--apply`.

## Arquivos criados ou alterados

- `backend/apps/rag/services.py`
- `backend/apps/rag/vector_store.py`
- `backend/apps/rag/evaluation.py`
- `backend/apps/rag/management/commands/reconcile_qdrant.py`
- `backend/config/settings.py`
- `backend/tests/test_rag_query.py`
- `backend/tests/test_rag_vector_store.py`
- `backend/tests/test_reconcile_qdrant.py`
- `backend/tests/test_evaluation.py`
- `.env.example`
- `README.md`
- `docs/ADR.md`
- `docs/FINAL_REVIEW_AND_ROADMAP.md`
- `docs/SPRINT_PLAN.md`
- `docs/TECHNICAL_MANUALS.md`
- `docs/SPRINT_13.md`

## Implementação

Validar divergencias sem alterar dados:

```bash
.venv/bin/python backend/manage.py reconcile_qdrant
```

Depois de revisar o resumo, remover pontos orfaos:

```bash
.venv/bin/python backend/manage.py reconcile_qdrant --apply
```

Remover orfaos e reindexar documentos com chunks ausentes:

```bash
.venv/bin/python backend/manage.py reconcile_qdrant \
  --apply \
  --reindex-missing
```

Reexecutar a avaliacao técnica, incluindo geração e recusas:

```bash
.venv/bin/python backend/manage.py evaluate_rag \
  --dataset data/evaluation/technical_manual_cases.json \
  --output outputs/evaluation/technical-manual \
  --with-generation
```

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check --settings=config.test_settings
.venv/bin/python backend/manage.py makemigrations --check --dry-run \
  --settings=config.test_settings
```

Resultados:

- `73 passed` no backend.
- Django check sem problemas.
- Nenhuma migration pendente.
- Reconciliação testada em dry-run, aplicação e segunda execução idempotente.
- Avaliação da base persistente não executada porque o daemon Docker estava
  indisponível.

## Documentação atualizada

- Variáveis, comandos de reconciliação e critérios do gate no README.
- Decisão arquitetural registrada em `ADR.md`.
- Operação especializada atualizada em `TECHNICAL_MANUALS.md`.
- Roadmap e revisão final alinhados ao estado real.

## Checklist de conclusão

- [x] Score minimo configuravel implementado.
- [x] Perguntas sem contexto confiavel nao chamam a LLM.
- [x] Incompatibilidade explícita de fabricante/modelo gera recusa.
- [x] Reconciliação idempotente implementada com dry-run seguro.
- [x] Refusal Accuracy incorporada ao quality gate.
- [x] Suite backend e documentação concluídas.
- [ ] Reconciliação aplicada na base persistente.
- [ ] Quality gate técnico certificado apos a reconciliação.

## Riscos e pendências

- O valor `0.35` precisa ser recalibrado quando embeddings ou acervo mudarem.
- Fabricantes/modelos fora das heuristicas conhecidas podem nao ativar a
  verificacao de compatibilidade.
- Pontos ausentes so sao reconstruidos com `--apply --reindex-missing`.
- Docker/Qdrant devem estar disponiveis para concluir a certificacao real.

## Próxima sprint prevista

Sprint 14 — Ingestao Assincrona e Ciclo de Vida.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
