# Sprint 14 — Ingestao Assincrona e Ciclo de Vida

## Objetivo

Processar manuais fora da requisicao HTTP e permitir revisao, reindexacao e
reprocessamento a partir do arquivo original.

## Escopo

- Hash de upload calculado em streaming.
- Arquivo original persistido em volume compartilhado.
- Extracao, chunking e indexacao executados pelo Celery.
- Reprocessamento idempotente.
- Edicao e normalizacao de metadados tecnicos.
- Dashboard com acompanhamento, revisao e reprocessamento.

## Fora do escopo

- Object storage externo e extracao incremental.
- Remocao administrativa de documentos e arquivos.
- Auditoria detalhada por usuario.

## Decisões técnicas

- Reaproveitar `IndexingJob` para o pipeline completo.
- `Document.file` mantem o artefato necessário para reprocessamento.
- API e worker compartilham o volume `document_uploads`.
- Corrigir metadados atualiza chunks e agenda reindexacao.
- O endpoint de ingestao retorna `202 Accepted` com documento e job.

## Arquivos criados ou alterados

- `backend/apps/documents/models.py`
- `backend/apps/documents/migrations/0003_document_file.py`
- `backend/apps/documents/services.py`
- `backend/apps/documents/serializers.py`
- `backend/apps/documents/views.py`
- `backend/apps/documents/urls.py`
- `backend/apps/rag/tasks.py`
- `backend/config/settings.py`
- `backend/tests/test_ingestion.py`
- `backend/tests/test_async_indexing.py`
- `backend/tests/test_document_lifecycle.py`
- `frontend/app/page.tsx`
- `frontend/app/globals.css`
- `frontend/playwright.config.ts`
- `docker-compose.yml`
- `.env.example`
- `.gitignore`
- `README.md`
- `docs/ADR.md`
- `docs/CONTAINERS.md`
- `docs/FINAL_REVIEW_AND_ROADMAP.md`
- `docs/SPRINT_PLAN.md`
- `docs/TECHNICAL_MANUALS.md`
- `docs/SPRINT_14.md`

## Implementação

O upload ja publica o pipeline:

```bash
curl -X POST http://127.0.0.1:8000/api/documents/ingest \
  -F "file=@docs/sm_elfb_e_ver2.pdf"
```

Corrigir metadados:

```bash
curl -X PATCH http://127.0.0.1:8000/api/documents/UUID \
  -H "Content-Type: application/json" \
  -d '{"manufacturer":"Brother","models":["MFC-L5710DN"]}'
```

Reprocessar o arquivo original:

```bash
curl -X POST http://127.0.0.1:8000/api/documents/UUID/reprocess
```

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check --settings=config.test_settings
.venv/bin/python backend/manage.py makemigrations --check --dry-run \
  --settings=config.test_settings
cd frontend && npm run lint && npm run build && npm run test:e2e -- --workers=1
docker compose config --quiet
```

Resultados:

- `77 passed` no backend.
- Django check sem problemas e migration consistente.
- Lint e build de producao aprovados.
- Configuracao do Compose aprovada.
- E2E nao certificado nesta execucao por exaustao de memoria do ambiente ao
  iniciar Chromium e servidor Next.js simultaneamente.
- Pilha real reconstruida com `docker compose up -d --build`; API, frontend,
  worker, PostgreSQL, Qdrant, RabbitMQ e embeddings ficaram saudaveis.
- API respondeu `200` em `/api/health`, frontend respondeu `200` e
  `migrate --check` confirmou ausencia de migrations pendentes.
- Reconciliacao Qdrant em dry-run encontrou `1` vetor orfao legado; nenhuma
  remocao automatica foi aplicada.

## Documentação atualizada

- README com novo contrato de upload e endpoints de ciclo de vida.
- ADR do pipeline integral no worker.
- Containers, manuais tecnicos, roadmap e plano de sprints atualizados.

## Checklist de conclusão

- [x] Upload persistido antes do processamento pesado.
- [x] Extracao, chunking e indexacao executam no worker.
- [x] Reprocessamento idempotente disponivel.
- [x] Metadados podem ser revisados e reindexados.
- [x] API e worker compartilham arquivos no Compose.
- [x] Backend, lint, build e documentacao validados.
- [x] Infraestrutura real iniciada e certificada com Docker, RabbitMQ e Qdrant.
- [ ] E2E desktop/mobile certificado em ambiente com memoria disponivel.

## Riscos e pendências

- Arquivos legados sem `Document.file` nao podem ser reprocessados.
- O worker ainda le o conteudo exigido pelos extratores em memoria.
- E necessario definir retencao e remocao de arquivos originais.
- Reprocessamentos concorrentes devem ser evitados ate existir trava explícita.
- A base persistente possui `1` vetor orfao identificado pelo dry-run; revisar e
  executar `reconcile_qdrant --apply` antes do quality gate final.

## Próxima sprint prevista

Sprint 15 — Auditoria, Deploy e Fechamento.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
