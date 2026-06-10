# Sprint 10 — Hardening e Documentacao Final

## Objetivo

Preparar o MVP para apresentacao e evolucao, reduzindo exposicao de dados e
detalhes internos, fortalecendo configuracoes e consolidando a documentacao.

## Escopo

- Chave compartilhada opcional nas APIs publicas.
- Throttling e headers HTTP seguros.
- IDs de requisicao validados e erros padronizados.
- Mascaramento de perguntas nos KPIs.
- Credenciais locais parametrizadas no Compose.
- Melhoria de mensagens operacionais no dashboard.
- Documentacao de seguranca e demonstracao.

## Fora do escopo

- Login no dashboard, JWT, papeis ou multi-tenancy.
- TLS, WAF, gerenciador externo de segredos ou deploy cloud.
- Antivirus, DLP e politica automatica de retencao.
- Busca hibrida, reranking e cache de respostas.

## Decisões técnicas

- `API_ACCESS_KEY` vazia preserva o setup local; preenchida protege APIs
  publicas e e injetada pelo proxy Next.js.
- Health check permanece publico e embeddings permanecem isolados pela rede.
- Detalhes de provider ficam em logs/banco; clientes recebem erro controlado e
  `request_id`.
- Perguntas continuam auditaveis no PostgreSQL, mas ficam ocultas dos KPIs por
  padrao.
- Credenciais padrao do Compose sao apenas conveniencia local parametrizavel.

## Arquivos criados ou alterados

- `backend/apps/common/exceptions.py`
- `backend/apps/common/middleware.py`
- `backend/apps/common/permissions.py`
- `backend/apps/documents/views.py`
- `backend/apps/rag/observability.py`
- `backend/apps/rag/views.py`
- `backend/config/settings.py`
- `backend/tests/test_async_indexing.py`
- `backend/tests/test_observability.py`
- `backend/tests/test_security.py`
- `frontend/app/backend-api/[...path]/route.ts`
- `frontend/app/page.tsx`
- `.env.example`
- `docker-compose.yml`
- `README.md`
- `docs/ADR.md`
- `docs/CONTAINERS.md`
- `docs/DEMO_GUIDE.md`
- `docs/SECURITY.md`
- `docs/SPRINT_10.md`
- `docs/SPRINT_PLAN.md`

## Implementação

Subir a pilha completa:

```bash
docker compose up -d --build
```

Em ambiente compartilhado, definir no `.env`:

```dotenv
API_ACCESS_KEY=gere-uma-chave-longa
OBSERVABILITY_EXPOSE_QUESTION_TEXT=False
DJANGO_DEBUG=False
```

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check --settings=config.test_settings

cd frontend
npm run lint
npm run test:e2e
npm run build

docker compose config --quiet
docker compose ps
```

Resultados:

- `57 passed` no backend.
- `6 passed` nos testes E2E desktop/mobile.
- Lint e build de producao do frontend concluídos.
- Compose valido e sete servicos saudaveis.

## Documentação atualizada

- README consolidado com stack real, seguranca e status final.
- ADR de hardening incremental.
- Guia de seguranca e checklist de deploy.
- Guia reproduzivel de demonstracao.
- Operacao de containers atualizada.

## Checklist de conclusão

- [x] APIs publicas possuem protecao opcional e throttling.
- [x] Erros e IDs de requisicao foram endurecidos.
- [x] KPIs ocultam perguntas por padrao.
- [x] Containers e credenciais locais foram organizados.
- [x] Backend, frontend e Compose foram validados.
- [x] Documentacao final e roteiro de demonstracao foram concluídos.

## Riscos e pendências

- A chave compartilhada nao substitui autenticacao e autorizacao por usuario.
- Perguntas completas permanecem no PostgreSQL para auditoria.
- Checks de deploy ainda alertam quando valores locais inseguros estao ativos.
- Producao exige TLS, segredos fortes, retencao e monitoramento externo.

## Próxima sprint prevista

O MVP planejado esta concluido. Uma futura Sprint 11 pode implementar
autenticacao por usuario, papeis, retencao e deploy com TLS, mas nao foi iniciada.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
