# Sprint 8 — Dashboard Inicial

## Objetivo

Criar uma interface responsiva para consultar o RAG, inspecionar fontes, enviar
documentos e acompanhar a indexacao assíncrona.

## Escopo

- Dashboard Next.js com React e Tailwind CSS.
- Chat RAG com respostas e fontes.
- Upload e listagem persistente de documentos.
- Acompanhamento de jobs de indexacao.
- Layout desktop/mobile e testes E2E.

## Fora do escopo

- Autenticacao e perfis.
- Historico persistente de conversas.
- Exclusao de documentos.
- KPIs e deploy de producao.

## Decisões técnicas

- Next.js 16, React 19, Tailwind CSS 4 e icones Lucide.
- Proxy `/backend-api/*` para a API Django, evitando CORS local.
- `GET /api/documents/` para carregar documentos persistidos.
- Polling de jobs ativos ate conclusao ou falha.
- Fontes laterais no desktop e sob demanda no mobile.

## Arquivos criados ou alterados

- `frontend/`
- `backend/apps/documents/serializers.py`
- `backend/apps/documents/views.py`
- `backend/apps/documents/urls.py`
- `backend/tests/test_document_list.py`
- `.gitignore`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_8.md`

## Implementação

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Dashboard: `http://127.0.0.1:3000`.

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check

cd frontend
npm run lint
npm run test:e2e
npm run build
```

Resultados:

- `46 passed` no backend.
- `4 passed` nos testes E2E desktop/mobile.
- Lint sem erros ou avisos.
- Build de producao concluido.
- Consulta real via proxy respondeu com `sabia-4` e duas fontes.
- Upload e indexacao via proxy terminaram em `completed`.
- Screenshots inspecionadas em `outputs/sprint8/`.

## Documentação atualizada

- README com execucao e testes do dashboard.
- ADR do proxy Next.js para Django.
- Plano de sprints e este registro.

## Checklist de conclusão

- [x] Dashboard criado.
- [x] Chat RAG e fontes integrados.
- [x] Upload e indexacao integrados.
- [x] Documentos persistidos listados.
- [x] Desktop e mobile validados.
- [x] Testes e documentacao concluidos.

## Riscos e pendências

- O dashboard ainda nao possui autenticacao ou historico de conversas.
- A listagem retorna os 100 documentos mais recentes e nao possui paginacao.
- O `npm audit` reporta duas vulnerabilidades moderadas transitivas no PostCSS
  empacotado pelo Next.js; a correcao sugerida exige downgrade incompatível.
- Health checks consolidados ficam para observabilidade.
- Rebuilds com a pilha ativa requerem ao menos 2 GiB disponíveis ao Docker; o
  runtime foi validado no limite local de aproximadamente 1 GiB.

## Adendo de infraestrutura

Antes da Sprint 9, a pilha completa foi containerizada e organizada:

- API Django com Gunicorn e migrations automaticas.
- Worker Celery usando a mesma imagem da API.
- Frontend Next.js em build standalone.
- Containers da aplicacao executados como usuarios sem privilegios.
- Health checks e dependencias condicionadas por saude.
- PostgreSQL, Qdrant e RabbitMQ isolados em rede interna.
- Volumes persistentes e cache de embeddings compartilhado.
- Inicializacao controlada das permissoes do cache para o backend sem root.
- Gunicorn ajustado para nao duplicar o modelo ONNX em memoria no ambiente local.
- Modelo ONNX centralizado em servico interno para controlar memoria e latencia.

Validacoes do adendo:

- Compose validado e sete servicos de longa duracao saudaveis.
- Health da API acessivel diretamente e pelo proxy Next.js.
- API, worker e frontend confirmados como usuarios sem privilegios.
- Fluxo assíncrono validado pela pilha containerizada.
- Consulta RAG real validada pelo proxy do frontend.
- Guia operacional registrado em `docs/CONTAINERS.md`.

## Próxima sprint prevista

Sprint 9: KPIs, historico de queries, logs estruturados e sinais operacionais.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
