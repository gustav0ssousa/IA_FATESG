# Sprint 9 — KPIs e Observabilidade

## Objetivo

Adicionar histórico de consultas, indicadores operacionais e logs estruturados
para acompanhar uso, recuperação, latência e falhas do sistema RAG.

## Escopo

- Persistência de consultas RAG e fontes recuperadas.
- KPIs de volume, latência, erros, documentos e jobs.
- Série diária de sete dias e histórico recente.
- Logs JSON correlacionados por `request_id`.
- Área de indicadores no dashboard.

## Fora do escopo

- Prometheus, Grafana, Loki ou tracing distribuído.
- Alertas automáticos.
- Retenção e anonimização automáticas.
- Autenticação e controle de acesso aos indicadores.

## Decisões técnicas

- PostgreSQL como fonte dos KPIs do MVP.
- Fontes normalizadas para permitir ranking por documento.
- Agregações calculadas sob demanda, sem cache nesta escala.
- Logs JSON em stdout sem conteúdo da pergunta.
- Mesmo `request_id` no histórico, resposta e logs HTTP/RAG.

## Arquivos criados ou alterados

- `backend/apps/common/logging.py`
- `backend/apps/common/middleware.py`
- `backend/apps/rag/models.py`
- `backend/apps/rag/observability.py`
- `backend/apps/rag/views.py`
- `backend/apps/rag/urls.py`
- `backend/apps/rag/admin.py`
- `backend/apps/rag/migrations/0002_ragqueryrecord_ragquerysource.py`
- `backend/config/settings.py`
- `backend/tests/test_observability.py`
- `backend/tests/test_rag_query.py`
- `frontend/app/page.tsx`
- `frontend/app/globals.css`
- `frontend/tests/dashboard.spec.ts`
- `docker-compose.yml`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_9.md`

## Implementação

Subir a pilha aplica a migration automaticamente:

```bash
docker compose up -d --build
```

Consultar KPIs:

```bash
curl http://127.0.0.1:8000/api/rag/kpis/overview
```

Inspecionar logs estruturados:

```bash
docker compose logs -f api
```

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check

cd frontend
npm run lint
npm run build
npm run test:e2e
```

Resultados:

- `52 passed` no backend.
- `6 passed` nos testes E2E desktop/mobile.
- Lint e build de produção concluídos.
- Migration aplicada no PostgreSQL containerizado.
- Consulta real registrada com modelo `sabia-4` e duas fontes.
- Endpoint de KPIs validado após a consulta real.
- Logs JSON correlacionados pelo mesmo `request_id`.

## Documentação atualizada

- README com endpoint, dashboard e operação dos logs.
- ADR de observabilidade persistida e logs JSON.
- Plano de sprints e este registro.

## Checklist de conclusão

- [x] Histórico de queries persistido.
- [x] Fontes recuperadas persistidas e agregáveis.
- [x] KPIs e histórico recente expostos pela API.
- [x] Logs JSON correlacionados.
- [x] Dashboard operacional integrado.
- [x] Testes e documentação concluídos.

## Riscos e pendências

- O histórico persiste a pergunta completa e exige política de retenção,
  mascaramento e acesso antes de dados sensíveis.
- O endpoint agrega KPIs sob demanda; volumes maiores podem exigir cache ou
  métricas pré-agregadas.
- Logs ainda não são enviados para uma plataforma centralizada.
- Alertas e health checks de dependências ficam para evolução futura.

## Próxima sprint prevista

Sprint 10: hardening, revisão de segurança, limites, erros e documentação final.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
