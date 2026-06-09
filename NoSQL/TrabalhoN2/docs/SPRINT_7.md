# Sprint 7 — Indexacao Assincrona com RabbitMQ

## Objetivo

Executar a indexacao vetorial fora da requisicao HTTP, com fila, worker,
retentativas e estado operacional persistido.

## Escopo

- RabbitMQ no Docker Compose.
- Celery configurado no projeto Django.
- Worker para indexacao de documentos no Qdrant.
- Jobs persistidos no PostgreSQL.
- Retentativas com backoff exponencial.
- Endpoints para enfileirar e consultar jobs.
- Testes automatizados e validacao real ponta a ponta.

## Fora do escopo

- Ingestao de arquivos inteiramente assíncrona.
- Filas separadas por prioridade.
- Cancelamento manual de jobs.
- Deteccao automatica de jobs presos em `processing`.
- Reconciliacao de vetores antigos no Qdrant.
- Dashboard e monitoramento de producao.

## Decisões técnicas

- Celery 5.6.3 foi adotado por sua integracao madura com Django e RabbitMQ.
- RabbitMQ e o broker; resultados e estados de negocio ficam no PostgreSQL.
- `IndexingJob` registra estados `queued`, `processing`, `retrying`,
  `completed` e `failed`.
- O worker confirma mensagens tardiamente e processa uma por vez por processo,
  reduzindo perda de trabalho e picos de memoria durante embeddings.
- Falhas recebem ate tres retentativas com backoff exponencial limitado.
- O endpoint sincrono foi preservado para diagnostico e operacoes locais.

## Arquivos criados ou alterados

- `docker-compose.yml`
- `.env.example`
- `requirements.txt`
- `backend/config/settings.py`
- `backend/config/celery.py`
- `backend/config/__init__.py`
- `backend/apps/rag/models.py`
- `backend/apps/rag/migrations/0001_initial.py`
- `backend/apps/rag/admin.py`
- `backend/apps/rag/async_indexing.py`
- `backend/apps/rag/tasks.py`
- `backend/apps/rag/serializers.py`
- `backend/apps/rag/views.py`
- `backend/apps/rag/urls.py`
- `backend/tests/test_async_indexing.py`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_7.md`

## Implementação

Subir a infraestrutura e aplicar migrations:

```bash
docker compose up -d
.venv/bin/python backend/manage.py migrate
```

Iniciar o worker:

```bash
cd backend
../.venv/bin/celery -A config worker --loglevel=INFO --concurrency=1
```

Enfileirar um documento:

```bash
curl -X POST http://127.0.0.1:8000/api/rag/documents/UUID_DO_DOCUMENTO/index-async
```

Consultar o job retornado:

```bash
curl http://127.0.0.1:8000/api/rag/jobs/UUID_DO_JOB
```

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py makemigrations --check --dry-run
.venv/bin/python backend/manage.py check
docker compose ps
```

Resultado obtido:

- `45 passed`.
- Nenhum drift de migrations.
- PostgreSQL, Qdrant e RabbitMQ saudaveis.
- Job real publicado no RabbitMQ e concluido pelo Celery.
- Indexacao real concluida com `attempts=1` e `indexed_chunks=1`.

## Documentação atualizada

- README com setup do worker, endpoints, estados e variaveis Celery.
- ADR sobre Celery, RabbitMQ e persistencia dos jobs.
- Plano de sprints marcado como concluido.
- Este registro detalhado da Sprint 7.

## Checklist de conclusão

- [x] RabbitMQ configurado e saudavel.
- [x] Celery integrado ao Django.
- [x] Job de indexacao persistido.
- [x] Worker com retentativas implementado.
- [x] Endpoints assíncronos implementados.
- [x] Testes automatizados aprovados.
- [x] Fluxo real validado.
- [x] Documentacao atualizada.

## Riscos e pendências

- Um worker encerrado de forma abrupta pode deixar um job temporariamente em
  `processing`; uma rotina de reconciliacao deve ser adicionada futuramente.
- Credenciais padrao do RabbitMQ servem apenas para desenvolvimento local.
- O worker carrega o modelo de embeddings e deve receber limites de recursos em
  producao.
- Vetores antigos duplicados no Qdrant permanecem como pendencia anterior.
- Seguranca, TLS, autenticacao e observabilidade de producao ficam para sprints
  posteriores.

## Próxima sprint prevista

Sprint 8: dashboard inicial em Next.js e Tailwind para chat, fontes e estado de
indexacao.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
