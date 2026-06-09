# Sprint 1 - Fundacao Backend

## Objetivo

Confirmar o framework backend e criar uma fundacao Django/DRF modular,
configuravel, testavel e pronta para receber os componentes RAG.

## Escopo

- Confirmacao de Django 5.2 LTS com Django REST Framework.
- Projeto Django e app inicial `common`.
- Endpoint publico `GET /api/health`.
- Configuracao por variaveis de ambiente.
- Ambiente virtual e dependencias reproduziveis.
- Testes automatizados com Pytest e Pytest-Django.
- Documentacao de setup, execucao e testes.

## Fora do escopo

- PostgreSQL e modelos de dominio.
- Qdrant, embeddings e LangChain.
- Integracao com Maritaca.
- RabbitMQ e workers.
- Autenticacao da API.
- Dashboard.

## Decisoes tecnicas

- Django 5.2 LTS foi escolhido pela estabilidade e suporte prolongado.
- Django REST Framework sera usado para os contratos HTTP da API.
- A estrutura usa apps por dominio para evitar concentrar regras em um unico app.
- O app `common` possui apenas recursos compartilhados, comecando pelo health
  check.
- SQLite e usado somente como fallback local desta sprint. PostgreSQL entra na
  Sprint 2.
- LangChain nao foi instalado antecipadamente; entrara apenas quando houver
  componentes RAG para integrar.
- O health check nao exige autenticacao e nao consulta dependencias externas
  nesta fase.

## Arquivos criados ou alterados

- `.env.example`
- `.gitignore`
- `requirements.txt`
- `pyproject.toml`
- `backend/manage.py`
- `backend/config/settings.py`
- `backend/config/urls.py`
- `backend/config/asgi.py`
- `backend/config/wsgi.py`
- `backend/apps/common/apps.py`
- `backend/apps/common/urls.py`
- `backend/apps/common/views.py`
- `backend/tests/test_health.py`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_1.md`

## Implementacao

O endpoint atual responde:

```http
GET /api/health
```

```json
{
  "status": "ok",
  "service": "adaptive-rag-api",
  "environment": "local"
}
```

Comandos principais:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/python backend/manage.py migrate
.venv/bin/python backend/manage.py runserver
```

## Como testar

```bash
.venv/bin/python backend/manage.py check
.venv/bin/pytest
```

Resultado obtido na conclusao da sprint:

- Django system check: nenhum problema identificado.
- Pytest: 2 testes aprovados.
- Compilacao Python: concluida sem erros.
- `git diff --check`: concluido sem erros.

## Documentacao atualizada

- README com stack confirmada, setup local, endpoint, testes e variaveis.
- ADR de Django/DRF marcado como aceito.
- Plano de sprints com Sprint 1 concluida.
- Este relatorio de sprint.

## Checklist de conclusao

- [x] Django + DRF confirmado como backend.
- [x] Estrutura modular inicial criada.
- [x] Configuracao por ambiente implementada.
- [x] Health check implementado.
- [x] Testes automatizados adicionados e aprovados.
- [x] Setup e comandos documentados.
- [x] Decisao arquitetural atualizada.

## Riscos e pendencias

- O valor padrao de `DJANGO_SECRET_KEY` e apenas para desenvolvimento local.
- O health check ainda nao verifica PostgreSQL, Qdrant ou RabbitMQ.
- SQLite sera substituido por PostgreSQL na proxima sprint.
- Autenticacao e limites da API ainda nao foram implementados.
- Logs estruturados serao adicionados em sprint posterior.

## Proxima sprint prevista

Sprint 2 - Persistencia Estruturada com PostgreSQL: adicionar container local,
configuracao de banco, models, migrations, Django Admin e testes de persistencia.

Sprint finalizada. Aguardando o comando `continuar` para iniciar a proxima sprint.
