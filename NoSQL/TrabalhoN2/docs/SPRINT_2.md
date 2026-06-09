# Sprint 2 - Persistencia Estruturada com PostgreSQL

## Objetivo

Adicionar a base de persistencia estruturada para documentos e chunks usando
Django ORM, PostgreSQL, migrations, Django Admin e uma camada fina de acesso a
dados.

## Escopo

- PostgreSQL 17 em Docker Compose.
- Configuracao de banco por `DATABASE_URL`.
- Driver Psycopg.
- Model `Document` com origem, hash, status, metadados e auditoria.
- Model `DocumentChunk` com conteudo, ordem, hash, pagina e metadados.
- Migration inicial.
- Django Admin para documentos e chunks.
- Repositorio para consulta por hash e substituicao transacional de chunks.
- Testes de models, constraints, cascata e repositorio.

## Fora do escopo

- Endpoints de documentos.
- Upload e extracao de arquivos.
- Chunking de texto.
- Qdrant e embeddings.
- LangChain e Maritaca.
- Historico de consultas RAG.

## Decisoes tecnicas

- PostgreSQL e o banco principal configurado no `.env.example`.
- SQLite permanece como fallback quando `DATABASE_URL` nao estiver definido,
  permitindo testes unitarios sem infraestrutura externa.
- IDs de documentos e chunks usam UUID para futura sincronizacao com Qdrant.
- `content_hash` do documento e unico para evitar duplicacao acidental.
- A posicao do chunk e unica dentro de cada documento.
- A exclusao de documento remove seus chunks por cascata.
- O conteudo dos chunks permanece no PostgreSQL para auditoria e reprocessamento.
- A substituicao de chunks ocorre em transacao para evitar estado parcial.

## Arquivos criados ou alterados

- `.env.example`
- `.gitignore`
- `requirements.txt`
- `docker-compose.yml`
- `backend/config/settings.py`
- `backend/apps/documents/apps.py`
- `backend/apps/documents/models.py`
- `backend/apps/documents/admin.py`
- `backend/apps/documents/repositories.py`
- `backend/apps/documents/migrations/0001_initial.py`
- `backend/tests/test_documents_models.py`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_2.md`

## Implementacao

Inicializacao esperada do PostgreSQL:

```bash
cp .env.example .env
docker compose up -d postgres
.venv/bin/python backend/manage.py migrate
```

O repositorio oferece:

- Busca de documento por hash de conteudo.
- Substituicao transacional dos chunks de um documento.

O Django Admin permite inspecionar documentos e chunks em `/admin/`.

## Como testar

Testes sem infraestrutura externa:

```bash
.venv/bin/pytest
.venv/bin/python backend/manage.py check
.venv/bin/python backend/manage.py makemigrations --check --dry-run
docker compose config --quiet
```

Validacao PostgreSQL quando o Docker estiver ativo:

```bash
docker compose up -d postgres
cp .env.example .env
.venv/bin/python backend/manage.py migrate
.venv/bin/python backend/manage.py check
```

Resultados obtidos:

- 8 testes aprovados.
- Django system check sem problemas.
- Nenhuma migration pendente.
- Migration inicial aplicada com sucesso no fallback SQLite.
- Configuracao Docker Compose validada.
- Integracao real com PostgreSQL nao executada porque o daemon Docker estava
  inacessivel neste ambiente.

## Documentacao atualizada

- README com setup PostgreSQL, models e comandos administrativos.
- ADR de PostgreSQL marcado como implementado.
- Plano de sprints atualizado.
- Este relatorio da Sprint 2.

## Checklist de conclusao

- [x] Docker Compose PostgreSQL criado.
- [x] Configuracao por `DATABASE_URL` implementada.
- [x] Models e constraints implementados.
- [x] Migration inicial criada.
- [x] Django Admin configurado.
- [x] Repositorio de documentos implementado.
- [x] Testes automatizados aprovados.
- [x] Integracao executada contra PostgreSQL real posteriormente na Sprint 6.

## Riscos e pendencias

- O daemon Docker precisa estar ativo para validar a conexao PostgreSQL real.
- As credenciais do `.env.example` servem apenas ao desenvolvimento local.
- Armazenar chunks no PostgreSQL e Qdrant duplica dados, mas melhora auditoria.
- O status `indexed` so deve ser usado depois da persistencia futura no Qdrant.
- Retencao, limpeza e versionamento de documentos ainda nao foram definidos.

## Proxima sprint prevista

Sprint 3 - Ingestao, Extracao e Chunking: implementar entrada de `.txt`, `.md` e
`.pdf`, normalizacao, geracao de chunks configuraveis e persistencia pelo
repositorio criado nesta sprint.

Sprint finalizada. Aguardando o comando `continuar` para iniciar a proxima sprint.
