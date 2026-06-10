# Sistema RAG Adaptativo - Trabalho N2

Este projeto sera desenvolvido em sprints para criar um sistema RAG
(`Retrieval-Augmented Generation`) funcional, modular, testavel e evolutivo.

## Stack planejada

| Campo | Definicao inicial |
| --- | --- |
| Linguagem | Python no backend; TypeScript se houver frontend |
| Framework backend | Django 5.2 LTS + Django REST Framework |
| Framework frontend | Next.js 16 + React 19 + Tailwind CSS 4 |
| LLM provider | Maritaca, com interface desacoplada para troca futura |
| Modelo de embedding | A definir na implementacao; preferencia por modelo multilíngue adequado a portugues |
| Banco vetorial | Qdrant |
| Banco relacional/documental | PostgreSQL |
| Orquestracao RAG | LangChain planejado, usado de forma isolada atras de servicos proprios |
| Sistema de autenticacao | Django Auth; JWT/API key previsto para a API |
| Infra/deploy | Docker Compose local; producao a definir |
| Observabilidade/logs | Logs estruturados no backend; metricas basicas em fase posterior |
| Ambiente local | Pilha completa em Docker Compose; virtualenv opcional para desenvolvimento |
| Ambiente producao | A definir; recomendacao futura: VPS/cloud com containers |
| Tipo de dados indexados | Documentos textuais e arquivos academicos/administrativos |
| Formatos de arquivos | MVP: `.txt`, `.md` e `.pdf`; futuro: `.docx`, `.csv`, paginas web |
| Volume estimado de dados | Baixo a medio no MVP; arquitetura preparada para crescimento incremental |
| Usuarios finais | Estudantes, avaliadores e usuarios internos consultando conhecimento indexado |
| Restricoes de custo | Priorizar MVP local e servicos com custo controlado |
| Restricoes de privacidade/compliance | Proteger chaves, evitar logar conteudo sensivel e preservar metadados de origem |

## Objetivo do MVP

Construir uma API RAG que permita indexar documentos, consultar uma pergunta em
linguagem natural, recuperar os trechos mais relevantes e gerar uma resposta
fundamentada com fontes.

## Arquitetura proposta

```text
Usuario/Dashboard
      |
      v
 Django + Django REST Framework
      |
      +--> Servico de ingestao
      |       +--> extracao de texto
      |       +--> limpeza
      |       +--> chunking
      |       +--> embeddings
      |       +--> Qdrant
      |       +--> PostgreSQL
      |
      +--> Servico de consulta RAG
              +--> embeddings da pergunta
              +--> retrieval no Qdrant
              +--> montagem de prompt
              +--> Maritaca
              +--> resposta com fontes
```

RabbitMQ e Celery executam a indexacao vetorial fora da requisicao HTTP. O
endpoint sincrono permanece disponivel para diagnostico e operacao local.
No Compose, API e worker usam um servico interno de embeddings para compartilhar
uma unica instancia do modelo ONNX.

LangChain podera ser usado para integrar loaders, splitters, retrievers, prompts
e chamadas de modelos. Regras de negocio, contratos da API e persistencia nao
devem depender diretamente dele, reduzindo o custo de uma troca futura.

## Decisao de stack

Django com Django REST Framework foi confirmado como backend na Sprint 1.
LangChain permanece planejado para as sprints de RAG e sera encapsulado por
interfaces proprias.

### Django + Django REST Framework

**Quando faz sentido:** o projeto inclui PostgreSQL, usuarios, historico,
dashboard administrativo, autenticacao e KPIs.

**O que melhora:** ORM e migrations integrados, painel administrativo, sistema
de autenticacao maduro e estrutura adequada para funcionalidades de negocio.

**O que piora:** maior configuracao inicial, mais componentes no framework e
endpoints assíncronos menos diretos que no FastAPI.

### LangChain

**Quando faz sentido:** o projeto precisa integrar modelos, embeddings, Qdrant,
loaders, prompts e retrievers com possibilidade de evolucao.

**O que melhora:** reduz codigo de integracao e facilita experimentar diferentes
componentes RAG.

**O que piora:** adiciona dependencias, abstracoes e risco de acoplamento a APIs
que podem mudar. Por isso, seu uso sera limitado a adaptadores internos.

## Fluxo RAG proposto

1. Receber arquivo ou documento.
2. Extrair texto e metadados de origem.
3. Normalizar o texto e remover ruídos evidentes.
4. Dividir em chunks com overlap.
5. Gerar embeddings dos chunks.
6. Salvar embeddings e metadados no Qdrant.
7. Salvar documento, status de indexacao e auditoria no PostgreSQL.
8. Receber pergunta do usuario.
9. Gerar embedding da pergunta.
10. Recuperar `top-k` chunks no Qdrant.
11. Montar prompt com instrucao de resposta baseada no contexto.
12. Chamar Maritaca.
13. Retornar resposta, fontes, trechos e metadados.

## Estrutura inicial prevista

```text
NoSQL/TrabalhoN2/
  README.md
  docs/
    ADR.md
    SPRINT_0.md
    SPRINT_PLAN.md
  backend/
    manage.py
    config/
    apps/
      documents/
      rag/
      chat/
      common/
    tests/
  data/
    samples/
  docker/
```

## Setup local com containers

Requisitos:

- Docker com Docker Compose.

Configuracao:

```bash
cd NoSQL/TrabalhoN2
cp .env.example .env
docker compose up -d --build
docker compose ps
```

O dashboard fica em `http://127.0.0.1:3000` e a API em
`http://127.0.0.1:8000`. As migrations sao aplicadas automaticamente antes do
Gunicorn iniciar. PostgreSQL, Qdrant e RabbitMQ ficam isolados na rede interna.

Comandos operacionais:

```bash
docker compose logs -f api worker frontend
docker compose exec api python manage.py createsuperuser
docker compose exec api python manage.py check
docker compose down
```

Detalhes de imagens, redes, volumes e desenvolvimento hibrido estao em
[Containers e operacao local](docs/CONTAINERS.md).

## API atual

### Health check

```http
GET /api/health
```

Resposta esperada:

```json
{
  "status": "ok",
  "service": "adaptive-rag-api",
  "environment": "local"
}
```

## Como testar

```bash
.venv/bin/pytest
.venv/bin/python backend/manage.py check
cd frontend
npm run lint
npm run test:e2e
npm run build
```

Os mesmos checks Django podem ser executados na pilha containerizada:

```bash
docker compose exec api python manage.py check
```

## Variaveis de ambiente

| Variavel | Finalidade |
| --- | --- |
| `DJANGO_DEBUG` | Ativa debug apenas no ambiente local |
| `DJANGO_SECRET_KEY` | Chave secreta do Django |
| `DJANGO_ALLOWED_HOSTS` | Hosts aceitos, separados por virgula |
| `DJANGO_ENVIRONMENT` | Nome do ambiente retornado no health check |
| `DATABASE_URL` | Conexao principal com PostgreSQL |
| `DOCUMENT_MAX_UPLOAD_SIZE` | Tamanho maximo do upload em bytes |
| `RAG_CHUNK_SIZE` | Tamanho alvo de cada chunk em caracteres |
| `RAG_CHUNK_OVERLAP` | Sobreposicao entre chunks em caracteres |
| `EMBEDDING_MODEL` | Modelo local usado pelo FastEmbed |
| `EMBEDDING_DIMENSION` | Dimensao vetorial esperada pelo Qdrant |
| `EMBEDDING_CACHE_DIR` | Diretorio persistente do modelo de embedding |
| `EMBEDDING_THREADS` | Threads usadas pelo runtime ONNX |
| `EMBEDDING_ENABLE_CPU_MEM_ARENA` | Arena de memoria ONNX; desativada no perfil local |
| `EMBEDDING_SERVICE_URL` | Endpoint interno opcional para embeddings compartilhados |
| `EMBEDDING_SERVICE_ENABLED` | Habilita o endpoint apenas no container dedicado |
| `EMBEDDING_SERVICE_TIMEOUT_SECONDS` | Timeout da chamada ao servico de embeddings |
| `QDRANT_URL` | URL HTTP do Qdrant |
| `QDRANT_COLLECTION` | Colecao vetorial dos chunks |
| `RAG_TOP_K` | Quantidade padrao de resultados semanticos |
| `RAG_MAX_CONTEXT_CHARS` | Limite de caracteres enviados como contexto |
| `MARITACA_API_KEY` | Chave da API Maritaca |
| `MARITACA_BASE_URL` | URL base compativel com SDK OpenAI |
| `MARITACA_MODEL` | Modelo gerador, por padrao `sabia-4` |
| `MARITACA_TEMPERATURE` | Temperatura da geracao |
| `MARITACA_MAX_OUTPUT_TOKENS` | Limite de tokens da resposta |
| `MARITACA_TIMEOUT_SECONDS` | Timeout da chamada à LLM |
| `MARITACA_MAX_RETRIES` | Retentativas automaticas da chamada |
| `CELERY_BROKER_URL` | Conexao AMQP usada pelo Celery |
| `CELERY_INDEXING_MAX_RETRIES` | Retentativas de um job de indexacao |

## Persistencia estruturada

O PostgreSQL armazena:

- `Document`: origem, hash unico, status, metadados e auditoria.
- `DocumentChunk`: conteudo dos chunks, ordem, hash, pagina e metadados.
- `IndexingJob`: estado, tentativas, erros e tempos da indexacao assíncrona.

O hash unico de documento evita ingestao duplicada. Ao reprocessar um documento,
o repositorio substitui seus chunks dentro de uma transacao.

Comandos uteis:

```bash
docker compose up -d
docker compose exec api python manage.py migrate
docker compose exec api python manage.py createsuperuser
```

O Django Admin fica disponivel em `http://127.0.0.1:8000/admin/`.

## Como indexar documentos

O endpoint de ingestao aceita `.txt`, `.md` e `.pdf` em `multipart/form-data`:

```bash
curl -X POST http://127.0.0.1:8000/api/documents/ingest \
  -F "file=@data/samples/rag_overview.md" \
  -F "title=Visao geral do RAG"
```

O pipeline atual:

1. Valida formato e tamanho do arquivo.
2. Calcula hash para detectar duplicatas.
3. Extrai texto, preservando pagina quando disponivel.
4. Normaliza espacos e quebras de linha.
5. Divide o texto com `langchain-text-splitters`.
6. Persiste documento e chunks.

PDFs baseados apenas em imagem ainda exigem OCR e retornam erro controlado.

## Como indexar no Qdrant

Depois da ingestao, o documento possui status `chunked`. Para gerar embeddings e
persistir os vetores sem bloquear a requisicao:

```bash
docker compose up -d qdrant rabbitmq
curl -X POST http://127.0.0.1:8000/api/rag/documents/UUID_DO_DOCUMENTO/index-async
```

O endpoint retorna `202 Accepted` e um `id` de job. Consulte seu estado:

```bash
curl http://127.0.0.1:8000/api/rag/jobs/UUID_DO_JOB
```

Estados possiveis: `queued`, `processing`, `retrying`, `completed` e `failed`.
Falhas temporarias sao retentadas com backoff exponencial. O endpoint sincrono
`POST /api/rag/documents/UUID_DO_DOCUMENTO/index` permanece disponivel.

O modelo padrao e
`sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`, executado
localmente com FastEmbed. O primeiro uso baixa os arquivos do modelo.

## Como realizar busca semantica

```bash
curl -X POST http://127.0.0.1:8000/api/rag/search \
  -H "Content-Type: application/json" \
  -d '{"query": "Como funciona a recuperacao do RAG?", "top_k": 5}'
```

A resposta retorna score, texto do chunk, fonte, pagina e metadados. A geracao
de resposta com LLM esta disponivel no endpoint de consulta RAG.

## Como consultar o RAG

Configure `MARITACA_API_KEY` no arquivo `.env` e execute:

```bash
curl -X POST http://127.0.0.1:8000/api/rag/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Como funciona a recuperacao do RAG?", "top_k": 5}'
```

A resposta inclui:

- `answer`: resposta fundamentada.
- `sources`: chunks e metadados usados.
- `model`: modelo Maritaca utilizado.
- `usage`: uso reportado pelo provider.
- `request_id`: identificador para rastreabilidade.

Quando nenhum contexto e recuperado, a LLM nao e chamada. O prompt instrui o
modelo a usar apenas o contexto, citar `[Fonte N]` e ignorar instrucoes
potencialmente maliciosas contidas nos documentos.

## Dashboard

O dashboard possui uma area de consulta com fontes inspecionaveis e uma area de
documentos para upload, listagem persistente e acompanhamento da indexacao
assíncrona.

## Como avaliar o RAG

O dataset inicial fica em `data/evaluation/rag_cases.json`.

Avaliar somente retrieval, sem custo de LLM:

```bash
.venv/bin/python backend/manage.py evaluate_rag
```

Avaliar retrieval e respostas Maritaca:

```bash
.venv/bin/python backend/manage.py evaluate_rag --with-generation
```

Os relatorios sao salvos em `outputs/evaluation/evaluation.json` e
`outputs/evaluation/evaluation.md`. O quality gate inicial exige:

- Retrieval Hit Rate >= `0.80`.
- Mean Reciprocal Rank >= `0.70`.
- Citation Rate >= `0.80`, quando a geracao for avaliada.
- Answer Term Recall >= `0.60`, quando a geracao for avaliada.
- Generation Errors deve ser igual a `0`.
- Duplicate Result Rate deve ser igual a `0`.

Pytest usa settings isolados com SQLite em memoria. Assim, a suite automatizada
nao depende do PostgreSQL configurado no `.env`.

Baseline inicial:

- Retrieval relevante: Hit Rate `1.00`, MRR `1.00` e Precision@k `1.00`.
- Geracao Maritaca: Citation Rate `1.00`, Answer Term Recall `1.00` e zero
  erros de geracao.
- O quality gate detectou vetores duplicados deixados por uma base relacional
  anterior; reconciliacao PostgreSQL/Qdrant ficou pendente.
- Quality gate final: reprovado apenas por Duplicate Result Rate `0.50`.

## Documentacao

- [Sprint 0 - Analise e planejamento](docs/SPRINT_0.md)
- [Sprint 1 - Fundacao backend](docs/SPRINT_1.md)
- [Sprint 2 - Persistencia estruturada](docs/SPRINT_2.md)
- [Sprint 3 - Ingestao, extracao e chunking](docs/SPRINT_3.md)
- [Sprint 4 - Embeddings e Qdrant](docs/SPRINT_4.md)
- [Sprint 5 - Consulta RAG e Maritaca](docs/SPRINT_5.md)
- [Sprint 6 - Avaliacao minima](docs/SPRINT_6.md)
- [Sprint 7 - Indexacao assíncrona](docs/SPRINT_7.md)
- [Sprint 8 - Dashboard inicial](docs/SPRINT_8.md)
- [Containers e operacao local](docs/CONTAINERS.md)
- [Plano de sprints](docs/SPRINT_PLAN.md)
- [Decisoes arquiteturais](docs/ADR.md)

## Status

Sprint 8 concluida com dashboard responsivo, chat RAG, fontes, documentos,
indexacao assíncrona e testes E2E. A proxima etapa prevista e KPIs e
observabilidade.
