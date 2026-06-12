# Sistema RAG Adaptativo - Trabalho N2

Este projeto desenvolve um sistema RAG (`Retrieval-Augmented Generation`)
funcional, modular e testavel, especializado inicialmente em suporte tecnico
baseado em manuais de impressoras e scanners. A arquitetura preserva expansao
para outros equipamentos e tecnologias.

## Stack atual

| Campo | Definicao inicial |
| --- | --- |
| Linguagem | Python no backend; TypeScript no frontend |
| Framework backend | Django 5.2 LTS + Django REST Framework |
| Framework frontend | Next.js 16 + React 19 + Tailwind CSS 4 |
| LLM provider | Maritaca, com interface desacoplada para troca futura |
| Modelo de embedding | `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` via FastEmbed |
| Banco vetorial | Qdrant |
| Banco relacional/documental | PostgreSQL |
| Orquestracao RAG | Servicos proprios; `langchain-text-splitters` isolado no chunking |
| Sistema de autenticacao | Django Auth + tokens DRF opcionais; chave de API para integracoes |
| Infra/deploy | Docker Compose local e override endurecido para ambiente alvo |
| Observabilidade/logs | Logs JSON, histórico de queries e KPIs operacionais no dashboard |
| Ambiente local | Pilha completa em Docker Compose; virtualenv opcional para desenvolvimento |
| Ambiente producao | Template Compose preparado; provisionamento externo pendente |
| Tipo de dados indexados | Manuais tecnicos, guias de servico, troubleshooting e documentos gerais |
| Formatos de arquivos | MVP: `.txt`, `.md` e `.pdf`; futuro: `.docx`, `.csv`, paginas web |
| Volume estimado de dados | Manuais extensos; exemplo validado com 513 paginas e 50 MB |
| Usuarios finais | Tecnicos, suporte, operadores e usuarios consultando procedimentos fundamentados |
| Restricoes de custo | Priorizar MVP local e servicos com custo controlado |
| Restricoes de privacidade/compliance | Proteger chaves, evitar logar conteudo sensivel e preservar metadados de origem |

## Objetivo do MVP

Construir uma API RAG que permita indexar documentos, consultar uma pergunta em
linguagem natural, recuperar os trechos mais relevantes e gerar uma resposta
fundamentada com fontes.

O foco inicial e responder duvidas de diagnostico, erros, manutencao,
especificacoes e procedimentos de impressoras/scanners sem misturar orientacoes
de modelos diferentes e preservando alertas de seguranca.

## Arquitetura atual

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

No MVP, somente `langchain-text-splitters` foi adotado. Retrieval, prompting,
providers, regras de negocio, contratos da API e persistencia permanecem em
servicos proprios. Uma chain completa nao trouxe ganho proporcional para o
fluxo linear atual e aumentaria o acoplamento a APIs externas.

## Decisao de stack

Django com Django REST Framework foi confirmado como backend na Sprint 1.
LangChain ficou restrito ao splitter, encapsulado pelas interfaces proprias do
projeto.

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

## Fluxo RAG atual

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

## Estrutura principal

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
# Preencha MARITACA_API_KEY e defina API_ACCESS_KEY em ambientes compartilhados.
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
| `DJANGO_SECURE_SSL_REDIRECT` | Forca HTTPS quando o ambiente possui TLS |
| `DJANGO_SECURE_COOKIES` | Restringe cookies de sessao e CSRF a HTTPS |
| `API_ACCESS_KEY` | Chave compartilhada opcional exigida pelas APIs publicas |
| `API_REQUIRE_AUTHENTICATION` | Exige login/token ou chave de API quando `True` |
| `API_ANON_THROTTLE_RATE` | Limite de requisicoes anonimas do DRF |
| `API_USER_THROTTLE_RATE` | Limite de requisicoes autenticadas do DRF |
| `DATABASE_URL` | Conexao principal com PostgreSQL |
| `POSTGRES_HOST_PORT` | Porta do PostgreSQL publicada no host; dentro do Compose permanece `5432` |
| `DOCUMENT_MAX_UPLOAD_SIZE` | Tamanho maximo do upload em bytes; padrao 75 MB |
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
| `EMBEDDING_SERVICE_BATCH_SIZE` | Quantidade de chunks enviados por chamada ao servico de embeddings |
| `EMBEDDING_SERVICE_MAX_RETRIES` | Retentativas por chamada ao servico de embeddings |
| `EMBEDDING_SERVICE_RETRY_BASE_DELAY_SECONDS` | Espera base entre retentativas do servico de embeddings |
| `QDRANT_URL` | URL HTTP do Qdrant |
| `QDRANT_COLLECTION` | Colecao vetorial dos chunks |
| `RAG_TOP_K` | Quantidade padrao de resultados semanticos |
| `RAG_MIN_RELEVANCE_SCORE` | Score cosseno minimo aceito no retrieval; padrao `0.35` |
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
| `AUDIT_STORE_QUESTION_TEXT` | Persiste texto integral da pergunta; desativado por padrao |
| `AUDIT_RETENTION_DAYS` | Retencao padrao da auditoria; padrao `90` dias |
| `OBSERVABILITY_EXPOSE_QUESTION_TEXT` | Exibe perguntas no endpoint de KPIs quando `True` |

Quando `API_ACCESS_KEY` estiver preenchida, chamadas diretas a API devem incluir:

```bash
-H "X-API-Key: $API_ACCESS_KEY"
```

O dashboard injeta essa chave no proxy server-side. O health check e o endpoint
interno de embeddings nao usam a chave; o segundo permanece acessivel apenas na
rede interna do Compose.

## Autenticacao e papeis

Para exigir identidade individual, configure:

```dotenv
API_REQUIRE_AUTHENTICATION=True
```

Crie usuarios pelo Django Admin ou pelo terminal:

```bash
docker compose exec api python manage.py createsuperuser
```

O dashboard passa a exibir login e envia o token DRF nas requisicoes. Usuarios
comuns podem consultar o RAG, listar documentos e acompanhar jobs. Usuarios
`staff` tambem podem ingerir/indexar documentos e visualizar KPIs. A
`API_ACCESS_KEY` continua disponivel para integracoes de servico com acesso
administrativo.

Endpoints: `GET /api/auth/config`, `POST /api/auth/login`,
`GET /api/auth/me` e `POST /api/auth/logout`.

## Persistencia estruturada

O PostgreSQL armazena:

- `Document`: origem, arquivo original, hash unico, status, metadados e auditoria.
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

O endpoint persiste o arquivo e retorna `202 Accepted` com documento e job. O
pipeline executado pelo worker:

1. Valida formato e tamanho do arquivo.
2. Calcula hash em streaming para detectar duplicatas.
3. Persiste o arquivo original no volume compartilhado.
4. Extrai texto, preservando pagina quando disponivel.
5. Normaliza e divide o texto com `langchain-text-splitters`.
6. Persiste chunks, gera embeddings e atualiza o Qdrant.

PDFs baseados apenas em imagem ainda exigem OCR e retornam erro controlado.
PDFs protegidos que permitem extracao sao suportados por PyMuPDF, com fallback
para `pypdf`.

### Metadados tecnicos

Para manuais, o sistema infere fabricante, modelos, tipo de equipamento, tipo de
manual, idioma, paginas, capitulos, secoes, codigos de erro e nivel de seguranca.
Os campos principais tambem podem ser informados no upload:

```bash
curl -X POST http://127.0.0.1:8000/api/documents/ingest \
  -F "file=@docs/sm_elfb_e_ver2.pdf" \
  -F "manufacturer=Brother" \
  -F "models=MFC-L5710DN,MFC-L5715DW" \
  -F "equipment_type=multifunction" \
  -F "manual_type=service_manual" \
  -F "language=en"
```

Detalhes do domínio e da análise do manual estão em
[Manuais técnicos](docs/TECHNICAL_MANUALS.md).

## Como indexar no Qdrant

O upload ja enfileira extracao, chunking e indexacao. Consulte o job retornado:

```bash
curl http://127.0.0.1:8000/api/rag/jobs/UUID_DO_JOB
```

Estados possiveis: `queued`, `processing`, `retrying`, `completed` e `failed`.
Falhas temporarias sao retentadas com backoff exponencial. O endpoint sincrono
`POST /api/rag/documents/UUID_DO_DOCUMENTO/index` permanece disponivel.

Metadados podem ser corrigidos e reindexados com
`PATCH /api/documents/UUID_DO_DOCUMENTO`. Para executar novamente todo o
pipeline a partir do arquivo original:

```bash
curl -X POST http://127.0.0.1:8000/api/documents/UUID_DO_DOCUMENTO/reprocess
```

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

Busca e consulta aceitam filtros opcionais: `manufacturer`, `model`,
`equipment_type`, `manual_type` e `content_type`. Exemplo:

```bash
curl -X POST http://127.0.0.1:8000/api/rag/query \
  -H "Content-Type: application/json" \
  -d '{"question":"Quais verificações fazer para erro de alimentação?","manufacturer":"Brother","model":"MFC-L5710DN","content_type":"troubleshooting"}'
```

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

## KPIs e observabilidade

Cada consulta RAG registra no PostgreSQL:

- `request_id`, identidade/metodo de autenticacao, hash da pergunta, status e modelo.
- Filtros tecnicos aplicados; texto integral da pergunta somente quando habilitado.
- Duracao, `top_k`, quantidade de fontes e uso reportado pela LLM.
- Fontes recuperadas com documento, chunk, pagina, metadados, rank e score.
- Mensagem de erro controlada quando a consulta falha.

O endpoint agregado fica disponível em:

```http
GET /api/rag/kpis/overview
```

Ele retorna volume de consultas, taxa de erro, latencia media/P95, fontes por
resposta, documentos mais recuperados, estado dos jobs, serie de sete dias e
historico recente. A area **Indicadores** do dashboard apresenta esses dados.

Logs HTTP e do fluxo RAG sao emitidos em JSON no stdout dos containers:

```bash
docker compose logs -f api
```

Os logs incluem `request_id`, status e duracao, mas nao incluem o texto da
pergunta. Por padrao, a auditoria persiste apenas o hash da pergunta e os KPIs
mascaram seu texto. A retencao pode ser revisada e aplicada explicitamente:

```bash
docker compose exec api python manage.py purge_rag_audit
docker compose exec api python manage.py purge_rag_audit --apply
```

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

Depois de indexar o manual de exemplo, avaliar o domínio técnico:

```bash
.venv/bin/python backend/manage.py evaluate_rag \
  --dataset data/evaluation/technical_manual_cases.json \
  --output outputs/evaluation/technical-manual
```

Antes de avaliar uma base persistente, verificar e reconciliar PostgreSQL e
Qdrant. O primeiro comando e sempre um dry-run:

```bash
.venv/bin/python backend/manage.py reconcile_qdrant
.venv/bin/python backend/manage.py reconcile_qdrant --apply
.venv/bin/python backend/manage.py reconcile_qdrant --apply --reindex-missing
```

Somente chunks de documentos com status `indexed` sao considerados esperados.
`--apply` remove pontos orfaos e `--reindex-missing` reindexa documentos que
possuem chunks ausentes.

Os relatorios sao salvos em `outputs/evaluation/evaluation.json` e
`outputs/evaluation/evaluation.md`. O quality gate inicial exige:

- Retrieval Hit Rate >= `0.80`.
- Mean Reciprocal Rank >= `0.70`.
- Citation Rate >= `0.80`, quando a geracao for avaliada.
- Answer Term Recall >= `0.60`, quando a geracao for avaliada.
- Refusal Accuracy igual a `1.00`, quando a geracao for avaliada.
- Generation Errors deve ser igual a `0`.
- Duplicate Result Rate deve ser igual a `0`.

Pytest usa settings isolados com SQLite em memoria. Assim, a suite automatizada
nao depende do PostgreSQL configurado no `.env`.
