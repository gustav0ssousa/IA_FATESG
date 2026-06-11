# Containers e operacao local

## Visao geral

O ambiente completo executa com Docker Compose. O perfil local publica portas
de diagnostico; o override de producao mantem dados e broker na rede interna.

| Servico | Imagem | Porta no host | Responsabilidade |
| --- | --- | --- | --- |
| `frontend` | `adaptive-rag-frontend:local` | `3000` | Dashboard Next.js e proxy para a API |
| `api` | `adaptive-rag-backend:local` | `8000` | API Django REST e migrations |
| `worker` | `adaptive-rag-backend:local` | nenhuma | Extracao, chunking e indexacao assíncronos |
| `embeddings` | `adaptive-rag-backend:local` | nenhuma | Modelo ONNX compartilhado internamente |
| `embedding-cache-init` | `alpine:3.22` | nenhuma | Prepara permissoes do volume de embeddings |
| `document-storage-init` | `alpine:3.22` | nenhuma | Prepara permissoes do volume de documentos |
| `postgres` | `postgres:17-alpine` | `POSTGRES_HOST_PORT` (`5432` por padrao) | Dados estruturados e jobs |
| `qdrant` | `qdrant/qdrant:v1.18.0` | `6333`/`6334` apenas no perfil local | Embeddings e busca vetorial |
| `rabbitmq` | `rabbitmq:4.2-management-alpine` | nenhuma | Broker de tarefas |

API, worker e embeddings compartilham a mesma imagem para evitar builds
redundantes. Os containers da aplicacao executam com usuarios sem privilegios.

## Redes

- `app`: conecta frontend, API e worker. Servicos publicados no host tambem
  usam essa rede nao interna. API e worker a usam para chamadas externas, como
  Maritaca.
- `backend`: rede interna que conecta API e worker ao servico de embeddings,
  PostgreSQL, Qdrant e RabbitMQ. Apenas servicos com uma porta explicitamente
  publicada e ligados tambem a rede `app` sao acessiveis pelo host.

## Volumes

- `postgres_data`: dados relacionais.
- `qdrant_data`: colecoes e vetores.
- `rabbitmq_data`: estado do broker.
- `embedding_cache`: modelo de embedding compartilhado entre API e worker.
- `document_uploads`: arquivos originais compartilhados entre API e worker.

Os servicos curtos `embedding-cache-init` e `document-storage-init` preparam os volumes para o UID sem
privilegios usado pelo backend e termina antes de API e worker iniciarem.

## Subir e verificar

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

As migrations Django sao aplicadas automaticamente antes de iniciar o
Gunicorn. O Compose aguarda os health checks das dependencias.

O Gunicorn local usa um processo com quatro threads. Essa configuracao evita
duplicar em memoria o modelo ONNX de embeddings; producao deve dimensionar
workers e memoria a partir de testes de carga.

O perfil local centraliza o modelo ONNX no servico interno `embeddings`, limita
o runtime a uma thread e desativa seu arena allocator. API e worker usam esse
servico pela rede privada, evitando duas copias pesadas do modelo em memoria.
Esses valores sao configuraveis por `EMBEDDING_THREADS` e
`EMBEDDING_ENABLE_CPU_MEM_ARENA`.

Endpoints publicados:

- Dashboard: `http://127.0.0.1:3000`
- API: `http://127.0.0.1:8000`
- Health check: `http://127.0.0.1:8000/api/health`

O dashboard encaminha `/backend-api/*` por uma rota proxy propria. Ela preserva
uploads e aguarda chamadas RAG que podem ultrapassar o timeout de proxies
automaticos. Quando `API_ACCESS_KEY` esta configurada, o proxy injeta a chave
somente no servidor enquanto `API_REQUIRE_AUTHENTICATION=False`; ela nao e
enviada ao JavaScript do navegador. Quando login individual esta ativo, o proxy
preserva o token do usuario e nao injeta a chave administrativa.

Credenciais locais de PostgreSQL e RabbitMQ sao configuradas por variaveis no
`.env`. Os valores padrao do Compose servem apenas para desenvolvimento e devem
ser trocados em qualquer ambiente compartilhado.

A porta publicada do PostgreSQL pode ser alterada no `.env`, por exemplo:

```dotenv
POSTGRES_HOST_PORT=5434
DATABASE_URL=postgresql://rag_user:rag_password@localhost:5434/rag_db
```

Conexoes feitas pelo host usam `localhost:5434`. API e worker executados no
Compose continuam usando `postgres:5432`, pois a comunicacao entre containers
usa a porta interna do servico. A publicacao fica limitada a `127.0.0.1` para
nao expor o banco em outras interfaces da maquina.

## Operacao

```bash
docker compose logs -f api worker frontend
docker compose exec api python manage.py createsuperuser
docker compose exec api python manage.py check
docker compose restart worker
docker compose down
```

## Perfil de producao

O override exige hosts, chave secreta e chave administrativa; ativa
autenticacao, cookies/redirect HTTPS e remove portas de PostgreSQL e Qdrant:

```bash
export DJANGO_ALLOWED_HOSTS=rag.example.com
export DJANGO_SECRET_KEY='valor-longo-e-aleatorio'
export API_ACCESS_KEY='valor-longo-e-rotacionavel'
docker compose -f docker-compose.yml -f docker-compose.prod.yml config
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

API e frontend ficam publicados somente em `127.0.0.1`, preparados para um
proxy reverso com TLS. RabbitMQ e embeddings continuam sem porta no host.

## Backup e retencao

Exemplo de backup logico do PostgreSQL:

```bash
docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" > rag.sql
```

Qdrant deve ser protegido com snapshots da colecao e os arquivos originais com
backup do volume `document_uploads`. A auditoria antiga pode ser inspecionada e
removida com `purge_rag_audit`; a remocao real exige `--apply`.

Para reconstruir apenas a aplicacao:

```bash
docker compose build api frontend
docker compose up -d api worker frontend
```

Recomenda-se disponibilizar ao menos 2 GiB ao Docker para reconstruir imagens
enquanto a pilha esta ativa. Em ambientes limitados a cerca de 1 GiB, pare os
containers antes do rebuild para evitar pressao de memoria:

```bash
docker compose down
docker compose up -d --build
```

`docker compose down` preserva volumes. O comando abaixo remove todos os dados
locais persistidos e deve ser usado somente de forma intencional:

```bash
docker compose down -v
```

## Desenvolvimento hibrido

Os valores `localhost` de `.env.example` servem para executar Django, Celery ou
Next.js diretamente no host durante desenvolvimento. Nesse modo, os servicos de
infraestrutura precisam ser publicados por um override Compose local. O fluxo
padrao e recomendado deste projeto e executar a pilha completa em containers.
