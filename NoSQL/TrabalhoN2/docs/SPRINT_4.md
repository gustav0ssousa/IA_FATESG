# Sprint 4 - Embeddings e Qdrant

## Objetivo

Gerar embeddings dos chunks, persistir vetores no Qdrant e oferecer busca
semantica inicial com fontes e metadados.

## Escopo

- Qdrant no Docker Compose.
- Interface `EmbeddingProvider`.
- Provider local FastEmbed.
- Interface `VectorStore`.
- Adaptador Qdrant.
- Criacao automatica da colecao vetorial.
- Indexacao e reindexacao por documento.
- Busca semantica `top_k`.
- Endpoints de indexacao e busca.
- Tratamento auditavel de falhas.
- Testes com provider falso e Qdrant em memoria.
- Validacao real do modelo de embeddings.

## Fora do escopo

- Geracao de respostas com Maritaca.
- Prompt RAG e citacoes na resposta final.
- Busca hibrida, filtros e reranking.
- Indexacao assíncrona.
- Avaliacao quantitativa da recuperacao.
- Autenticacao dos endpoints.

## Decisoes tecnicas

- Modelo padrao:
  `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`.
- O modelo e multilíngue, local e gera vetores de 384 dimensoes.
- FastEmbed foi escolhido para evitar a dependencia direta de PyTorch no MVP.
- `EmbeddingProvider` separa dominio e modelo concreto.
- `VectorStore` separa servicos RAG e Qdrant.
- A colecao usa similaridade cosseno.
- A reindexacao remove vetores antigos do documento antes do `upsert`.
- O payload Qdrant preserva IDs, conteudo, fonte, pagina e metadados.
- Testes usam Qdrant em memoria e provider deterministico.
- Falhas de embedding ou Qdrant atualizam o documento para `failed`.

## Arquivos criados ou alterados

- `.env.example`
- `requirements.txt`
- `docker-compose.yml`
- `backend/config/settings.py`
- `backend/config/urls.py`
- `backend/apps/rag/apps.py`
- `backend/apps/rag/embeddings.py`
- `backend/apps/rag/vector_store.py`
- `backend/apps/rag/services.py`
- `backend/apps/rag/serializers.py`
- `backend/apps/rag/views.py`
- `backend/apps/rag/urls.py`
- `backend/tests/test_rag_vector_store.py`
- `backend/tests/test_rag_api.py`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_4.md`

## Implementacao

Subir o Qdrant:

```bash
docker compose up -d qdrant
```

Indexar um documento previamente ingerido:

```bash
curl -X POST http://127.0.0.1:8000/api/rag/documents/UUID_DO_DOCUMENTO/index
```

Buscar chunks:

```bash
curl -X POST http://127.0.0.1:8000/api/rag/search \
  -H "Content-Type: application/json" \
  -d '{"query": "Como funciona o RAG?", "top_k": 5}'
```

## Como testar

```bash
.venv/bin/pytest
.venv/bin/python backend/manage.py check
.venv/bin/python backend/manage.py makemigrations --check --dry-run
docker compose config --quiet
```

Resultados obtidos:

- 27 testes automatizados aprovados.
- Qdrant em memoria validou indexacao, busca e reindexacao.
- Endpoints de indexacao e busca validados com providers falsos.
- Modelo FastEmbed real baixado e executado.
- Vetores reais de documentos e consulta confirmados com 384 dimensoes.
- Django system check sem problemas.
- Nenhuma migration pendente.
- Compose validado.
- Servidor Qdrant real nao foi iniciado porque o daemon Docker continua
  inacessivel neste ambiente.

## Documentacao atualizada

- README com Qdrant, indexacao e busca semantica.
- ADR do Qdrant marcado como implementado.
- Novo ADR para o modelo de embeddings.
- Plano de sprints atualizado.
- Este relatorio da Sprint 4.

## Checklist de conclusao

- [x] Qdrant adicionado ao Compose.
- [x] Provider de embeddings implementado.
- [x] Vector store Qdrant implementado.
- [x] Indexacao e reindexacao implementadas.
- [x] Busca semantica implementada.
- [x] Endpoints implementados.
- [x] Tratamento de falhas implementado.
- [x] Testes automatizados aprovados.
- [x] Modelo real validado.
- [x] Integracao executada contra servidor Qdrant real posteriormente na Sprint 6.

## Riscos e pendencias

- Trocar o modelo ou a versao do FastEmbed exige reindexacao completa.
- O modelo e baixado no primeiro uso e depende de rede nesse momento.
- O modelo escolhido precisa ser avaliado com perguntas reais em portugues.
- A busca ainda nao possui filtros por metadados, threshold ou reranking.
- Os endpoints de indexacao e busca ainda sao publicos.
- PostgreSQL e Qdrant reais dependem da ativacao do daemon Docker.

## Proxima sprint prevista

Sprint 5 - Consulta RAG e Geracao com Maritaca: montar prompt com contexto,
integrar a LLM e retornar resposta fundamentada com fontes.

Sprint finalizada. Aguardando o comando `continuar` para iniciar a proxima sprint.
