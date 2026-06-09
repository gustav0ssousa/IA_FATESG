# Sprint 3 - Ingestao, Extracao e Chunking

## Objetivo

Implementar o fluxo sincrono de entrada de documentos, extracao de texto,
normalizacao, chunking e persistencia estruturada.

## Escopo

- Endpoint multipart `POST /api/documents/ingest`.
- Validacao de formato, arquivo vazio e tamanho maximo.
- Extracao de `.txt`, `.md` e `.pdf`.
- Preservacao do numero da pagina em PDFs.
- Normalizacao de texto.
- Chunking configuravel com overlap.
- Deteccao de documentos duplicados por SHA-256.
- Persistencia de documentos e chunks.
- Registro auditavel de falhas.
- Documento de exemplo.
- Testes de componentes, servico e API.

## Fora do escopo

- OCR para PDFs baseados em imagem.
- Arquivos `.docx`, `.csv` e paginas web.
- Embeddings e Qdrant.
- Indexacao assíncrona com RabbitMQ.
- Autenticacao do endpoint.
- Tokenizacao especifica do modelo de embeddings.

## Decisoes tecnicas

- Extratores implementam contratos proprios e nao dependem de LangChain.
- `langchain-text-splitters` foi adotado isoladamente atras de
  `LangChainTextChunker`.
- O chunking e baseado em caracteres, com tamanho padrao `1200` e overlap `200`.
- Separadores priorizam paragrafos, linhas, frases e palavras.
- O texto e normalizado sem remover pontuacao ou informacao semantica.
- `token_count` permanece vazio ate a escolha do tokenizer de embeddings.
- PDFs preservam o numero da pagina; PDFs sem texto extraivel informam a
  limitacao de OCR.
- Documentos processados recebem status `chunked`, pois ainda nao foram enviados
  ao Qdrant.

## Arquivos criados ou alterados

- `.env.example`
- `requirements.txt`
- `backend/config/settings.py`
- `backend/config/urls.py`
- `backend/apps/documents/models.py`
- `backend/apps/documents/repositories.py`
- `backend/apps/documents/extractors.py`
- `backend/apps/documents/chunking.py`
- `backend/apps/documents/services.py`
- `backend/apps/documents/serializers.py`
- `backend/apps/documents/views.py`
- `backend/apps/documents/urls.py`
- `backend/apps/documents/migrations/0002_alter_document_status.py`
- `backend/tests/test_chunking.py`
- `backend/tests/test_extractors.py`
- `backend/tests/test_ingestion.py`
- `data/samples/rag_overview.md`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_3.md`

## Implementacao

Exemplo:

```bash
curl -X POST http://127.0.0.1:8000/api/documents/ingest \
  -F "file=@data/samples/rag_overview.md" \
  -F "title=Visao geral do RAG"
```

Resposta resumida:

```json
{
  "id": "uuid-do-documento",
  "title": "Visao geral do RAG",
  "source_name": "rag_overview.md",
  "source_type": "md",
  "status": "chunked",
  "chunk_count": 1,
  "duplicate": false
}
```

Reenviar o mesmo conteudo retorna o documento existente com
`"duplicate": true`.

## Como testar

```bash
.venv/bin/pytest
.venv/bin/python backend/manage.py check
.venv/bin/python backend/manage.py makemigrations --check --dry-run
.venv/bin/python backend/manage.py migrate
```

Validacoes esperadas:

- Texto e Markdown sao extraidos como UTF-8.
- PDF textual preserva paginas.
- PDF sem texto retorna erro de OCR nao suportado.
- Arquivos nao suportados e uploads vazios sao rejeitados.
- Duplicatas nao criam novos documentos.
- Falhas ficam registradas com status `failed`.

Resultados obtidos:

- 19 testes automatizados aprovados.
- Django system check sem problemas.
- Nenhuma migration pendente.
- Migration de status aplicada com sucesso no fallback SQLite.
- Upload HTTP real do documento de exemplo retornou status `chunked`.
- Segundo upload do mesmo conteudo retornou `duplicate: true` e o mesmo ID.
- Configuracao Docker Compose validada; PostgreSQL real continua indisponivel
  porque o daemon Docker nao esta acessivel.

## Documentacao atualizada

- README com endpoint, exemplo de ingestao, configuracao e limitacoes.
- ADR de LangChain atualizado com adocao parcial.
- Plano de sprints atualizado.
- Este relatorio da Sprint 3.

## Checklist de conclusao

- [x] Endpoint de upload criado.
- [x] Validacoes de upload implementadas.
- [x] Extracao `.txt`, `.md` e `.pdf` implementada.
- [x] Normalizacao implementada.
- [x] Chunking configuravel implementado.
- [x] Metadados e paginas preservados.
- [x] Persistencia e deduplicacao implementadas.
- [x] Testes automatizados aprovados.
- [x] Documentacao atualizada.

## Riscos e pendencias

- PDFs escaneados exigirao OCR.
- O endpoint ainda e publico e sincrono.
- A extracao de PDFs complexos pode perder estrutura visual.
- O chunking por caracteres precisara ser avaliado com documentos reais.
- Os chunks ainda nao possuem embeddings nem `token_count`.
- A integracao PostgreSQL real continua dependente do daemon Docker.

## Proxima sprint prevista

Sprint 4 - Embeddings e Qdrant: definir o modelo de embeddings, criar adaptadores,
persistir vetores e implementar busca semantica inicial.

Sprint finalizada. Aguardando o comando `continuar` para iniciar a proxima sprint.
