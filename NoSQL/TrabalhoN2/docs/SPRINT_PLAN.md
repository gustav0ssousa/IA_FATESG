# Plano de Sprints

## Sprint 0 - Analise e Planejamento

Objetivo: definir arquitetura, riscos, escopo do MVP, plano incremental e ADRs
iniciais sem implementar codigo de aplicacao.

Status: concluida.

## Sprint 1 - Fundacao Backend

Objetivo: criar a base do backend FastAPI com estrutura modular, configuracao por
ambiente, health check, testes iniciais e documentacao de setup local.

Escopo previsto:

- Estrutura `backend/app`.
- FastAPI com `GET /health`.
- Configuracao com Pydantic Settings.
- `.env.example`.
- Testes com Pytest.
- README com como executar e testar.

Fora do escopo:

- Banco de dados.
- Qdrant.
- LLM.
- Ingestao real de documentos.

## Sprint 2 - Persistencia Estruturada com PostgreSQL

Objetivo: adicionar PostgreSQL para registrar documentos, chunks, status de
indexacao e historico basico.

Escopo previsto:

- Docker Compose com PostgreSQL.
- SQLAlchemy ou SQLModel.
- Modelos/tabelas iniciais.
- Repositorios.
- Migrations, se viavel.
- Testes de persistencia.

## Sprint 3 - Ingestao, Extracao e Chunking

Objetivo: implementar entrada de documentos e transformacao em chunks com
metadados.

Escopo previsto:

- Upload ou leitura de arquivos.
- Suporte inicial a `.txt`, `.md` e `.pdf`.
- Normalizacao de texto.
- Chunking configuravel.
- Testes unitarios de chunking.

## Sprint 4 - Embeddings e Qdrant

Objetivo: gerar embeddings e persistir chunks vetorizados no Qdrant.

Escopo previsto:

- Docker Compose com Qdrant.
- Interface `EmbeddingProvider`.
- Implementacao configuravel de embeddings.
- Interface `VectorStore`.
- Insercao e busca vetorial inicial.
- Testes com mocks e/ou ambiente local.

## Sprint 5 - Consulta RAG e Geracao com Maritaca

Objetivo: implementar endpoint de pergunta e resposta com retrieval, prompt e
LLM.

Escopo previsto:

- `POST /chat/query`.
- Retriever semantico `top_k`.
- Prompt builder com contexto e instrucao anti-alucinacao.
- Interface `LLMProvider`.
- Integracao com Maritaca.
- Resposta com fontes.

## Sprint 6 - Avaliacao Minima e Testes de Fluxo

Objetivo: criar validacoes de qualidade para recuperacao e geracao.

Escopo previsto:

- Dataset pequeno de perguntas/respostas esperadas.
- Teste de fluxo RAG com providers falsos.
- Avaliacao manual assistida.
- Criterios minimos de qualidade.

## Sprint 7 - Ingestao Assincrona com RabbitMQ

Objetivo: mover indexacao para worker assíncrono quando a base RAG estiver
funcional.

Escopo previsto:

- Docker Compose com RabbitMQ.
- Worker de indexacao.
- Fila de jobs.
- Status de indexacao no PostgreSQL.
- Retentativas basicas.

## Sprint 8 - Dashboard Inicial

Objetivo: criar dashboard para chat e visualizacao de fontes.

Escopo previsto:

- Next.js + Tailwind.
- Tela de chat.
- Lista de fontes/trechos.
- Indicadores basicos de status.
- Integracao com backend.

## Sprint 9 - KPIs e Observabilidade

Objetivo: adicionar consultas de KPIs e sinais operacionais.

Escopo previsto:

- Historico de queries.
- Documentos mais recuperados.
- Tempo de resposta.
- Taxa de erro.
- Logs estruturados.

## Sprint 10 - Hardening e Documentacao Final

Objetivo: preparar o projeto para apresentacao e evolucao.

Escopo previsto:

- Revisao de seguranca.
- Limites de upload.
- Melhorias de erro.
- Documentacao completa.
- Guia de demonstracao.
