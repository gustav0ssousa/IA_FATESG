# Plano de Sprints

## Sprint 0 - Analise e Planejamento

Objetivo: definir arquitetura, riscos, escopo do MVP, plano incremental e ADRs
iniciais sem implementar codigo de aplicacao.

Status: concluida.

## Sprint 1 - Fundacao Backend

Objetivo: confirmar a decisao de framework e criar a base do backend. A opcao
recomendada em avaliacao e Django com Django REST Framework, mantendo FastAPI
como alternativa caso o escopo seja reduzido a uma API RAG enxuta.

Status: concluida com Django 5.2 LTS e Django REST Framework.

Escopo previsto:

- Registrar decisao final entre Django/DRF e FastAPI.
- Projeto Django e apps iniciais, caso Django seja confirmado.
- Django REST Framework com `GET /api/health`.
- Configuracao por variaveis de ambiente.
- `.env.example`.
- Testes com Pytest.
- README com como executar e testar.

Fora do escopo:

- Banco de dados.
- Qdrant.
- LLM.
- Integracao LangChain.
- Ingestao real de documentos.

## Sprint 2 - Persistencia Estruturada com PostgreSQL

Objetivo: adicionar PostgreSQL para registrar documentos, chunks, status de
indexacao e historico basico.

Status: concluida. A validacao real do container PostgreSQL ficou pendente porque
o daemon Docker nao estava acessivel no ambiente de execucao.

Escopo previsto:

- Docker Compose com PostgreSQL.
- Django ORM.
- Models e migrations.
- Camada de servicos e acesso a dados.
- Django Admin basico para inspecao dos documentos.
- Testes de persistencia.

## Sprint 3 - Ingestao, Extracao e Chunking

Objetivo: implementar entrada de documentos e transformacao em chunks com
metadados.

Status: concluida com endpoint de upload, extratores proprios e
`langchain-text-splitters` encapsulado.

Escopo previsto:

- Upload ou leitura de arquivos.
- Suporte inicial a `.txt`, `.md` e `.pdf`.
- Normalizacao de texto.
- Loaders e text splitters do LangChain avaliados atras de adaptadores proprios.
- Chunking configuravel e independente do framework de orquestracao.
- Testes unitarios de chunking.

## Sprint 4 - Embeddings e Qdrant

Objetivo: gerar embeddings e persistir chunks vetorizados no Qdrant.

Status: concluida com FastEmbed, Qdrant, indexacao por documento e busca
semantica inicial.

Escopo previsto:

- Docker Compose com Qdrant.
- Interface `EmbeddingProvider`.
- Implementacao configuravel de embeddings.
- Interface `VectorStore`.
- Adaptador LangChain para Qdrant, se aprovado pelos testes.
- Insercao e busca vetorial inicial.
- Testes com mocks e/ou ambiente local.

## Sprint 5 - Consulta RAG e Geracao com Maritaca

Objetivo: implementar endpoint de pergunta e resposta com retrieval, prompt e
LLM.

Status: concluida com Responses API da Maritaca, prompt fundamentado e retorno
de fontes.

Escopo previsto:

- `POST /chat/query`.
- Retriever semantico `top_k`.
- Chain LangChain para retrieval e prompt, encapsulada em servico proprio.
- Prompt com contexto e instrucao anti-alucinacao.
- Interface `LLMProvider`.
- Integracao com Maritaca.
- Resposta com fontes.

## Sprint 6 - Avaliacao Minima e Testes de Fluxo

Objetivo: criar validacoes de qualidade para recuperacao e geracao.

Status: concluida com dataset versionado, metricas deterministicas, quality gate
e comando de avaliacao.

Escopo previsto:

- Dataset pequeno de perguntas/respostas esperadas.
- Teste de fluxo RAG com providers falsos.
- Avaliacao manual assistida.
- Criterios minimos de qualidade.

## Sprint 7 - Ingestao Assincrona com RabbitMQ

Objetivo: mover indexacao para worker assíncrono quando a base RAG estiver
funcional.

Status: concluida com RabbitMQ, Celery, jobs persistidos no PostgreSQL,
retentativas com backoff e validacao real de indexacao.

Escopo previsto:

- Docker Compose com RabbitMQ.
- Worker de indexacao.
- Fila de jobs.
- Status de indexacao no PostgreSQL.
- Retentativas basicas.

## Sprint 8 - Dashboard Inicial

Objetivo: criar dashboard para chat e visualizacao de fontes.

Status: concluida com Next.js 16, Tailwind CSS 4, chat RAG, painel de fontes,
gestao de documentos e validacao E2E responsiva.

Escopo previsto:

- Next.js + Tailwind.
- Tela de chat.
- Lista de fontes/trechos.
- Indicadores basicos de status.
- Integracao com backend.

## Sprint 9 - KPIs e Observabilidade

Objetivo: adicionar consultas de KPIs e sinais operacionais.

Status: concluida com historico de queries e fontes, KPIs agregados, logs JSON
correlacionados e dashboard operacional.

Escopo previsto:

- Historico de queries.
- Documentos mais recuperados.
- Tempo de resposta.
- Taxa de erro.
- Logs estruturados.

## Sprint 10 - Hardening e Documentacao Final

Objetivo: preparar o projeto para apresentacao e evolucao.

Status: concluida com chave de API opcional, throttling, respostas de erro
controladas, protecoes HTTP, mascaramento de perguntas nos KPIs, Compose
parametrizado e documentacao final.

Escopo previsto:

- Revisao de seguranca.
- Limites de upload.
- Melhorias de erro.
- Documentacao completa.
- Guia de demonstracao.

## Sprint 11 - Autenticacao e Controle de Acesso

Objetivo: adicionar identidade individual e separar operacoes de leitura e
administracao sem quebrar o perfil local.

Status: concluida com TokenAuthentication do DRF, login/logout no dashboard,
papeis baseados em `is_staff` e compatibilidade com chave de API.

Escopo:

- Login, logout, perfil atual e configuracao publica de autenticacao.
- Tokens individuais.
- Papel leitor para consulta/listagem.
- Papel gestor para ingestao, indexacao e KPIs.
- Dashboard autenticado responsivo.
- Testes e documentacao.

## Sprint 12 - Especializacao em Manuais Tecnicos

Objetivo: adaptar o RAG ao contexto principal de manuais de impressoras e
scanners, preservando expansao para outros equipamentos.

Status: concluida com extracao otimizada de PDFs extensos/protegidos,
classificacao tecnica, metadados por modelo, filtros de retrieval, prompt seguro
e dataset de avaliacao do domínio.

Escopo:

- Analise do manual Brother de exemplo.
- Suporte a PDF AES e uploads de ate 75 MB.
- Inferencia de fabricante, modelos, tipo de manual/equipamento e idioma.
- Classificacao de seguranca, troubleshooting, erros e procedimentos.
- Filtros de consulta por metadados tecnicos.
- Prompt orientado a suporte tecnico e seguranca.
- Avaliacao e documentacao do domínio.

## Roadmap final de entrega

O detalhamento, os achados da revisao e a Definition of Done estao em
[`FINAL_REVIEW_AND_ROADMAP.md`](FINAL_REVIEW_AND_ROADMAP.md).

### Sprint 13 - Gate de Confiabilidade

Status: concluida em codigo e testes. A pendencia operacional registrada nesta
sprint foi resolvida na Sprint 16, com reconciliacao limpa e quality gate
aprovado na base controlada.

Objetivo: impedir respostas sem contexto suficiente e aprovar integralmente o
quality gate do RAG.

Escopo previsto:

- Limiar minimo de relevancia e resposta de recusa implementados.
- Reconciliacao idempotente entre PostgreSQL e Qdrant implementada.
- Testes negativos implementados; reexecucao da avaliacao tecnica pendente no
  ambiente persistente.

### Sprint 14 - Ingestao Assincrona e Ciclo de Vida

Status: concluida com upload persistido, pipeline integral no worker,
reprocessamento e revisao de metadados no backend e frontend. A pilha Docker
completa foi reconstruida e validada com os servicos saudaveis.

Objetivo: processar manuais grandes fora da requisicao HTTP e permitir
reprocessamento e correcao de metadados.

Escopo previsto:

- Persistencia do upload e pipeline integral no worker implementados.
- Estados, retentativas e reprocessamento idempotente implementados.
- Revisao e edicao de metadados tecnicos implementadas no backend e frontend.

### Sprint 15 - Auditoria, Deploy e Fechamento

Status: concluida em codigo e validacoes automatizadas.

Objetivo: fechar rastreabilidade, seguranca operacional e criterios de entrega.

Escopo previsto:

- Auditoria por usuario, autenticacao, filtros, chunks, paginas e metadados.
- Politica executavel de retencao e redacao da pergunta por padrao.
- Override Compose de producao com portas de dados removidas.
- Biblioteca paginada com facets globais de fabricante/modelo.
- Validacao automatizada de backend, frontend e perfis Compose.

### Sprint 16 - Certificacao Final da Base Controlada

Status: concluida. A base controlada foi indexada e reconciliada, e o quality
gate de retrieval e geracao foi aprovado dentro da imagem containerizada.

Objetivo: certificar o fluxo RAG completo com dados reais e corrigir desvios
encontrados na avaliacao tecnica.

Escopo entregue:

- Base com `2` documentos, `803` chunks e `803` vetores consistentes.
- Expansao de contexto para chunks adjacentes na mesma pagina.
- Normalizacao de citacoes para o contrato `[Fonte N]`.
- Backend com `85` testes aprovados.
- Quality gate tecnico aprovado, incluindo geracao Maritaca.
