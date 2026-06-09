# Sprint 0 - Analise e Planejamento

## Objetivo

Definir a direcao tecnica do sistema RAG adaptativo antes de iniciar codigo de
aplicacao, analisando stack, riscos, arquitetura, fluxo RAG, plano de sprints,
criterios de aceite do MVP e suposicoes iniciais.

## Escopo

- Diagnostico da stack proposta: Qdrant, PostgreSQL, FastAPI/Django, RabbitMQ,
  Maritaca, LangChain e dashboard com Next.js/Tailwind.
- Identificacao de pontos fortes, lacunas, ambiguidades e riscos.
- Recomendacoes separadas em essenciais, recomendadas e opcionais.
- Proposta de arquitetura inicial.
- Proposta de fluxo completo do RAG.
- Estrutura inicial de pastas.
- Plano incremental de sprints.
- Criterios de aceite do MVP.
- Suposicoes documentadas.
- Perguntas bloqueantes, caso existam.

## Fora do escopo

- Implementacao de backend, frontend, banco, containers ou pipelines.
- Integracao real com Maritaca, Qdrant, PostgreSQL ou RabbitMQ.
- Escolha final de todos os modelos e parametros.
- Testes automatizados.
- Deploy em producao.

## Diagnostico da stack

### Pontos fortes

- **Django + Django REST Framework**: opcao recomendada em avaliacao por integrar
  ORM, migrations, autenticacao, painel administrativo e APIs estruturadas.
- **FastAPI**: permanece uma boa alternativa se o sistema for reduzido a uma API
  RAG enxuta, com prioridade para simplicidade e operacoes assíncronas.
- **LangChain**: oferece integracoes para loaders, splitters, embeddings,
  retrievers, Qdrant e modelos, acelerando experimentos RAG.
- **Qdrant**: apropriado para busca vetorial, filtros por metadados e evolucao
  para colecoes com payloads mais ricos.
- **PostgreSQL**: forte para dados estruturados, auditoria, usuarios, historico
  de consultas, documentos, status de indexacao e metadados.
- **RabbitMQ**: adequado para ingestao assíncrona, reprocessamento, retentativas
  e separacao entre API e workers.
- **Maritaca**: compatível com o objetivo de usar um provider de LLM voltado ao
  contexto em portugues, desde que a interface seja desacoplada.
- **Next.js + Tailwind**: boa combinacao para dashboard de chat, consultas e
  KPIs, quando o backend ja tiver endpoints estaveis.

### Riscos ou lacunas

- O modelo de embedding ainda nao esta definido.
- Nao ha definicao final dos formatos de documentos e volume esperado.
- A estrategia de autenticacao esta indefinida.
- RabbitMQ pode aumentar complexidade no inicio se usado antes da necessidade.
- O dashboard pode distrair do MVP caso comece antes do fluxo RAG estar estavel.
- Falta criterio inicial de avaliacao da qualidade de recuperacao e resposta.
- Chaves de API, logs e historico de perguntas precisam de cuidado para nao
  expor dados sensiveis.
- Se Maritaca nao oferecer embedding adequado ao caso, sera necessario usar um
  provedor/modelo separado para embeddings.
- Django adiciona mais estrutura e configuracao inicial que FastAPI.
- LangChain pode introduzir acoplamento, dependencias extras e mudancas de API.

## Recomendacoes de melhoria

### Essenciais

- Criar uma interface desacoplada para LLM e embeddings.
- Separar responsabilidades em modulos: ingestao, chunking, embeddings, vector
  store, retrieval, prompting e geracao.
- Usar variaveis de ambiente para chaves, URLs e parametros.
- Registrar documentos e chunks com IDs estaveis e metadados rastreaveis.
- Retornar fontes e trechos recuperados em toda resposta RAG.
- Implementar testes minimos para chunking, configuracao e fluxo de consulta.
- Comecar com MVP sincrono e simples antes de ativar workers com RabbitMQ.
- Encapsular LangChain em adaptadores e servicos proprios.

### Recomendadas

- Usar Docker Compose para PostgreSQL, Qdrant e, futuramente, RabbitMQ.
- Usar logs estruturados com `request_id` e `document_id`.
- Registrar historico de consultas sem armazenar dados sensiveis desnecessarios.
- Criar dados de exemplo para testes e demonstracao.
- Adotar busca por similaridade com `top_k` configuravel.
- Preparar metadados para filtros futuros, como fonte, tipo de arquivo, data e
  categoria.
- Documentar cada decisao arquitetural importante em ADR simplificado.
- Confirmar Django/DRF ou FastAPI antes de criar o scaffold da Sprint 1.

### Opcionais

- Reranking com modelo especializado.
- Busca hibrida combinando vetores e texto.
- Cache de respostas ou de embeddings.
- Autenticacao JWT, RBAC e painel administrativo.
- Observabilidade com Prometheus/Grafana ou OpenTelemetry.
- Ingestao por fila com RabbitMQ desde o inicio, caso o volume seja alto.
- Frontend Next.js na mesma sprint do backend somente se houver prazo curto para
  demonstracao visual.

## Arquitetura proposta

```text
Dashboard Next.js ou cliente HTTP
        |
        v
 Django + Django REST Framework
        |
        +--> App documents / API de documentos
        |       +--> IngestionService
        |       +--> TextExtractor
        |       +--> TextNormalizer
        |       +--> Chunker
        |       +--> EmbeddingProvider
        |       +--> QdrantVectorStore
        |       +--> DocumentRepository/PostgreSQL
        |
        +--> App rag + app chat / API de consulta
        |       +--> QueryEmbedding
        |       +--> Retriever
        |       +--> PromptBuilder
        |       +--> LLMProvider/Maritaca
        |       +--> QueryRepository/PostgreSQL
        |
        +--> App common / API de health
        +--> Django ORM/Admin/Auth

Opcional futuro:
Django --> RabbitMQ --> Worker de indexacao --> Qdrant/PostgreSQL

LangChain, encapsulado em servicos:
loaders/splitters --> embeddings --> Qdrant retriever --> prompt --> Maritaca
```

## Fluxo RAG proposto

### Ingestao

- Entrada por upload ou leitura de pasta local.
- Formatos iniciais assumidos: `.txt`, `.md` e `.pdf`.
- Cada documento recebe `document_id`, nome, origem, tipo, hash, data de
  ingestao e status.
- O hash evita reindexacao acidental de conteudo igual.

### Extracao, limpeza e normalizacao

- Extrair texto preservando metadados de origem.
- Normalizar espacos, quebras de linha repetidas e caracteres problemáticos.
- Evitar limpeza agressiva para nao perder informacao semantica.

### Chunking

- Estrategia inicial: chunk por caracteres/tokens aproximados, respeitando
  paragrafos quando possivel.
- Tamanho inicial sugerido: 800 a 1.200 tokens equivalentes.
- Overlap inicial sugerido: 100 a 200 tokens equivalentes.
- Cada chunk deve preservar `document_id`, `chunk_id`, indice, pagina quando
  existir, titulo/fonte e hash do texto.

### Embeddings

- Gerar embeddings para cada chunk.
- Modelo deve ser configuravel por variavel de ambiente.
- Em caso de falha, registrar erro e permitir reprocessamento.
- Se Maritaca nao for usado para embedding, manter provider separado.

### Vector store

- Qdrant armazena vetor e payload de metadados.
- Nome da colecao configuravel.
- Payload deve permitir filtros futuros por documento, tipo, fonte e data.
- Atualizacao inicial: reindexar documento por `document_id` removendo chunks
  antigos antes de inserir novos.

### Retrieval

- Gerar embedding da pergunta.
- Buscar no Qdrant por similaridade.
- `top_k` inicial sugerido: 4 a 6.
- Filtros por metadados entram apos o MVP basico.
- Reranking fica fora do MVP, mas a arquitetura deve permitir plug-in futuro.

### Prompting e geracao

- Prompt de sistema orienta a responder apenas com base no contexto quando a
  pergunta depender dos documentos.
- Prompt deve incluir trechos recuperados numerados e metadados.
- A resposta deve citar fontes ou IDs dos trechos usados.
- Configuracoes sugeridas: temperatura baixa, timeout e retry simples.

### API

- `GET /health`: validar se API esta ativa.
- `POST /documents/index`: indexar documento ou texto.
- `POST /chat/query`: consultar RAG.
- Respostas padronizadas com `answer`, `sources`, `metadata` e `request_id`.

### Testes

- Teste unitario de chunking.
- Teste de configuracao via variaveis de ambiente.
- Teste de montagem de prompt.
- Teste de fluxo RAG com providers falsos/mocks.
- Teste de health check.

### Observabilidade

- Logs estruturados com evento, timestamp, request_id e status.
- Logar IDs de documentos/chunks recuperados.
- Evitar logar chaves, prompts completos ou dados sensiveis em producao.

### Seguranca

- Chaves em `.env`, nunca no codigo.
- Validacao de upload e tamanho maximo de arquivo.
- Sanitizacao basica de entrada.
- Controle de acesso fica fora do MVP, mas deve ser previsto.

## Decisoes tecnicas

- MVP deve priorizar API RAG funcional antes de dashboard.
- RabbitMQ fica previsto, mas nao obrigatorio na primeira implementacao.
- PostgreSQL sera usado para metadados, auditoria e historico; Qdrant para
  vetores.
- LLM e embeddings terao interfaces separadas para facilitar troca de provider.
- Django + DRF e a opcao recomendada em avaliacao para o backend.
- LangChain sera considerado para orquestracao, encapsulado por servicos proprios.

## Arquivos criados ou alterados

- `NoSQL/TrabalhoN2/README.md`
- `NoSQL/TrabalhoN2/docs/SPRINT_0.md`
- `NoSQL/TrabalhoN2/docs/SPRINT_PLAN.md`
- `NoSQL/TrabalhoN2/docs/ADR.md`

## Implementacao

Nao houve implementacao de codigo nesta sprint. Foram criados documentos de
planejamento e arquitetura para guiar as proximas sprints.

## Como testar

Validacao desta sprint:

1. Conferir se o README descreve stack, objetivo, arquitetura e documentacao.
2. Conferir se este documento responde aos itens exigidos para Sprint 0.
3. Conferir se o plano de sprints esta incremental e prioriza MVP.
4. Conferir se as decisoes principais foram registradas em ADR.

## Documentacao atualizada

- README inicial do projeto.
- Documento completo da Sprint 0.
- Plano de sprints.
- ADRs simplificados das decisoes iniciais.

## Estrutura inicial de pastas

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

As pastas de implementacao serao criadas nas sprints correspondentes.

## Plano de sprints

Resumo:

1. Sprint 1: decisao final de framework, scaffold Django/DRF recomendado,
   configuracao e health check.
2. Sprint 2: modelos, PostgreSQL e persistencia de documentos.
3. Sprint 3: ingestao, extracao simples e chunking.
4. Sprint 4: embeddings e Qdrant.
5. Sprint 5: retrieval, prompt e geracao com Maritaca.
6. Sprint 6: testes de fluxo RAG e avaliacao minima.
7. Sprint 7: RabbitMQ e worker de indexacao.
8. Sprint 8: dashboard inicial de chat e fontes.
9. Sprint 9: KPIs, observabilidade e melhorias operacionais.
10. Sprint 10: seguranca, hardening e documentacao final.

Detalhes em `docs/SPRINT_PLAN.md`.

## Criterios de aceite do MVP

- API sobe localmente com configuracao por ambiente.
- Health check funcional.
- PostgreSQL registra documentos e status de indexacao.
- Qdrant armazena chunks vetorizados com metadados.
- Sistema indexa ao menos arquivos `.txt`, `.md` e `.pdf`.
- Usuario consegue enviar pergunta e receber resposta gerada por LLM.
- Resposta inclui fontes ou trechos usados.
- Existe teste automatizado minimo para chunking, prompt e consulta com mocks.
- README explica setup, execucao, indexacao e consulta.
- Chaves e URLs sensiveis sao configuradas por variaveis de ambiente.

## Suposicoes assumidas

- O projeto sera desenvolvido dentro de `NoSQL/TrabalhoN2`.
- O backend sera Python; Django + DRF e a opcao recomendada em avaliacao.
- LangChain podera ser usado somente atras de interfaces proprias.
- Se nao houver decisao contraria antes da Sprint 1, Django + DRF sera adotado e
  LangChain sera integrado de forma encapsulada nas sprints de RAG.
- O idioma principal dos documentos e perguntas sera portugues.
- O MVP pode iniciar sem autenticacao.
- O MVP pode iniciar com ingestao sincrona.
- RabbitMQ sera adicionado quando o fluxo basico estiver funcionando.
- O dashboard sera implementado depois que a API RAG estiver estavel.
- O modelo de embedding sera escolhido na sprint de embeddings, mantendo
  configuracao desacoplada.
- O volume inicial de dados permite execucao local com Docker Compose.

## Perguntas bloqueantes

Nao ha perguntas bloqueantes para iniciar a Sprint 1, pois existe uma suposicao
padrao documentada. As duvidas abaixo podem ser respondidas ao longo do
desenvolvimento:

- Quais documentos reais serao usados na demonstracao?
- O provider de embeddings devera ser Maritaca ou outro modelo multilíngue?
- Havera necessidade de autenticar usuarios no MVP academico?
- O dashboard sera obrigatorio para a entrega ou apenas diferencial?
- Existe limite de custo mensal para chamadas de LLM?
- Django + DRF deve ser confirmado como framework final antes da Sprint 1?
- LangChain sera aceito como dependencia de orquestracao no projeto academico?

## Checklist de conclusao

- [x] Diagnostico da stack realizado.
- [x] Pontos fortes identificados.
- [x] Riscos e lacunas listados.
- [x] Recomendacoes classificadas.
- [x] Arquitetura inicial proposta.
- [x] Fluxo RAG documentado.
- [x] Estrutura inicial de pastas proposta.
- [x] Plano de sprints definido.
- [x] Criterios de aceite do MVP definidos.
- [x] Suposicoes documentadas.
- [x] Perguntas bloqueantes avaliadas.
- [x] ADRs iniciais registrados.
- [x] Possivel troca para Django e LangChain analisada e documentada.

## Riscos e pendencias

- Escolha do modelo de embedding pode impactar qualidade e custo.
- PDFs complexos podem exigir parser mais robusto.
- Sem avaliacao quantitativa, a qualidade RAG pode parecer boa apenas por
  inspecao manual.
- Autenticacao e autorizacao devem ser revisitadas antes de qualquer uso com
  dados sensiveis.
- RabbitMQ deve entrar apenas quando houver ganho claro de robustez operacional.
- A decisao Django/DRF versus FastAPI deve ser fechada antes do scaffold.
- LangChain deve ser validado por ganho real, cobertura de testes e facilidade de
  substituicao.

## Proxima sprint prevista

Sprint 1 - Fundacao Backend: confirmar Django/DRF ou FastAPI e criar o scaffold
escolhido, configuracao por ambiente, health check, estrutura modular, README de
setup local e primeiros testes.

Sprint finalizada. Aguardando o comando `continuar` para iniciar a proxima sprint.
