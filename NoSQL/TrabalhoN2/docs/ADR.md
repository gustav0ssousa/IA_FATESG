# Decisoes Arquiteturais

## ADR: Backend com FastAPI

### Contexto

O projeto precisa expor endpoints para ingestao, consulta RAG, health check e
futuramente dashboard. A API deve ser simples de testar, documentar e evoluir.

### Decisao

Usar FastAPI como framework backend principal.

### Alternativas consideradas

- Flask: mais simples, mas com menos suporte nativo a tipagem e validacao.
- Django: robusto, mas mais pesado para o MVP RAG.
- Node.js/Express: viavel, mas a stack de IA em Python e mais conveniente.

### Consequencias

FastAPI facilita validacao com Pydantic, documentacao OpenAPI e testes. A
principal consequencia e manter disciplina na organizacao de modulos para evitar
que a aplicacao cresca como um unico arquivo.

## ADR: Qdrant como banco vetorial

### Contexto

O sistema precisa armazenar embeddings e recuperar chunks por similaridade, com
metadados e filtros futuros.

### Decisao

Usar Qdrant como vector store.

### Alternativas consideradas

- pgvector no PostgreSQL: reduziria componentes, mas pode limitar recursos
  dedicados de busca vetorial dependendo do caso.
- Chroma: simples para prototipos, mas menos indicado como alvo de producao.
- Weaviate/Milvus: poderosos, porem podem adicionar complexidade ao MVP.

### Consequencias

Qdrant oferece boa separacao de responsabilidades e filtros por payload. A
desvantagem e adicionar mais um servico para configurar e monitorar.

## ADR: PostgreSQL para metadados e auditoria

### Contexto

O RAG precisa registrar documentos, status, chunks, historico de consultas e
metadados estruturados que nao pertencem exclusivamente ao banco vetorial.

### Decisao

Usar PostgreSQL como banco estruturado principal.

### Alternativas consideradas

- Apenas Qdrant: simplificaria a stack, mas dificultaria auditoria e consultas
  relacionais.
- MongoDB: flexivel, mas a stack proposta ja inclui PostgreSQL e o historico do
  RAG se encaixa bem em modelo relacional.

### Consequencias

PostgreSQL aumenta rastreabilidade e facilita dashboards/KPIs. A desvantagem e
exigir modelagem e migracoes.

## ADR: RabbitMQ adiado para depois do MVP funcional

### Contexto

RabbitMQ e util para indexacao assíncrona, mas pode aumentar complexidade antes
de o fluxo RAG basico estar validado.

### Decisao

Prever RabbitMQ na arquitetura, mas iniciar com ingestao sincrona. Introduzir
fila quando a indexacao basica ja estiver funcionando.

### Alternativas consideradas

- Usar RabbitMQ desde a Sprint 1: melhora arquitetura operacional, mas aumenta
  tempo ate o primeiro MVP.
- Nunca usar fila: mais simples, mas pior para documentos grandes e retentativas.

### Consequencias

A decisao acelera o MVP e reduz risco inicial. O impacto negativo e que a sprint
de RabbitMQ exigira adaptar endpoints e status de indexacao.

## ADR: Interfaces separadas para LLM e embeddings

### Contexto

O provider de LLM esta planejado como Maritaca, mas o modelo de embedding ainda
nao esta definido e pode mudar por custo, qualidade ou disponibilidade.

### Decisao

Criar abstracoes separadas para `LLMProvider` e `EmbeddingProvider`.

### Alternativas consideradas

- Acoplar tudo diretamente a Maritaca: simples inicialmente, mas dificulta
  testes e troca de modelo.
- Usar framework RAG completo desde o inicio: acelera algumas partes, mas pode
  esconder decisoes importantes para fins academicos.

### Consequencias

A arquitetura fica mais testavel e flexivel. A desvantagem e um pouco mais de
codigo de infraestrutura no MVP.

## ADR: Orquestracao RAG propria no MVP

### Contexto

Frameworks como LangChain ou LlamaIndex podem acelerar prototipos, mas tambem
adicionam abstracoes e dependencias.

### Decisao

Implementar a orquestracao RAG principal com servicos proprios em Python no MVP.

### Alternativas consideradas

- LangChain: ecossistema amplo, mas pode gerar acoplamento e complexidade.
- LlamaIndex: forte para indexacao e RAG, mas pode esconder detalhes do fluxo.

### Consequencias

O projeto fica mais didatico, controlavel e facil de testar por partes. A
desvantagem e implementar manualmente alguns componentes que frameworks ja
oferecem.
