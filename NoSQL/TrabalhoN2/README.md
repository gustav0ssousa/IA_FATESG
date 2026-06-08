# Sistema RAG Adaptativo - Trabalho N2

Este projeto sera desenvolvido em sprints para criar um sistema RAG
(`Retrieval-Augmented Generation`) funcional, modular, testavel e evolutivo.

## Stack planejada

| Campo | Definicao inicial |
| --- | --- |
| Linguagem | Python no backend; TypeScript se houver frontend |
| Framework backend | FastAPI |
| Framework frontend | Next.js + Tailwind CSS, previsto para fase posterior |
| LLM provider | Maritaca, com interface desacoplada para troca futura |
| Modelo de embedding | A definir na implementacao; preferencia por modelo multilíngue adequado a portugues |
| Banco vetorial | Qdrant |
| Banco relacional/documental | PostgreSQL |
| Orquestracao RAG | Servicos proprios em Python, sem framework pesado no MVP |
| Sistema de autenticacao | Fora do MVP inicial; JWT/API key previsto depois |
| Infra/deploy | Docker Compose local; producao a definir |
| Observabilidade/logs | Logs estruturados no backend; metricas basicas em fase posterior |
| Ambiente local | Docker Compose + Python virtualenv |
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
 FastAPI
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

RabbitMQ sera introduzido quando a ingestao assíncrona fizer sentido para o
volume ou para evitar bloqueio das requisicoes HTTP. No MVP inicial, a ingestao
pode ser sincrona para reduzir complexidade.

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
    app/
      api/
      core/
      models/
      schemas/
      services/
      repositories/
      rag/
    tests/
  data/
    samples/
  docker/
```

## Documentacao

- [Sprint 0 - Analise e planejamento](docs/SPRINT_0.md)
- [Plano de sprints](docs/SPRINT_PLAN.md)
- [Decisoes arquiteturais](docs/ADR.md)

## Status

Sprint 0 concluida. A proxima etapa prevista e a Sprint 1, com scaffold do
backend FastAPI, configuracao local e health check.
