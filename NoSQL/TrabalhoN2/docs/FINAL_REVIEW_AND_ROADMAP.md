# Revisao Final e Roadmap de Entrega

## Resumo executivo

O projeto possui um MVP RAG funcional e bem estruturado para consulta de
manuais tecnicos de impressoras e scanners. A arquitetura separa ingestao,
chunking, embeddings, armazenamento vetorial, retrieval, geracao, avaliacao,
observabilidade e interface web. O backend possui boa cobertura automatizada e
o frontend oferece chat, fontes, filtros tecnicos, biblioteca e operacao de
documentos.

O MVP foi certificado na Sprint 16 para a base controlada atual. A reconciliacao
PostgreSQL/Qdrant ficou limpa e o quality gate de retrieval e geracao foi
aprovado dentro da imagem containerizada. O sistema ainda nao substitui
orientacao oficial ou avaliacao de tecnico qualificado e precisa de
provisionamento operacional antes de producao.

## Estado validado

Validacao consolidada em 12 de junho de 2026:

- Backend: `85` testes aprovados apos a Sprint 16.
- Django: `check` sem problemas e nenhuma migration pendente.
- Frontend: lint e build de producao aprovados.
- Frontend E2E: `8` cenarios desktop/mobile aprovados apos correcao responsiva.
- Docker Compose: imagens reconstruidas e pilha completa saudavel; API e
  frontend responderam `200`.
- Base controlada: `2` documentos, `803` chunks e `803` vetores.
- Reconciliacao PostgreSQL/Qdrant: `0` orfaos e `0` ausentes.
- Quality gate RAG na imagem: aprovado nos `4` casos tecnicos, com Citation Rate
  `1.00`, Answer Term Recall `0.889` e Duplicate Result Rate `0.00`.

## Pontos fortes

- Stack coerente: Django/DRF, PostgreSQL, Qdrant, Celery/RabbitMQ, servico de
  embeddings isolado, Maritaca e Next.js.
- Interfaces desacopladas para LLM, embeddings e vector store.
- Citacoes com pagina, trecho e metadados tecnicos.
- Filtros exatos por fabricante, modelo, equipamento e tipo de manual.
- Prompt especializado com orientacao de seguranca.
- Autenticacao, chave de API opcional, throttling, logs estruturados e KPIs.
- Avaliacao versionada e documentacao suficiente para continuidade.

## Pendencias por prioridade

### P1 - Necessarias antes de producao

1. Provisionar o ambiente alvo.
   Configurar proxy TLS, secrets manager ou equivalente, credenciais exclusivas,
   backups e restauracao testada usando o perfil Compose de producao.

2. Fixar e revisar o baseline do modelo de embeddings.
   A biblioteca FastEmbed instalada informou mudanca de pooling; uma atualizacao
   pode alterar scores e exige reindexacao e nova avaliacao.

3. Implementar exclusao consistente de documentos.
   A operacao deve remover arquivo, chunks, jobs e vetores de forma idempotente
   e auditavel.

### P2 - Evolucao recomendada

- Criar indices de payload no Qdrant para campos usados como filtros.
- Ampliar avaliacao para modelo correto, pagina, alertas de seguranca, ordem de
  procedimentos e comportamento dos filtros.
- Separar contagem total da biblioteca dos filtros ativos do chat.
- Avaliar Git LFS ou armazenamento externo para manuais grandes.
- Processar PDFs de forma incremental e fechar recursos explicitamente.

### P3 - Evolucao opcional

- OCR para documentos digitalizados.
- Extracao estruturada de tabelas e diagramas.
- Busca hibrida, reranking e cache.
- SSO, MFA, tenants e permissoes granulares.
- Antivirus, DLP, WAF, secrets manager e alertas automaticos.

## Fases finais de implementacao

### Fase 1 - Gate de confiabilidade

Escopo:

- Limiar configuravel de score e resposta de recusa.
- Testes para pergunta irrelevante, modelo incorreto e filtros sem resultado.
- Comando idempotente de reconciliacao PostgreSQL/Qdrant.
- Reexecucao da avaliacao tecnica.

Aceite:

- Perguntas sem contexto suficiente nao chamam a LLM.
- Duplicate Result Rate igual a `0`.
- Todos os limites do quality gate aprovados.
- Suite backend totalmente verde.

### Fase 2 - Ingestao e ciclo de vida

Escopo:

- Upload persistido e jobs assincronos para extracao, chunking e indexacao.
- Estados claros: recebido, processando, chunked, indexando, pronto e falhou.
- Retentativa idempotente, reprocessamento e remocao consistente.
- Endpoint e interface para editar/revisar metadados.

Aceite:

- Upload de manual grande nao bloqueia worker HTTP nem mantem todo o arquivo em
  memoria.
- Falhas podem ser reprocessadas sem duplicar chunks ou vetores.
- Metadados corrigidos aparecem nos filtros e no retrieval.

### Fase 3 - Auditoria, seguranca e deploy

Escopo:

- Auditoria por usuario, filtros, chunks e paginas.
- Politica de retencao e redacao de dados sensiveis.
- Restricao de portas de dados e perfil separado para desenvolvimento.
- Validacao integral do Compose, TLS e credenciais de producao.

Aceite:

- Cada consulta pode ser rastreada sem expor dados alem da politica definida.
- PostgreSQL, Qdrant, RabbitMQ e embeddings nao ficam publicamente acessiveis.
- Health checks, migrations, API, worker, frontend e avaliacao funcionam no
  ambiente containerizado.

### Fase 4 - Qualidade tecnica e experiencia

Escopo:

- Corrigir comportamento mobile e deixar E2E totalmente verde.
- Paginar biblioteca e criar facets de fabricante/modelo.
- Expandir dataset com casos de seguranca, modelo e procedimento.
- Adicionar indices de payload do Qdrant.

Aceite:

- Lint, build e E2E desktop/mobile aprovados.
- Filtros continuam eficientes com crescimento do acervo.
- Avaliacao detecta resposta para modelo errado e omissao de alerta critico.

## Definition of Done da entrega final

- [x] Quality gate RAG integralmente aprovado na base controlada.
- [x] Backend, lint, build e E2E desktop/mobile aprovados.
- [x] Recusa por baixa relevancia implementada e testada.
- [x] Ingestao de documentos grandes processada de forma assincrona.
- [x] PostgreSQL e Qdrant reconciliados sem duplicatas ou orfaos.
- [x] Auditoria registra identidade, filtros e fontes recuperadas.
- [x] Metadados tecnicos podem ser revisados e corrigidos.
- [x] Compose completo validado e perfil de producao sem exposicao de dados.
- [x] Segredos, TLS, retencao e backup documentados para o ambiente alvo.
- [x] README, ADRs, seguranca, operacao e demonstracao refletem o estado real.

## Comandos de validacao final

```bash
.venv/bin/python -m pytest
.venv/bin/python backend/manage.py check
.venv/bin/python backend/manage.py makemigrations --check --dry-run
.venv/bin/python backend/manage.py evaluate_rag \
  --dataset data/evaluation/technical_manual_cases.json \
  --output outputs/evaluation/technical-manual
cd frontend && npm run lint && npm run build && npm run test:e2e
docker compose config
docker compose up --build
```

## Riscos residuais

Mesmo apos o fechamento do MVP, respostas geradas nao substituem procedimentos
oficiais nem avaliacao de um tecnico qualificado. Instrucoes envolvendo energia,
calor, partes moveis, consumiveis ou desmontagem devem preservar alertas do
manual, citar a fonte e recusar orientacao quando o contexto nao for suficiente.

A classificacao final, arquitetura consolidada, lacunas e checklist de promocao
para producao estao em [`FINAL_DELIVERY.md`](FINAL_DELIVERY.md).
