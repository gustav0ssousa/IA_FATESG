# Revisao Final e Roadmap de Entrega

## Resumo executivo

O projeto possui um MVP RAG funcional e bem estruturado para consulta de
manuais tecnicos de impressoras e scanners. A arquitetura separa ingestao,
chunking, embeddings, armazenamento vetorial, retrieval, geracao, avaliacao,
observabilidade e interface web. O backend possui boa cobertura automatizada e
o frontend oferece chat, fontes, filtros tecnicos, biblioteca e operacao de
documentos.

O sistema ainda nao deve ser considerado pronto para uso tecnico sensivel ou
producao. O limiar minimo e as recusas por incompatibilidade foram implementados
na Sprint 13. A Sprint 14 moveu extracao, chunking e indexacao para o worker e
adicionou revisao de metadados. A Sprint 15 completou auditoria detalhada,
retencao, paginacao/facets e perfil Compose de producao. O bloqueio principal
restante e revisar a divergencia encontrada na reconciliacao e certificar o
quality gate na base persistente.

## Estado validado

Validacao executada em 10 de junho de 2026:

- Backend: `81` testes aprovados apos a Sprint 15.
- Django: `check` sem problemas e nenhuma migration pendente.
- Frontend: lint e build de producao aprovados.
- Frontend E2E: `8` cenarios desktop/mobile aprovados apos correcao responsiva.
- Docker Compose: imagens reconstruidas e pilha completa saudavel; API e
  frontend responderam `200`.
- Reconciliacao PostgreSQL/Qdrant: dry-run encontrou `1` vetor orfao legado e
  nenhuma ausencia.
- Quality gate RAG documentado: reprovado por Duplicate Result Rate `0.50`.

A cobertura de modelos passou a ficar visivel em mobile, mas a suite E2E
completa ainda precisa ser executada em ambiente com memoria suficiente.

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

### P0 - Bloqueiam a entrega

1. Aplicar a reconciliacao PostgreSQL/Qdrant e certificar o quality gate.
   O comando idempotente encontrou `1` vetor orfao em dry-run. Depois da revisao
   e limpeza com `--apply`, o dataset tecnico deve passar integralmente.

2. Certificar a suite E2E desktop/mobile.
   A interface mobile foi ajustada, mas a execucao completa depende de ambiente
   com memoria suficiente para Chromium e Next.js simultaneamente.

### P1 - Necessarias antes de producao

1. Provisionar o ambiente alvo.
   Configurar proxy TLS, secrets manager ou equivalente, credenciais exclusivas,
   backups e restauracao testada usando o perfil Compose de producao.

### P2 - Evolucao recomendada

- Criar indices de payload no Qdrant para campos usados como filtros.
- Ampliar avaliacao para modelo correto, pagina, alertas de seguranca, ordem de
  procedimentos e comportamento dos filtros.
- Separar contagem total da biblioteca dos filtros ativos do chat.
- Avaliar Git LFS ou armazenamento externo para manuais grandes.
- Fechar explicitamente recursos PDF apos extracao.

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

- [ ] Quality gate RAG integralmente aprovado.
- [x] Backend, lint, build e E2E desktop/mobile aprovados.
- [x] Recusa por baixa relevancia implementada e testada.
- [x] Ingestao de documentos grandes processada de forma assincrona.
- [ ] PostgreSQL e Qdrant reconciliados sem duplicatas ou orfaos.
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
