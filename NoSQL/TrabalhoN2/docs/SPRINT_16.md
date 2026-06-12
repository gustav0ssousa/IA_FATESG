# Sprint 16 — Certificacao Final da Base Controlada

## Objetivo

Certificar o fluxo RAG completo na pilha containerizada, reconciliar PostgreSQL
e Qdrant e corrigir desvios de contexto e citacao encontrados na avaliacao real
de manuais tecnicos.

## Escopo

- Formacao de uma base controlada com documento introdutorio e manual tecnico.
- Validacao da ingestao e indexacao assincrona.
- Reconciliacao entre PostgreSQL e Qdrant.
- Avaliacao de retrieval e geracao com Maritaca.
- Expansao de contexto para chunks adjacentes na mesma pagina.
- Normalizacao do contrato publico de citacoes.
- Certificacao da imagem containerizada e documentacao final.

## Fora do escopo

- Certificacao de acervos futuros ou de documentos ainda nao avaliados.
- OCR, interpretacao de diagramas e extracao estruturada de tabelas.
- Provisionamento de proxy TLS, secrets manager e backups no ambiente alvo.
- Busca hibrida, reranking e calibracao ampla para outros fabricantes.

## Decisões técnicas

- PostgreSQL permanece a fonte de verdade para a reconciliacao vetorial.
- O contexto recuperado e expandido apenas para chunks imediatamente
  adjacentes e da mesma pagina, evitando atravessar limites de pagina.
- A resposta da LLM tem citacoes normalizadas para o contrato exato
  `[Fonte N]`; pagina e demais metadados continuam nas fontes estruturadas.
- A certificacao se aplica explicitamente a base controlada atual, sem
  generalizar os resultados para novos acervos.

## Arquivos criados ou alterados

- `backend/apps/rag/services.py`
- `backend/apps/rag/prompting.py`
- `backend/tests/test_rag_query.py`
- `docs/SPRINT_16.md`
- `docs/SPRINT_PLAN.md`
- `docs/FINAL_REVIEW_AND_ROADMAP.md`
- `docs/TECHNICAL_MANUALS.md`
- `docs/ADR.md`
- `README.md`
- `outputs/sprint16/`

## Implementação

A base controlada possui:

- `data/samples/rag_overview.md`: `1` chunk.
- `docs/sm_elfb_e_ver2.pdf`: `802` chunks em `513` paginas.
- Total: `2` documentos indexados e `803` chunks/vetores.

O servico de consulta agora inclui o chunk anterior e posterior ao resultado
semantico quando pertencem ao mesmo documento e pagina. Isso recupera detalhes
tecnicos separados apenas pelo limite de chunk. Citacoes agrupadas ou
enriquecidas pela LLM sao convertidas para referencias individuais no formato
aceito pela API.

Relatorios da certificacao:

- `outputs/sprint16/rag/evaluation.md`
- `outputs/sprint16/technical/evaluation.md`
- `outputs/sprint16/technical-generation-image/evaluation.md`

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check --settings=config.test_settings
.venv/bin/python backend/manage.py makemigrations --check --dry-run \
  --settings=config.test_settings
docker compose exec -T api python manage.py check
docker compose exec -T api python manage.py reconcile_qdrant
docker compose ps
```

Resultados:

- Backend: `85 passed`.
- Django check: sem problemas; nenhuma migration pendente.
- Reconciliacao: `803` esperados, `803` escaneados, `0` orfaos e `0` ausentes.
- API e frontend: HTTP `200`; todos os servicos saudaveis.
- Compose local e override de producao: configuracoes validas.
- Quality gate na imagem: aprovado nos `4` casos tecnicos.
- Retrieval Hit Rate, MRR e Precision@k: `1.000`.
- Duplicate Result Rate: `0.000`.
- Citation Rate: `1.000`.
- Answer Term Recall: `0.889`.
- Refusal Accuracy: `1.000`.
- Generation Errors: `0`.

A imagem de runtime nao inclui a configuracao de testes do ambiente de
desenvolvimento. Por isso, a suite automatizada e executada no `.venv`; a
imagem e validada por checks Django, reconciliacao, health checks e avaliacao
RAG real.

## Documentação atualizada

- README: baseline certificado, estado atual e indice de documentacao.
- ADR: expansao adjacente na mesma pagina e normalizacao de citacoes.
- Manuais tecnicos: comportamento de contexto e limites.
- Roadmap: Definition of Done e riscos residuais.
- Plano de sprints: Sprint 16 concluida.

## Checklist de conclusão

- [x] Base controlada indexada pelo pipeline real.
- [x] PostgreSQL e Qdrant reconciliados.
- [x] Contexto adjacente implementado e testado.
- [x] Contrato de citacoes normalizado e testado.
- [x] Quality gate de retrieval e geracao aprovado.
- [x] Imagem containerizada e servicos saudaveis.
- [x] Documentacao atualizada com resultados e limites.

## Riscos e pendências

- A certificacao nao garante qualidade para novos fabricantes ou formatos.
- O modelo FastEmbed instalado emitiu aviso sobre mudanca para mean pooling;
  a versao e o baseline devem ser fixados antes de atualizacoes em producao.
- OCR, tabelas complexas e diagramas continuam sem interpretacao dedicada.
- TLS, segredos externos, backups e restauracao ainda dependem do ambiente alvo.

## Próxima sprint prevista

Nao ha nova sprint obrigatoria para o MVP. A proxima evolucao deve ser definida
por prioridade operacional: ambiente alvo, ampliacao do dataset ou suporte a
OCR e conteúdo multimodal.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
