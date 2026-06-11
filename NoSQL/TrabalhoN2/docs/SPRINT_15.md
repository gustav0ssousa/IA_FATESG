# Sprint 15 — Auditoria, Deploy e Fechamento

## Objetivo

Fechar rastreabilidade das consultas, reduzir exposicao operacional e preparar
o MVP para validacao final em ambiente alvo.

## Escopo

- Auditoria de identidade, autenticacao, filtros e fontes detalhadas.
- Redacao da pergunta por padrao e politica executavel de retencao.
- Perfil Compose de producao sem portas de dados publicadas.
- Paginacao e facets globais da biblioteca.
- Testes, documentacao de seguranca, backup e operacao.

## Fora do escopo

- Provisionamento de proxy TLS, secrets manager e backups gerenciados.
- SSO, tenants, antivirus, OCR e permissoes granulares.
- Aplicacao destrutiva da reconciliacao na base persistente sem revisao.

## Decisões técnicas

- Perguntas sao correlacionadas por SHA-256; texto integral e opt-in.
- Retencao usa comando dry-run por padrao e exige `--apply`.
- O Compose local permanece ergonomico; `docker-compose.prod.yml` endurece o
  ambiente alvo.
- Facets sao calculadas sobre todo o acervo, independentes da pagina atual.

## Arquivos criados ou alterados

- `backend/apps/rag/models.py`, `views.py`, `admin.py` e migration `0003`.
- `backend/apps/rag/management/commands/purge_rag_audit.py`.
- `backend/apps/documents/views.py` e serializers.
- `backend/tests/test_query_audit.py` e testes da biblioteca.
- `frontend/app/page.tsx`, estilos e E2E.
- `docker-compose.prod.yml`, `.env.example`, README, ADR e guias operacionais.

## Implementação

Consultas agora registram usuario quando autenticado, metodo de autenticacao,
filtros, hash da pergunta e fontes com chunk, pagina e metadados. A biblioteca
retorna `results`, `pagination` e `facets`. O override de producao exige
segredos, ativa configuracoes HTTPS e remove publicacoes de PostgreSQL/Qdrant.

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check --settings=config.test_settings
.venv/bin/python backend/manage.py makemigrations --check --dry-run \
  --settings=config.test_settings
cd frontend && npm run lint && npm run build && npm run test:e2e -- --workers=1
DJANGO_ALLOWED_HOSTS=rag.example.com DJANGO_SECRET_KEY=test \
  API_ACCESS_KEY=test docker compose -f docker-compose.yml \
  -f docker-compose.prod.yml config --quiet
```

## Documentação atualizada

- README: auditoria, retencao, variaveis e estado final.
- ADR: minimizacao de auditoria e perfil de producao.
- Seguranca e containers: TLS, segredos, portas, backup e operacao.
- Roadmap e plano de sprints: entregas e riscos residuais.

Resultados:

- Backend: `81 passed`.
- Django check e migrations: aprovados.
- Frontend lint e build: aprovados.
- E2E desktop/mobile: `8 passed`.
- Compose local e override de producao: configuracoes validas.
- Pilha local reconstruida; API, frontend, worker e dependencias saudaveis.
- Migration `rag.0003_query_audit_details` aplicada na base persistente.
- Retencao real em dry-run: `0` registros anteriores a 90 dias.
- Reconciliacao real em dry-run: `1` vetor orfao e `0` chunks ausentes.

## Checklist de conclusão

- [x] Auditoria registra identidade, filtros, chunks, paginas e metadados.
- [x] Pergunta integral fica redigida por padrao.
- [x] Retencao possui dry-run e aplicacao explícita.
- [x] Perfil Compose de producao remove portas de dados.
- [x] Biblioteca possui paginacao e facets globais.
- [x] Backend, Django, lint, build, E2E e Compose validados.
- [ ] Reconciliacao destrutiva e quality gate aprovados na base persistente.

## Riscos e pendências

- TLS, segredos externos e backups precisam ser provisionados no ambiente alvo.
- A base persistente ainda possui vetor orfao identificado em dry-run.
- OCR, tabelas/diagramas e protecoes avancadas permanecem evolucoes futuras.

## Próxima sprint prevista

Nao ha nova sprint funcional planejada. O proximo passo e executar o checklist
operacional final no ambiente alvo e decidir o tratamento do vetor orfao.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
