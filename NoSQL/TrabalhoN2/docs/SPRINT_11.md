# Sprint 11 — Autenticacao e Controle de Acesso

## Objetivo

Adicionar identidade individual e separar operacoes de leitura e administracao,
preservando o perfil local e a chave de integracao existentes.

## Escopo

- TokenAuthentication do Django REST Framework.
- Endpoints de configuracao, login, perfil e logout.
- Papel leitor para consulta, busca, documentos e jobs.
- Papel gestor (`is_staff`) para ingestao, indexacao e KPIs.
- Login/logout responsivo no dashboard.
- Compatibilidade com `API_ACCESS_KEY` para integracoes.

## Fora do escopo

- Cadastro publico, recuperacao de senha e MFA.
- JWT, expiracao automatica, OIDC ou SSO.
- Permissoes granulares, tenants e auditoria por usuario.
- Politica de retencao e deploy com TLS.

## Decisões técnicas

- Autenticacao individual e ativada por `API_REQUIRE_AUTHENTICATION`.
- O perfil local permanece aberto quando a flag esta desativada.
- A chave de API representa uma integracao administrativa.
- O proxy nao injeta a chave administrativa quando login individual esta ativo.
- `is_staff` reutiliza o modelo nativo Django para o papel gestor.

## Arquivos criados ou alterados

- `backend/apps/common/permissions.py`
- `backend/apps/common/urls.py`
- `backend/apps/common/views.py`
- `backend/apps/documents/views.py`
- `backend/apps/rag/views.py`
- `backend/config/settings.py`
- `backend/tests/test_security.py`
- `frontend/app/backend-api/[...path]/route.ts`
- `frontend/app/globals.css`
- `frontend/app/page.tsx`
- `frontend/tests/dashboard.spec.ts`
- `.env.example`
- `docker-compose.yml`
- `README.md`
- `docs/ADR.md`
- `docs/CONTAINERS.md`
- `docs/DEMO_GUIDE.md`
- `docs/SECURITY.md`
- `docs/SPRINT_11.md`
- `docs/SPRINT_PLAN.md`

## Implementação

```bash
# .env
API_REQUIRE_AUTHENTICATION=True

docker compose up -d --build
docker compose exec api python manage.py createsuperuser
```

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check --settings=config.test_settings

cd frontend
npm run lint
npx playwright test --workers=1
npm run build

docker compose config --quiet
docker compose ps
```

Resultados:

- `61 passed` no backend.
- `8 passed` nos testes E2E desktop/mobile.
- Lint e build de producao do frontend concluidos.
- Compose valido e fluxo de login validado na API containerizada.

## Documentação atualizada

- README com ativacao, papeis e endpoints.
- ADR de autenticacao individual.
- Guias de seguranca, demonstracao e containers.
- Plano de sprints e este registro.

## Checklist de conclusão

- [x] Login, perfil e logout implementados.
- [x] Leitura e administracao separadas.
- [x] Dashboard autenticado e responsivo.
- [x] Chave de integracao preservada sem elevar usuarios do dashboard.
- [x] Testes e documentacao atualizados.

## Riscos e pendências

- Tokens nao expiram automaticamente.
- O dashboard mantem token em `localStorage`.
- Nao ha MFA, SSO, tenants ou permissoes granulares.
- Consultas ainda nao registram o usuario responsavel.

## Próxima sprint prevista

Uma futura Sprint 12 pode adicionar auditoria por usuario e politica de retencao,
mas nao foi iniciada.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
