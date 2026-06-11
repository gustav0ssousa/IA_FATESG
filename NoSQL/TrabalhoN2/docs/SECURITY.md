# Seguranca

## Modelo do MVP

- `frontend` e `api` publicam portas no host para uso da aplicacao.
- No Compose local, PostgreSQL publica somente em `127.0.0.1` e Qdrant publica
  portas para diagnostico. O override de producao remove ambas as publicacoes.
- Containers da aplicacao executam sem privilegios.
- Segredos ficam no `.env`, que nao deve ser versionado.
- A API pode exigir token individual ou `X-API-Key`.
- O Django Admin usa autenticacao nativa do Django.

Com `API_REQUIRE_AUTHENTICATION=True`, usuarios comuns possuem acesso de leitura
e consulta; usuarios `staff` possuem acesso a ingestao, indexacao e KPIs.
`API_ACCESS_KEY` permanece como credencial administrativa compartilhada para
integracoes e deve ser longa, rotacionada e mantida fora do repositorio.

## Protecoes implementadas

- Limite configuravel de upload e formatos permitidos `.txt`, `.md` e `.pdf`.
- Throttling global configuravel pelo Django REST Framework.
- Tokens individuais revogaveis por logout e papeis baseados em `is_staff`.
- IDs de requisicao UUID para correlacao sem aceitar conteudo arbitrario.
- Erros padronizados com mensagem controlada e `request_id`.
- Detalhes de falhas persistidos/logados sem serem devolvidos ao cliente.
- Perguntas mascaradas no endpoint de KPIs por padrao.
- Auditoria registra identidade, metodo de autenticacao, filtros, hash da
  pergunta, chunks, paginas e metadados recuperados.
- Texto integral da pergunta nao e persistido por padrao.
- Comando de retencao executa dry-run por padrao e exige `--apply` para apagar.
- Headers contra MIME sniffing, clickjacking e vazamento de referrer.
- Cookies seguros e redirecionamento HTTPS configuraveis por ambiente.

## Checklist de deploy

- Definir `DJANGO_DEBUG=False` e uma `DJANGO_SECRET_KEY` longa e aleatoria.
- Definir hosts exatos em `DJANGO_ALLOWED_HOSTS`.
- Definir `API_ACCESS_KEY` longa e rotacionavel.
- Ativar `API_REQUIRE_AUTHENTICATION=True` em ambientes multiusuario.
- Trocar credenciais padrao de PostgreSQL e RabbitMQ.
- Servir por TLS e ativar `DJANGO_SECURE_SSL_REDIRECT=True` e
  `DJANGO_SECURE_COOKIES=True`.
- Manter `OBSERVABILITY_EXPOSE_QUESTION_TEXT=False` para dados sensiveis.
- Manter `AUDIT_STORE_QUESTION_TEXT=False` e definir `AUDIT_RETENTION_DAYS`.
- Restringir acesso ao Django Admin e agendar `purge_rag_audit --apply`.
- Revisar usuarios `staff` e revogar tokens inativos periodicamente.
- Usar `docker-compose.prod.yml`, que remove as portas de PostgreSQL e Qdrant e
  publica API/frontend apenas em loopback para um proxy TLS.

## Auditoria e retencao

Por padrao, cada consulta persiste um SHA-256 da pergunta, identidade quando
disponivel, metodo de autenticacao, filtros e fontes usadas. Habilitar
`AUDIT_STORE_QUESTION_TEXT=True` aumenta a rastreabilidade, mas exige base legal,
controle de acesso e retencao compatíveis com os dados tratados.

```bash
docker compose exec api python manage.py purge_rag_audit --days 90
docker compose exec api python manage.py purge_rag_audit --days 90 --apply
```

## TLS, segredos e backup

- Terminar TLS em proxy reverso ou load balancer e manter API/frontend em
  loopback ou rede privada.
- Fornecer segredos por mecanismo externo no ambiente alvo; `.env` e apenas uma
  conveniencia local.
- Fazer backup consistente de PostgreSQL, Qdrant e `document_uploads`.
- Testar restauracao periodicamente; copiar volumes ativos sem consistencia nao
  constitui estrategia de backup.

## Limitacoes conhecidas

- Ha papeis simples de leitor/gestor, mas nao ha permissoes granulares ou tenants.
- O token do dashboard fica em `localStorage` e exige uma politica forte contra XSS.
- Nao ha antivírus, DLP, OCR seguro ou varredura de documentos.
- Nao ha WAF, gerenciador externo de segredos ou alertas automaticos.
