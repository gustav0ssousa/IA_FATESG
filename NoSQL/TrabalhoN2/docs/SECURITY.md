# Seguranca

## Modelo do MVP

- Apenas `frontend` e `api` publicam portas no host.
- PostgreSQL, Qdrant, RabbitMQ e embeddings ficam na rede interna do Compose.
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
- Restringir acesso ao Django Admin e criar politica de retencao.
- Revisar usuarios `staff` e revogar tokens inativos periodicamente.

## Limitacoes conhecidas

- Ha papeis simples de leitor/gestor, mas nao ha permissoes granulares ou tenants.
- O token do dashboard fica em `localStorage` e exige uma politica forte contra XSS.
- Consultas ainda nao registram o usuario responsavel.
- Perguntas completas ainda sao persistidas no PostgreSQL.
- Nao ha antivírus, DLP, OCR seguro ou varredura de documentos.
- Nao ha WAF, gerenciador externo de segredos ou alertas automaticos.
