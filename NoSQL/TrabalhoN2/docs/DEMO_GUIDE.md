# Guia de Demonstracao

## Preparacao

```bash
cp .env.example .env
# Configure MARITACA_API_KEY no .env.
docker compose up -d --build
docker compose ps
```

Confirme que todos os servicos estao saudaveis e abra
`http://127.0.0.1:3000`.

## Roteiro sugerido

1. Em **Documentos**, envie `docs/sm_elfb_e_ver2.pdf`.
2. Acompanhe o job passar pela fila ate o estado **Concluido**.
3. Em **Chat**, pergunte: `O que verificar no papel antes de iniciar o troubleshooting?`
4. Abra as fontes e mostre pagina, secao tecnica, score e trecho.
5. Em **Indicadores**, mostre latencia, taxa de erro e documentos recuperados.
6. Mostre a correlacao operacional:

```bash
docker compose logs --tail=30 api
```

7. Execute os checks principais:

```bash
.venv/bin/pytest -q
docker compose exec api python manage.py check
```

## Demonstrar protecao por chave

Defina `API_ACCESS_KEY` no `.env`, recrie `api` e `frontend`, e compare:

```bash
curl -i http://127.0.0.1:8000/api/documents/
curl -i -H "X-API-Key: $API_ACCESS_KEY" http://127.0.0.1:8000/api/documents/
```

O dashboard continua funcionando porque injeta a chave no proxy server-side.

## Demonstrar login e papeis

Defina `API_REQUIRE_AUTHENTICATION=True`, recrie API e frontend e crie um gestor:

```bash
docker compose up -d --build api frontend
docker compose exec api python manage.py createsuperuser
```

Ao abrir o dashboard, autentique-se com o gestor. Um usuario sem `is_staff`
consegue consultar e listar documentos, mas nao visualiza controles
administrativos.

## Pontos tecnicos para apresentar

- Django/DRF organiza API, ORM, migrations, Admin e validacao.
- PostgreSQL guarda documentos, chunks, jobs e auditoria de consultas.
- Qdrant executa busca vetorial; RabbitMQ/Celery desacoplam indexacao.
- FastEmbed gera embeddings locais; Maritaca gera respostas fundamentadas.
- LangChain foi usado somente no splitter, atras de servicos proprios.
- Fontes e `request_id` tornam respostas e operacao rastreaveis.
- Tokens DRF identificam usuarios e `is_staff` separa leitura de administracao.
- O manual Brother de 513 paginas e classificado por modelo, secao, erro e seguranca.
- Filtros por modelo reduzem o risco de misturar procedimentos de equipamentos.
