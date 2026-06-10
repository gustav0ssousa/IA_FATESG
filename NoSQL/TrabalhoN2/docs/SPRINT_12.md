# Sprint 12 — Especializacao em Manuais Tecnicos

## Objetivo

Adaptar o RAG ao contexto principal de manuais tecnicos de impressoras e
scanners, mantendo expansao para outras tecnologias.

## Escopo

- Analise estrutural do manual Brother de exemplo.
- Extracao eficiente de PDFs extensos e protegidos.
- Upload padrao ampliado para 75 MB.
- Metadados tecnicos inferidos e sobrescreviveis.
- Classificacao tecnica por chunk.
- Filtros de retrieval por equipamento/modelo/conteudo.
- Prompt orientado a suporte tecnico e seguranca.
- Dataset de avaliacao do domínio.

## Fora do escopo

- OCR, visao computacional e interpretação de diagramas.
- Extracao estruturada de tabelas complexas.
- Tradução/indexação multilíngue dedicada.
- Ingestao/extracao inteiramente assincrona.
- Recomendacao automatica de pecas ou execucao de reparos.

## Decisões técnicas

- PyMuPDF e o extrator primario por desempenho e suporte ao PDF AES do exemplo.
- `pypdf` permanece como fallback.
- Metadados ficam em JSON para permitir evolucao sem migrations frequentes.
- Heuristicas sao genericas e recebem vocabulário inicial do domínio.
- Filtros Qdrant evitam mistura de modelos quando o chamador conhece o alvo.
- Prompt prioriza alertas e distingue usuario de tecnico.

## Arquivos criados ou alterados

- `backend/apps/documents/technical.py`
- `backend/apps/documents/chunking.py`
- `backend/apps/documents/extractors.py`
- `backend/apps/documents/serializers.py`
- `backend/apps/documents/services.py`
- `backend/apps/documents/views.py`
- `backend/apps/rag/prompting.py`
- `backend/apps/rag/serializers.py`
- `backend/apps/rag/services.py`
- `backend/apps/rag/vector_store.py`
- `backend/apps/rag/views.py`
- `backend/config/settings.py`
- `backend/tests/test_chunking.py`
- `backend/tests/test_rag_vector_store.py`
- `backend/tests/test_technical_documents.py`
- `data/evaluation/technical_manual_cases.json`
- `frontend/app/page.tsx`
- `.env.example`
- `requirements.txt`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_12.md`
- `docs/SPRINT_PLAN.md`
- `docs/TECHNICAL_MANUALS.md`

## Implementação

Ingerir o manual:

```bash
curl -X POST http://127.0.0.1:8000/api/documents/ingest \
  -F "file=@docs/sm_elfb_e_ver2.pdf"
```

Consultar com filtro:

```bash
curl -X POST http://127.0.0.1:8000/api/rag/query \
  -H "Content-Type: application/json" \
  -d '{"question":"O que verificar antes do troubleshooting?","manufacturer":"Brother","model":"MFC-L5710DN"}'
```

## Como testar

```bash
.venv/bin/pytest -q
.venv/bin/python backend/manage.py check --settings=config.test_settings

cd frontend
npm run lint
npm run test:e2e -- --workers=1
npm run build

docker compose up -d --build
docker compose ps
```

Resultados:

- `66 passed` no backend.
- `8 passed` nos testes E2E desktop/mobile.
- Lint e build de producao concluídos.
- Manual extraído e classificado integralmente em cerca de 18 segundos.
- 513 paginas, 25 modelos e 802 chunks analisados.
- Reconstrucao containerizada pendente porque o daemon Docker ficou indisponivel.

## Documentação atualizada

- Contexto, metadados, filtros e limites em `TECHNICAL_MANUALS.md`.
- README, ADR, plano de sprints e este registro.

## Checklist de conclusão

- [x] Manual de exemplo analisado.
- [x] PDF extenso/protegido suportado.
- [x] Estrutura tecnica preservada em metadados.
- [x] Filtros por modelo e domínio implementados.
- [x] Prompt e dataset adaptados.
- [x] Testes e documentacao concluídos.

## Riscos e pendências

- PDFs escaneados e diagramas ainda exigem OCR/multimodal.
- Tabelas podem perder alinhamento sem parser especializado.
- Ingestao de arquivos muito grandes ainda ocupa o request HTTP.
- Novas marcas exigirao validacao das heuristicas e dataset.
- A imagem Docker com as novas dependencias deve ser reconstruida quando o daemon
  estiver novamente disponivel.

## Próxima sprint prevista

Uma futura Sprint 13 pode mover toda a ingestao para jobs e adicionar OCR ou
extracao multimodal, mas nao foi iniciada.

> Sprint finalizada. Aguardando o comando `continuar` para iniciar a próxima sprint.
