# Sprint 5 - Consulta RAG e Geracao com Maritaca

## Objetivo

Completar o fluxo RAG sincrono, recuperando contexto semantico, construindo um
prompt fundamentado, chamando a Maritaca e retornando resposta com fontes.

## Escopo

- Interface `LLMProvider`.
- Provider Maritaca pela Responses API.
- Modelo padrao `sabia-4`.
- Prompt de sistema contra alucinacao e prompt injection.
- Orcamento configuravel de contexto.
- Servico de consulta RAG.
- Endpoint `POST /api/rag/query`.
- Retorno de fontes, modelo, uso e `request_id`.
- Timeout, retries, temperatura e limite de tokens configuraveis.
- Resposta controlada quando nao ha contexto.
- Testes de prompt, provider, servico e endpoint.

## Fora do escopo

- Streaming de respostas.
- Historico de conversas.
- Avaliacao quantitativa do RAG.
- Busca hibrida, filtros e reranking.
- Autenticacao e rate limiting.
- Persistencia de perguntas e respostas.
- Interface frontend.

## Decisoes tecnicas

- A Responses API foi escolhida porque e recomendada pela Maritaca para projetos
  novos e compativel com o SDK OpenAI.
- `LLMProvider` impede acoplamento do servico RAG ao SDK.
- O prompt exige respostas apenas com base no contexto e citacoes `[Fonte N]`.
- O contexto e tratado como dado nao confiavel; instrucoes contidas nele devem
  ser ignoradas.
- O contexto possui limite de caracteres para controlar custo e tamanho.
- Sem resultados recuperados, a LLM nao e chamada.
- O fluxo linear permaneceu em servicos proprios; LangChain nao adicionaria
  ganho claro nesta sprint.
- Falhas de retrieval ou geracao retornam erro controlado com `request_id`.

## Arquivos criados ou alterados

- `.env.example`
- `requirements.txt`
- `backend/config/settings.py`
- `backend/apps/rag/prompting.py`
- `backend/apps/rag/generation.py`
- `backend/apps/rag/services.py`
- `backend/apps/rag/serializers.py`
- `backend/apps/rag/views.py`
- `backend/apps/rag/urls.py`
- `backend/tests/test_rag_query.py`
- `README.md`
- `docs/ADR.md`
- `docs/SPRINT_PLAN.md`
- `docs/SPRINT_5.md`

## Implementacao

Configure a chave:

```env
MARITACA_API_KEY=sua-chave
```

Consulte:

```bash
curl -X POST http://127.0.0.1:8000/api/rag/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Como funciona o RAG?", "top_k": 5}'
```

Formato resumido:

```json
{
  "answer": "Resposta fundamentada [Fonte 1].",
  "sources": [
    {
      "number": 1,
      "source_name": "guia.md",
      "page_number": null,
      "score": 0.92,
      "content": "Trecho recuperado"
    }
  ],
  "model": "sabia-4",
  "usage": {},
  "request_id": "uuid"
}
```

## Como testar

```bash
.venv/bin/pytest
.venv/bin/python backend/manage.py check
.venv/bin/python backend/manage.py makemigrations --check --dry-run
docker compose config --quiet
```

Resultados obtidos:

- 33 testes automatizados aprovados.
- Prompt, limite de contexto e fontes validados.
- Provider Maritaca validado com cliente falso da Responses API.
- Fluxo RAG validado com retrieval e LLM falsos.
- Endpoint validado com resposta, fontes e `request_id`.
- Sem contexto, a LLM nao e chamada.
- Endpoint HTTP real sem chave retornou `502`, erro controlado e `request_id`.
- Django system check sem problemas.
- Nenhuma migration pendente.
- Chamada real à Maritaca nao executada por ausencia de `MARITACA_API_KEY`.
- PostgreSQL e Qdrant reais continuam indisponiveis porque o daemon Docker nao
  esta acessivel.

## Documentacao atualizada

- README com configuracao e consulta RAG.
- ADR da Maritaca pela Responses API.
- ADR de LangChain atualizado.
- Plano de sprints atualizado.
- Este relatorio da Sprint 5.

## Checklist de conclusao

- [x] Interface LLM implementada.
- [x] Provider Maritaca implementado.
- [x] Prompt fundamentado implementado.
- [x] Servico RAG implementado.
- [x] Endpoint de consulta implementado.
- [x] Fontes e rastreabilidade retornadas.
- [x] Testes automatizados aprovados.
- [x] Documentacao atualizada.
- [x] Chamada real à Maritaca validada posteriormente na Sprint 6.

## Riscos e pendencias

- A integracao real depende de uma chave Maritaca valida.
- Prompt injection nao pode ser eliminado apenas por instrucoes de prompt.
- O limite atual usa caracteres, nao tokens reais do modelo Maritaca.
- Nao ha threshold minimo de relevancia para descartar resultados fracos.
- Perguntas e respostas ainda nao sao persistidas.
- Endpoints continuam publicos e sem limite de taxa.

## Proxima sprint prevista

Sprint 6 - Avaliacao Minima e Testes de Fluxo: criar dataset de perguntas,
metricas iniciais de retrieval e criterios de qualidade para respostas.

Sprint finalizada. Aguardando o comando `continuar` para iniciar a proxima sprint.
