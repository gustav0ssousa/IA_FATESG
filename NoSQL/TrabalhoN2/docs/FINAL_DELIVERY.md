# Documentacao Final de Entrega

## Identificacao

- Projeto: Sistema RAG Adaptativo para Manuais Tecnicos.
- Dominio inicial: impressoras, scanners e multifuncionais.
- Data da auditoria final: 12 de junho de 2026.
- Classificacao: MVP certificado para demonstracao e piloto controlado.
- Producao publica: condicionada ao checklist de promocao deste documento.

## Resumo executivo

O projeto entrega um sistema RAG completo para ingerir manuais tecnicos,
classificar metadados, gerar embeddings, recuperar contexto e responder com
fontes inspecionaveis. O fluxo principal foi validado com um manual Brother de
`513` paginas e um documento introdutorio, totalizando `803` chunks e vetores.

A arquitetura possui separacao clara entre API, processamento assincrono,
persistencia relacional, busca vetorial, geracao e dashboard. O MVP atende ao
objetivo academico e esta apto para demonstracao e piloto com acervo controlado.
Ainda nao deve ser exposto como servico publico sem infraestrutura de producao,
backup restauravel, gestao externa de segredos e monitoramento.

## Estado da entrega

| Area | Estado | Evidencia |
| --- | --- | --- |
| Ingestao `.txt`, `.md`, `.pdf` | Entregue | Upload persistido e pipeline no worker |
| Extracao e chunking | Entregue | Paginas e metadados tecnicos preservados |
| Embeddings | Entregue | FastEmbed em servico interno compartilhado |
| Vector store | Entregue | Qdrant com filtros e reconciliacao |
| Consulta RAG | Entregue | Maritaca, recusa e fontes `[Fonte N]` |
| Dashboard | Entregue | Chat, biblioteca, jobs, filtros e KPIs |
| Autenticacao | Entregue para MVP | Token DRF, `is_staff` e chave de API |
| Observabilidade | Entregue para MVP | Logs JSON, auditoria e KPIs |
| Containers | Entregue | Compose local e override endurecido |
| Producao publica | Pendente | Requer itens P1 do checklist |

## Arquitetura atual

```text
Navegador
  |
  v
Next.js dashboard e proxy server-side
  |
  v
Django REST Framework
  |-- PostgreSQL: documentos, chunks, jobs, usuarios e auditoria
  |-- Qdrant: vetores, payloads e busca semantica
  |-- Maritaca: geracao da resposta
  |-- RabbitMQ -> Celery worker: extracao, chunking e indexacao
  `-- Servico interno FastEmbed: embeddings compartilhados
```

### Componentes

- `frontend`: dashboard Next.js responsivo e proxy para a API.
- `api`: contratos HTTP, autenticacao, consulta RAG, KPIs e administracao.
- `worker`: pipeline assincrono com retentativas.
- `embeddings`: modelo ONNX compartilhado somente na rede interna.
- `postgres`: fonte de verdade dos dados estruturados.
- `qdrant`: indice vetorial reconciliavel a partir do PostgreSQL.
- `rabbitmq`: broker das tarefas Celery.

## Fluxos principais

### Ingestao

1. API valida formato e limite do upload.
2. Hash SHA-256 impede duplicacao do mesmo arquivo.
3. Arquivo original e persistido no volume compartilhado.
4. Job Celery extrai texto e paginas.
5. Heuristicas inferem fabricante, modelos, manual, secoes, erros e seguranca.
6. LangChain Text Splitters cria chunks com overlap.
7. PostgreSQL recebe chunks; FastEmbed gera vetores; Qdrant recebe payloads.
8. Documento passa para `indexed`, ou registra falha e retentativas.

### Consulta

1. API valida pergunta, filtros e identidade.
2. FastEmbed gera o vetor da pergunta.
3. Qdrant retorna os melhores resultados acima do limiar configurado.
4. Resultados incompatíveis com fabricante/modelo explícito sao recusados.
5. Chunks adjacentes da mesma pagina ampliam o contexto local.
6. Prompt instrui a Maritaca a usar somente o contexto e preservar seguranca.
7. Resposta normaliza citacoes para `[Fonte N]`.
8. API retorna resposta, fontes, paginas, metadados, uso e `request_id`.

## Contratos e operacao

Endpoints principais:

- `GET /api/health`
- `POST /api/documents/ingest`
- `GET /api/documents/`
- `PATCH /api/documents/{id}`
- `POST /api/documents/{id}/reprocess`
- `GET /api/rag/jobs/{id}`
- `POST /api/rag/search`
- `POST /api/rag/query`
- `GET /api/rag/kpis/overview`

Comandos essenciais:

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f api worker frontend
docker compose exec api python manage.py check
docker compose exec api python manage.py reconcile_qdrant
```

Dashboard: `http://127.0.0.1:3000`

API: `http://127.0.0.1:8000`

## Evidencias de qualidade

Auditoria final:

- Backend: `85` testes aprovados.
- Django: `check` aprovado e nenhuma migration pendente.
- Frontend: lint e build de producao aprovados.
- Frontend E2E: `8` cenarios desktop/mobile aprovados. A execucao concorrente
  com build e containers mostrou sensibilidade ao limite de `30s` em maquina
  sob pressao, portanto CI deve reservar recursos ou adotar timeout calibrado.
- Docker Compose local e override de producao: configuracoes validas.
- Base controlada: `2` documentos, `803` chunks e `803` vetores.
- Reconciliacao: `0` vetores orfaos e `0` chunks ausentes.
- Quality gate RAG com geracao: aprovado nos `4` casos tecnicos.

Metricas da base controlada:

| Metrica | Resultado |
| --- | ---: |
| Retrieval Hit Rate | `1.000` |
| Mean Reciprocal Rank | `1.000` |
| Precision@k | `1.000` |
| Duplicate Result Rate | `0.000` |
| Citation Rate | `1.000` |
| Answer Term Recall | `0.889` |
| Refusal Accuracy | `1.000` |
| Generation Errors | `0` |

O dataset e pequeno e representa somente o acervo controlado. Qualquer troca de
modelo, chunking, limiar ou conjunto documental exige nova avaliacao.

## Analise tecnica

### Pontos fortes

- Modulos e providers desacoplados, com baixo acoplamento ao LangChain.
- Pipeline assincrono idempotente e com estados operacionais claros.
- PostgreSQL como fonte de verdade e reconciliacao segura do Qdrant.
- Recusa por baixa relevancia e incompatibilidade de equipamento.
- Fontes rastreaveis com pagina, chunk, rank, score e metadados.
- Auditoria minimizada, throttling, papeis simples e perfil de producao.
- Dashboard funcional para consulta, operacao e indicadores.

### Lacunas prioritarias

1. Nao existe endpoint de exclusao completa de documento.
   A exclusao deve remover arquivo, chunks, jobs relacionados e vetores Qdrant
   de forma idempotente e auditavel.

2. Extracao PDF carrega o arquivo inteiro em memoria.
   Arquivos maiores que o manual validado podem pressionar o worker. Tambem e
   recomendavel fechar explicitamente o recurso PyMuPDF.

3. O servico interno de embeddings confia no isolamento da rede Compose.
   Em outro orquestrador, deve receber autenticacao de servico ou network
   policy equivalente.

4. Filtros Qdrant ainda nao possuem indices de payload dedicados.
   A ausencia nao bloqueia o acervo atual, mas afeta escala.

5. Dependencias de desenvolvimento e runtime compartilham `requirements.txt`.
   Separar dependencias reduz tamanho e superficie da imagem de producao.

6. O manual de exemplo possui aproximadamente `49 MB` no Git.
   Novos manuais grandes devem usar Git LFS ou armazenamento externo.

## Seguranca e privacidade

Protecoes existentes:

- Token DRF, papeis `staff`, chave de API opcional e throttling.
- Texto integral das perguntas desativado na auditoria por padrao.
- Logs estruturados sem conteúdo da pergunta.
- Containers da aplicacao executados sem privilegios.
- Perfil de producao sem portas publicas de PostgreSQL e Qdrant.
- Configuracoes de HTTPS, cookies seguros e retencao.

Limites:

- Token do dashboard fica em `localStorage`.
- Nao ha SSO, MFA, tenants, antivirus, DLP ou WAF.
- O override Compose nao provisiona TLS nem gerencia segredos.

## Checklist para promocao a producao

### Obrigatorio

- [ ] Provisionar proxy reverso ou load balancer com TLS.
- [ ] Usar secrets manager e credenciais exclusivas.
- [ ] Validar backup e restauracao de PostgreSQL, Qdrant e documentos.
- [ ] Ativar autenticacao obrigatoria e revisar usuarios `staff`.
- [ ] Configurar monitoramento, alertas e politica de logs.
- [ ] Fixar versoes/imagens e recertificar embeddings antes de atualizacoes.
- [ ] Executar teste de carga com o acervo e concorrencia esperados.
- [ ] Implementar exclusao consistente de documentos.

### Recomendado

- [ ] Criar indices de payload no Qdrant.
- [ ] Processar PDFs de forma incremental e fechar recursos explicitamente.
- [ ] Separar dependencias de runtime e desenvolvimento.
- [ ] Ampliar o dataset de avaliacao com mais fabricantes e alertas criticos.
- [ ] Definir Git LFS ou object storage para novos manuais.

## Limitacoes funcionais

- PDFs apenas em imagem nao possuem OCR.
- Diagramas e tabelas complexas nao sao interpretados estruturalmente.
- A inferencia de metadados e heuristica e pode exigir revisao humana.
- Procedimentos extensos entre varias paginas podem perder continuidade.
- Respostas nao substituem o manual oficial nem um tecnico qualificado.

## Indice documental

- `README.md`: uso geral, API, setup e comandos.
- `docs/ADR.md`: decisoes arquiteturais e trade-offs.
- `docs/CONTAINERS.md`: containers, redes, volumes e operacao.
- `docs/SECURITY.md`: modelo de seguranca e checklist de deploy.
- `docs/TECHNICAL_MANUALS.md`: regras e limitacoes do dominio.
- `docs/DEMO_GUIDE.md`: roteiro de demonstracao.
- `docs/SPRINT_PLAN.md`: historico e estado das sprints.
- `docs/FINAL_REVIEW_AND_ROADMAP.md`: roadmap e Definition of Done.
- `docs/SPRINT_16.md`: certificacao da base controlada.

## Conclusao

O MVP cumpre o objetivo de oferecer consulta RAG rastreavel sobre manuais
tecnicos e possui base arquitetural adequada para evolucao. A entrega esta
aprovada para demonstracao e piloto controlado. A promocao para producao deve
ser condicionada aos itens obrigatorios de infraestrutura, ciclo de vida,
seguranca e validacao em escala.
