# Decisoes Arquiteturais

## ADR: Django + Django REST Framework como backend

### Status

Aceito na Sprint 1.

### Contexto

O projeto precisa expor endpoints para ingestao, consulta RAG, health check e
futuramente dashboard. Tambem precisara de PostgreSQL, migrations, usuarios,
historico, KPIs e possivelmente painel administrativo.

### Decisao

Adotar Django 5.2 LTS com Django REST Framework como backend. FastAPI permanece
como alternativa registrada, mas nao sera implementado em paralelo.

### Alternativas consideradas

- FastAPI: inicializacao simples, tipagem forte e excelente experiencia para
  APIs, mas exige montar separadamente ORM, migrations, autenticacao e admin.
- Flask: simples, mas demanda ainda mais decisoes de infraestrutura.
- Node.js/Express: viavel, mas a stack de IA em Python e mais conveniente.

### Consequencias

Django melhora produtividade nas partes relacionais, administrativas e de
autenticacao. Django REST Framework fornece APIs estruturadas e validacao.
Em contrapartida, a fundacao inicial e maior e o uso assíncrono exige mais
cuidado. A troca e recomendada, mas nao obrigatoria.

## ADR: Qdrant como banco vetorial

### Status

Aceito e implementado na Sprint 4.

### Contexto

O sistema precisa armazenar embeddings e recuperar chunks por similaridade, com
metadados e filtros futuros.

### Decisao

Usar Qdrant como vector store.

### Alternativas consideradas

- pgvector no PostgreSQL: reduziria componentes, mas pode limitar recursos
  dedicados de busca vetorial dependendo do caso.
- Chroma: simples para prototipos, mas menos indicado como alvo de producao.
- Weaviate/Milvus: poderosos, porem podem adicionar complexidade ao MVP.

### Consequencias

Qdrant oferece boa separacao de responsabilidades e filtros por payload. A
desvantagem e adicionar mais um servico para configurar e monitorar.

Os testes usam o modo em memoria do cliente Qdrant. O ambiente local e producao
usam o servidor configurado por `QDRANT_URL`.

## ADR: Embeddings locais multilíngues com FastEmbed

### Status

Aceito na Sprint 4.

### Contexto

Os documentos e perguntas serao principalmente em portugues. O MVP precisa de
embeddings com custo controlado, execucao local e integracao simples com Qdrant.

### Decisao

Usar FastEmbed com
`sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`, que gera vetores
de 384 dimensoes. O provider fica atras da interface `EmbeddingProvider`.

### Alternativas consideradas

- `intfloat/multilingual-e5-large`: potencial de maior qualidade, mas usa 1024
  dimensoes e exige mais memoria e processamento.
- API externa de embeddings: reduz carga local, mas adiciona custo, latencia e
  dependencia de rede.
- Sentence Transformers com PyTorch: flexivel, mas traz uma dependencia maior
  para o MVP.

### Consequencias

O modelo escolhido e multilíngue, compacto e executado localmente. O primeiro
uso exige download. A versao do FastEmbed deve permanecer fixada, pois mudancas
de pooling podem alterar os vetores e exigir reindexacao completa.

## ADR: PostgreSQL para metadados e auditoria

### Status

Aceito e implementado na Sprint 2.

### Contexto

O RAG precisa registrar documentos, status, chunks, historico de consultas e
metadados estruturados que nao pertencem exclusivamente ao banco vetorial.

### Decisao

Usar PostgreSQL como banco estruturado principal.

### Alternativas consideradas

- Apenas Qdrant: simplificaria a stack, mas dificultaria auditoria e consultas
  relacionais.
- MongoDB: flexivel, mas a stack proposta ja inclui PostgreSQL e o historico do
  RAG se encaixa bem em modelo relacional.

### Consequencias

PostgreSQL aumenta rastreabilidade e facilita dashboards/KPIs. A desvantagem e
exigir modelagem e migracoes.

No MVP, o texto dos chunks tambem sera persistido no PostgreSQL para auditoria e
reprocessamento. Isso duplica parte dos dados que futuramente estarao no Qdrant,
mas preserva uma fonte estruturada independente do banco vetorial.

## ADR: RabbitMQ adiado para depois do MVP funcional

### Contexto

RabbitMQ e util para indexacao assíncrona, mas pode aumentar complexidade antes
de o fluxo RAG basico estar validado.

### Decisao

Prever RabbitMQ na arquitetura, mas iniciar com ingestao sincrona. Introduzir
fila quando a indexacao basica ja estiver funcionando.

### Alternativas consideradas

- Usar RabbitMQ desde a Sprint 1: melhora arquitetura operacional, mas aumenta
  tempo ate o primeiro MVP.
- Nunca usar fila: mais simples, mas pior para documentos grandes e retentativas.

### Consequencias

A decisao acelera o MVP e reduz risco inicial. O impacto negativo e que a sprint
de RabbitMQ exigira adaptar endpoints e status de indexacao.

## ADR: Interfaces separadas para LLM e embeddings

### Contexto

O provider de LLM esta planejado como Maritaca, mas o modelo de embedding ainda
nao esta definido e pode mudar por custo, qualidade ou disponibilidade.

### Decisao

Criar abstracoes separadas para `LLMProvider` e `EmbeddingProvider`.

### Alternativas consideradas

- Acoplar tudo diretamente a Maritaca: simples inicialmente, mas dificulta
  testes e troca de modelo.
- Usar framework RAG completo desde o inicio: acelera algumas partes, mas pode
  esconder decisoes importantes para fins academicos.

### Consequencias

A arquitetura fica mais testavel e flexivel. A desvantagem e um pouco mais de
codigo de infraestrutura no MVP.

## ADR: Maritaca pela Responses API

### Status

Aceito e implementado na Sprint 5.

### Contexto

O sistema precisa gerar respostas em portugues a partir do contexto recuperado,
com timeout, retentativas, limites e possibilidade de trocar o provider.

### Decisao

Usar o modelo `sabia-4` da Maritaca pela Responses API compativel com o SDK
OpenAI. A implementacao fica atras de `LLMProvider`, sem expor tipos do SDK para
o servico RAG.

### Alternativas consideradas

- API de Chat Completions: suportada, mas a Maritaca recomenda Responses API
  para projetos novos.
- Chamada HTTP manual: reduz dependencia, mas exige implementar contratos,
  erros, timeout e retries.
- Outro provider de LLM: permanece possivel pela interface criada.

### Consequencias

A integracao usa uma interface atual e possui timeout, retries, temperatura e
limite de tokens configuraveis. A desvantagem e depender do SDK OpenAI e de uma
chave Maritaca. A integracao real precisa ser validada quando a chave estiver
disponivel.

A integracao real com `sabia-4` foi validada na Sprint 6 durante a baseline de
geracao.

## ADR: LangChain encapsulado para orquestracao RAG

### Status

Parcialmente aceito na Sprint 3. O pacote independente
`langchain-text-splitters` foi adotado atras de um adaptador proprio. Os demais
componentes LangChain continuam em avaliacao para as Sprints 4 e 5.

### Contexto

O sistema precisa integrar loaders, splitters, embeddings, Qdrant, retrievers,
prompts e Maritaca. LangChain pode acelerar essas integracoes, mas tambem
adiciona abstracoes, dependencias e APIs sujeitas a mudancas.

### Decisao

Usar LangChain nos componentes de integracao RAG, sempre encapsulado por servicos
e interfaces proprias. Models Django, regras de negocio, serializers e contratos
HTTP nao devem importar LangChain diretamente.

### Alternativas consideradas

- Servicos totalmente proprios: maior controle e estabilidade, mas exige mais
  codigo de integracao.
- LlamaIndex: forte para indexacao e RAG, mas pode esconder detalhes do fluxo.

### Consequencias

LangChain reduz codigo de integracao e facilita experimentos com modelos,
retrievers e vector stores. O encapsulamento reduz acoplamento, mas exige
adaptadores e testes de contrato. A adocao e recomendada, mas deve ser revista
se aumentar a complexidade sem ganho mensuravel.

Na Sprint 3, instalar apenas o pacote de splitters mostrou-se suficiente para o
chunking. Models, serializers, views e servicos nao dependem de tipos LangChain.

Na Sprint 5, prompting e geracao permaneceram em servicos proprios. Adicionar
uma chain LangChain aqui nao traria ganho claro para o fluxo linear atual.

## ADR: Avaliacao RAG com dataset versionado e quality gate

### Status

Aceito e implementado na Sprint 6.

### Contexto

Inspecao manual isolada nao permite detectar regressao de retrieval, prompt ou
geracao. Avaliacoes exclusivamente baseadas em LLM-as-judge podem adicionar
custo, variabilidade e dependencia de outro modelo.

### Decisao

Manter um dataset JSON versionado com perguntas, fontes esperadas e termos
esperados. Calcular metricas deterministicas de retrieval e geracao e aplicar um
quality gate inicial.

### Alternativas consideradas

- Apenas testes manuais: simples, mas nao reproduziveis.
- LLM-as-judge: flexivel, mas mais caro e menos deterministico.
- Framework externo de avaliacao: pode oferecer mais metricas, mas adiciona
  complexidade antes de existir um dataset representativo.

### Consequencias

A avaliacao pode rodar localmente e detectar regressao cedo. As metricas de
termos e citacoes sao aproximacoes e nao provam fidelidade semantica completa.
O dataset precisa crescer junto com os documentos reais.

## ADR: Celery com RabbitMQ e estado de jobs no PostgreSQL

### Status

Aceito e implementado na Sprint 7.

### Contexto

A geracao de embeddings e a indexacao no Qdrant podem demorar, falhar
temporariamente ou bloquear requisicoes HTTP. Tambem e necessario consultar o
estado da operacao sem depender da disponibilidade do broker.

### Decisao

Usar Celery 5.6 com RabbitMQ como broker para executar a indexacao vetorial.
Persistir o ciclo de vida de cada `IndexingJob` no PostgreSQL e aplicar ate tres
retentativas com backoff exponencial. Manter a indexacao sincrona como endpoint
de diagnostico.

### Alternativas consideradas

- Django-Q ou Huey: integracao simples, mas menor aderencia a stack RabbitMQ
  inicialmente planejada.
- RabbitMQ consumido diretamente: maior controle, mas exige implementar
  protocolo de tarefas, retentativas e ciclo do worker.
- Resultado apenas no backend do Celery: reduz modelagem, mas acopla a API a
  detalhes operacionais e oferece historico de negocio mais fraco.

### Consequencias

Requisicoes de indexacao retornam rapidamente e falhas temporarias podem ser
recuperadas. O PostgreSQL fornece historico consultavel mesmo sem backend de
resultados Celery. Em contrapartida, o ambiente passa a exigir RabbitMQ e ao
menos um worker ativo. Jobs interrompidos durante `processing` ainda exigirao
uma politica futura de deteccao e reenvio.

## ADR: Dashboard Next.js com proxy para a API Django

### Status

Aceito e implementado na Sprint 8.

### Contexto

O dashboard precisa consumir upload, jobs e consultas RAG no navegador. Acesso
direto ao Django exigiria configuracao CORS e exporia dois destinos ao cliente.

### Decisao

Usar Next.js 16 com React 19 e Tailwind CSS 4. Requisicoes usam o prefixo
`/backend-api`, encaminhado por uma rota proxy do Next.js para a API definida em
`API_BASE_URL`. A rota explicita preserva uploads e consultas RAG longas.

### Alternativas consideradas

- CORS direto entre Next.js e Django: simples, mas adiciona configuracao antes
  da sprint de seguranca.
- Templates Django: reduzem componentes, mas limitam a evolucao interativa.
- Next.js como BFF completo: flexivel, mas duplicaria contratos no MVP.

### Consequencias

O navegador acessa uma origem unica e o destino da API pode mudar por ambiente.
O dashboard passa a depender do servidor Next.js para encaminhar chamadas.

## ADR: Compose full-stack com redes segmentadas

### Status

Aceito e implementado no adendo de infraestrutura da Sprint 8.

### Contexto

O ambiente local exigia processos iniciados manualmente e expunha servicos de
dados que so precisam ser acessados pela aplicacao. API e worker tambem possuem
as mesmas dependencias Python e nao justificam imagens diferentes.

### Decisao

Executar frontend, API, worker, servico de embeddings, PostgreSQL, Qdrant e
RabbitMQ pelo Docker Compose. API, worker e embeddings compartilham
`adaptive-rag-backend:local`; o frontend usa build standalone. Somente portas
`3000` e `8000` sao publicadas. Bancos, broker e embeddings ficam na rede
interna `backend`, enquanto `app` conecta os componentes da aplicacao e permite
chamadas externas. O servico de embeddings mantem uma unica copia ONNX para
evitar disputa de memoria entre API e worker.

### Alternativas consideradas

- Expor todos os servicos no host: facilita diagnostico manual, mas amplia a
  superficie local e enfraquece o isolamento.
- Uma imagem por processo backend: permite customizacao isolada, mas duplica
  build e armazenamento sem necessidade atual.
- Kubernetes local: oferece mais recursos operacionais, mas seria
  desproporcional ao MVP.

### Consequencias

O ambiente passa a subir com um comando, possui health checks, persistencia e
isolamento coerentes. API, worker e frontend executam sem privilegios. Para
acessar diretamente PostgreSQL, Qdrant ou RabbitMQ a partir do host, sera
necessario um override Compose temporario.

## ADR: Observabilidade persistida no PostgreSQL e logs JSON

### Status

Aceito e implementado na Sprint 9.

### Contexto

O MVP precisava medir uso, latencia, erros e documentos recuperados sem
introduzir uma nova plataforma operacional. Tambem era necessario correlacionar
uma consulta com os logs HTTP sem registrar seu conteudo no stdout.

### Decisao

Persistir consultas em `RAGQueryRecord` e fontes em `RAGQuerySource`. Agregar
KPIs sob demanda no endpoint `GET /api/rag/kpis/overview`. Emitir logs JSON para
stdout com `request_id`, evento, status e duracao. O texto da pergunta nao e
incluido nos logs, mas permanece no PostgreSQL para historico e auditoria.

### Alternativas consideradas

- Prometheus, Grafana e Loki: oferecem observabilidade mais completa, mas
  aumentariam muito o escopo operacional do MVP.
- Armazenar apenas logs: reduz modelagem, mas dificulta KPIs e consultas de
  negocio confiaveis.
- Salvar fontes como JSON na query: simples, mas pior para agregar documentos
  mais recuperados.

### Consequencias

O dashboard obtém indicadores reproduziveis sem infraestrutura adicional e os
eventos podem ser coletados futuramente por qualquer stack de logs. As
agregacoes sao calculadas sob demanda e podem exigir cache com maior volume.
Antes de dados sensiveis, sera necessario definir retencao, mascaramento e
controle de acesso ao historico.

## ADR: Hardening incremental com chave de API opcional

### Status

Aceito e implementado na Sprint 10.

### Contexto

O dashboard e as APIs do MVP precisam funcionar localmente sem provisionamento
de usuarios, mas endpoints de ingestao, consulta e KPIs nao devem permanecer
abertos em ambientes compartilhados. Tambem e necessario evitar vazamento de
erros de providers e perguntas no endpoint operacional.

### Decisao

Adicionar uma chave compartilhada opcional via `API_ACCESS_KEY` nas APIs
publicas. O proxy Next.js injeta a chave server-side. Aplicar throttling,
headers HTTP seguros, IDs de requisicao validados, respostas de erro controladas
e mascaramento de perguntas nos KPIs por padrao. Manter health check publico e
o servico de embeddings isolado pela rede interna.

### Alternativas consideradas

- JWT com usuarios Django: oferece identidade e permissoes individuais, mas
  exige fluxo de login e gestao de usuarios fora do escopo do MVP.
- Apenas isolamento de rede: reduz configuracao, mas nao protege a API publicada.
- Chave obrigatoria em todo ambiente: mais restritiva, mas prejudica setup local
  e testes sem trazer identidade por usuario.

### Consequencias

O MVP ganha uma barreira simples para ambientes compartilhados e reduz
exposicao de detalhes internos sem quebrar o desenvolvimento local. A chave
compartilhada nao fornece identidade, papeis, revogacao individual ou auditoria
por usuario. Um deploy real ainda exige TLS, segredo forte, autenticacao por
usuario, rotacao de credenciais e politica de retencao.

## ADR: Token DRF e papeis simples para acesso individual

### Status

Aceito e implementado na Sprint 11.

### Contexto

A chave compartilhada protege integracoes, mas nao identifica pessoas nem
separa consultas de operacoes administrativas. O dashboard precisa suportar
login sem exigir um provedor externo no MVP.

### Decisao

Usar `TokenAuthentication` do Django REST Framework, ativado por
`API_REQUIRE_AUTHENTICATION`. Usuarios autenticados podem consultar e listar;
usuarios `is_staff` podem ingerir, indexar e visualizar KPIs. Manter a chave de
API como credencial administrativa de servico. Quando login individual esta
ativo, o proxy Next.js nao injeta essa chave.

### Alternativas consideradas

- JWT: adequado a sistemas distribuidos, mas adiciona refresh, expiracao e
  rotacao antes de serem necessarios.
- Sessao Django com CSRF: madura, mas exige compartilhar cookies e CSRF entre
  o proxy Next.js e Django.
- Provedor OIDC externo: melhor para producao corporativa, mas amplia
  infraestrutura e configuracao do MVP.

### Consequencias

O sistema passa a identificar usuarios e revogar tokens individualmente, com
uma matriz de acesso simples. Tokens DRF nao expiram automaticamente e o
dashboard os mantem em `localStorage`; producao deve considerar OIDC ou tokens
curtos em cookies seguros. Ainda nao existe auditoria de consultas por usuario.

## ADR: Metadados e filtros de domínio para manuais técnicos

### Status

Aceito e implementado na Sprint 12.

### Contexto

Manuais de impressoras e scanners podem cobrir dezenas de modelos, codigos de
erro, procedimentos perigosos, tabelas e centenas de paginas. Retrieval apenas
semantico pode misturar modelos ou omitir alertas relevantes. O manual de
exemplo possui 513 paginas, 50 MB, criptografia AES permitida para copia e 25
modelos Brother.

### Decisao

Usar PyMuPDF como extrator PDF primario e `pypdf` como fallback. Inferir e
preservar metadados tecnicos no documento e nos chunks. Classificar chunks como
seguranca, troubleshooting, erro, procedimento, especificacao, manutencao ou
referencia. Permitir filtros Qdrant por fabricante, modelo, equipamento, manual
e conteúdo. Orientar o prompt a preservar codigos/modelos e priorizar seguranca.

### Alternativas consideradas

- Tratar manuais como PDFs genericos: simples, mas aumenta risco de contexto
  incorreto e perde estrutura tecnica.
- Um parser especifico para Brother: mais preciso no exemplo, mas impediria a
  expansao para outras marcas.
- OCR e extracao multimodal completa: maior cobertura de diagramas, mas exige
  modelos, custo e pipeline adicionais.

### Consequências

Consultas podem ser restringidas ao equipamento correto e respostas recebem
contexto técnico mais rico. Heuristicas continuam aproximadas e devem evoluir
com novos fabricantes. Diagramas, tabelas complexas e PDFs apenas em imagem
ainda precisam de pipeline multimodal/OCR.
