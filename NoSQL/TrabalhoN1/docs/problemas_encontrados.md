# ⚠️ Problemas Encontrados e Desafios

O maior desafio enfrentado durante o projeto foi o **volume massivo** e a **sujeira contínua** dos dados brutos armazenados no dataset. Trabalhar com um escopo de quase **16 milhões de registros recheados de anomalias ou duplicações** exigiu cuidado arquitetural. 

Logo de cara, o arquivo de `equipe.jsonl` (~737 MB) revelou sua complexidade empírica: **mais de 40% das linhas continham incoerências ou repetições literais**.

Abaixo detalhamos os gargalos mitigados, categorizados pelo seu impacto na solução:

### 1. Inconsistência Crítica de Tipos 🧩
> [!WARNING]
> Muitas colunas numéricas vinham mascaradas na forma de Strings combinadas a ruídos ou caracteres que não faziam parte das chaves numéricas.

Encontramos frequentemente o campo de `ano` (na coleção de Produção) cadastrado contendo caracteres especiais acoplados, como `#2024`, `ano: 1999` ou mesmo letras avulsas. Injetar isso de forma nativa para o MongoDB quebraria o suporte de agregações analíticas numéricas (soma, média ou filtros temporais) de imediato nas etapas estáticas. Para lidar, introduzimos Expressões Regulares (`RegEx`) estritas, extraindo silenciosamente as ocorrências lícitas e as convertendo formalmente de volta ao tipo primitivo inteiro (`Int`).

### 2. Acoplamento de Dados Órfãos 🔗
> [!IMPORTANT]
> Manter chaves estrangeiras (`id_pessoa` ou `id_producao`) flutuantes na estrutura do JSON geraria instabilidades e anomalias drásticas na extração analítica.

Observou-se uma teia na coleção de `equipe` apontando para IDs de Pessoas que nunca nasceram e para IDs de Produções que não existiam em nenhum dos artefatos originais. Se simplesmente ordenássemos o script de Ingestão para engolir tudo organicamente, a Collection do MongoDB ficaria com as proporções inchadas e abarrotada de metadados ilusórios. Implementamos verificações de **sets in-memory** cruzados (no ecossistema padrão em Python) e aplicamos as restrições de validação analítica com  **leftsemi joins**  (no escopo escalável usando DataFrame PySpark) para refutar a carga caso os identificadores âncora não existissem antecipadamente.

### 3. Gargalo Primário de Memória RAM 💻
> [!CAUTION]
> Tentar instanciar estruturas colossais JSON no escopo funcional da sua máquina gera picos de latência (Overheads) seguidos de falta severa de memória RAM.

A versão precoce do laboratório tentou varrer os logs empurrando bibliotecas JSON clássicas (como `.read()` global para memória no Python) estourando o ambiente ou provocando o desabamento em disco através do pagefile nativo. Precisamos reescrever a filosofia para transações via **Streaming Lazy Iterator**, engajando loops assíncronos restritos para a leitura (`for line in pipeline`). Os processos limitam sua fome alocativa carregando parcelas milimétricas, aliás, garantindo com maestria estabilidade incondicional durante o Apache Spark via otimizador nativo Tungsten.
