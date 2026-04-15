# 🎬 Produção Artística — Pipeline NoSQL

> **Trabalho N1** — Banco de Dados DB_Producao_Artistica - NoSQL · FATESG  
> Pipeline completo de **ingestão**, **limpeza** e **armazenamento** de dados massivos de produção artística usando **MongoDB + Docker**.

---

## 📋 Visão Geral

Este projeto implementa um pipeline ETL para processar dados de produção artística (filmes, peças, obras) a partir de arquivos JSONL massivos (**~16 milhões de registros, ~900 MB**) e importá-los para o MongoDB rodando em **Docker**.

```
data/*.jsonl  →  clean_data.py  →  data/clean/*_clean.jsonl  →  import_json.py  →  MongoDB (Docker)
   (raw)          (limpeza)             (tratado)                  (ingestão)         (container)
```

---

## 📁 Estrutura do Projeto

```
TraballhoN1/
├── docker/
│   └── docker-compose.yml       # MongoDB 7 containerizado
├── src/
│   ├── clean_data.py            # Script de limpeza e tratamento
│   └── import_json.py           # Script de importação para MongoDB
├── data/                        # Dados brutos (JSONL) — gitignored
│   ├── pessoa.jsonl             #   ~90 MB  ·  2.062.495 registros
│   ├── producao.jsonl           #   ~75 MB  ·  1.023.616 registros
│   ├── equipe.jsonl             #  ~737 MB  · 12.849.041 registros
│   └── clean/                   # Dados limpos (gerados pelo clean_data.py)
│       ├── pessoa_clean.jsonl
│       ├── producao_clean.jsonl
│       └── equipe_clean.jsonl
├── docs/                        # Documentação adicional
├── requirements.txt             # Dependências Python
├── .gitignore
└── README.md
```

---

## 🗄️ Modelo de Dados

O dataset é composto por **3 coleções** interrelacionadas:

```mermaid
erDiagram
    PESSOA {
        int id_pessoa PK
        string nome
    }
    PRODUCAO {
        int id_producao PK
        string titulo
        int ano
        int tipo_id
    }
    EQUIPE {
        int id_producao FK
        int id_pessoa FK
        string papel
    }

    PESSOA ||--o{ EQUIPE : "participa"
    PRODUCAO ||--o{ EQUIPE : "envolve"
```

| Coleção      | Descrição                                    | Campos                               |
|-------------|----------------------------------------------|--------------------------------------|
| **pessoa**   | Pessoas envolvidas em produções artísticas   | `id_pessoa`, `nome`                  |
| **producao** | Produções artísticas (filmes, peças, etc.)   | `id_producao`, `titulo`, `ano`, `tipo_id` |
| **equipe**   | Relação pessoa ↔ produção (papel exercido)   | `id_producao`, `id_pessoa`, `papel`  |

---

## 🚀 Quick Start

### Pré-requisitos

- **Python 3.8+**
- **Docker** e **Docker Compose**

### 1. Configurar o Ambiente Virtual

```bash
# Criar venv
python -m venv .venv

# Ativar (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Ativar (Linux/macOS)
source .venv/bin/activate

# Instalar dependências
pip install -r requirements.txt
```

### 2. Subir o MongoDB via Docker

```bash
# Iniciar o container
docker compose -f docker/docker-compose.yml up -d

# Verificar se está rodando
docker ps
```

O MongoDB estará disponível em `localhost:27017`.

### 3. Executar o Pipeline

```bash
# Passo 1: Limpar e tratar os dados brutos
python src/clean_data.py

# Passo 2: Importar dados limpos para o MongoDB
python src/import_json.py
```

### 4. Parar o MongoDB

```bash
# Parar o container (dados persistem no volume)
docker compose -f docker/docker-compose.yml down

# Parar e REMOVER os dados do volume
docker compose -f docker/docker-compose.yml down -v
```

---

## 🐳 Docker

O MongoDB roda em container Docker via `docker-compose.yml`:

| Configuração    | Valor                              |
|----------------|-------------------------------------|
| **Imagem**      | `mongo:7`                          |
| **Container**   | `mongodb_producao_artistica`       |
| **Porta**       | `27017:27017`                      |
| **Volume**      | `mongo_data` (dados persistentes)  |
| **Database**    | `DB_Producao_Artistica`            |

### Variáveis de Ambiente (opcionais)

Os scripts aceitam variáveis de ambiente para configuração:

```bash
# Conectar a um MongoDB customizado
MONGO_URI="mongodb://user:pass@host:27017/" python src/import_json.py

# Alterar o nome do banco
DB_NAME="outro_banco" python src/import_json.py
```

---

## 🧹 Limpeza de Dados — `src/clean_data.py`

Script de tratamento e limpeza otimizado para dados massivos. Processa **~16M de registros** em **~3 minutos**.

### Tratamentos Aplicados

| Tratamento                     | pessoa | producao | equipe |
|-------------------------------|--------|----------|--------|
| Remoção de duplicatas          | ✅ `id_pessoa` | ✅ `id_producao` | ✅ `(id_producao, id_pessoa, papel)` |
| Normalização de strings        | ✅ `nome` | ✅ `titulo` | ✅ `papel` |
| Correção de valores inválidos  | —      | ✅ `ano` (`#2004` → `2004`) | — |
| Conversão de tipos             | —      | ✅ `ano` → `int`, `tipo_id` → `int` | — |
| Validação de range             | —      | ✅ `ano` entre 1800–2100 | — |
| Integridade referencial        | —      | —        | ✅ Remove órfãos |
| Remoção de campos `null`       | ✅     | ✅       | ✅     |

### Resultados

| Arquivo         | Registros Brutos | Registros Limpos | Removidos  |
|----------------|-----------------|-----------------|------------|
| `pessoa.jsonl`  | 2.062.495       | 2.062.491       | 4 duplicatas |
| `producao.jsonl`| 1.023.616       | 1.023.616       | 2 correções de ano |
| `equipe.jsonl`  | **12.849.041**  | **7.549.637**   | **~5.3M** (duplicatas + órfãos) |

> **Redução total:** 41.2% das linhas de equipe removidas, economia de ~286 MB em disco.

### Otimizações de Performance

- **Streaming line-by-line** — nunca carrega o arquivo inteiro em memória
- **Deduplicação via `set`** — lookup O(1) por registro
- **Buffer de escrita** — flush a cada 10.000 linhas (reduz chamadas de I/O)
- **Ordem de processamento estratégica** — `pessoa` → `producao` → `equipe` (gera sets de IDs válidos antes de validar referências)
- **Paths relativos via `__file__`** — executa de qualquer diretório

---

## 📥 Importação — `src/import_json.py`

Importa os arquivos JSONL limpos (`data/clean/`) para as coleções MongoDB. Otimizado para velocidade máxima.

### Estratégia

- **`insert_many`** com `ordered=False` — sem overhead de upsert (dados já deduplicados)
- **Batch de 5.000 docs** — reduz round-trips ao MongoDB
- **Drop + reimport** — garante coleção limpa, sem dados stale
- **Sem campo `id` artificial** — usa apenas o `_id` nativo do MongoDB
- **Criação automática de índices** nos campos-chave após importação

### Configuração

| Parâmetro    | Valor Padrão                   | Descrição                          |
|-------------|-------------------------------|--------------------------------------|
| `MONGO_URI`  | `mongodb://localhost:27017/`  | URI de conexão (env var)           |
| `DB_NAME`    | `DB_Producao_Artistica`       | Nome do banco (env var)            |
| `BATCH_SIZE` | `5000`                        | Documentos por lote de inserção    |

### Coleções Geradas

| Arquivo de Entrada          | Coleção MongoDB    | Índice Criado                   |
|----------------------------|--------------------|---------------------------------|
| `pessoa_clean.jsonl`        | `pessoa_clean`     | `id_pessoa` (unique)            |
| `producao_clean.jsonl`      | `producao_clean`   | `id_producao` (unique)          |
| `equipe_clean.jsonl`        | `equipe_clean`     | `(id_producao, id_pessoa)` composto |


---

## 📊 Estatísticas do Dataset

| Métrica                  | Valor          |
|-------------------------|----------------|
| Total de registros brutos | ~15.935.152   |
| Total após limpeza       | ~10.635.744   |
| Pessoas únicas           | 2.062.491     |
| Produções únicas         | 1.023.616     |
| Relações pessoa-produção | 7.549.637     |
| Tamanho bruto total      | ~902 MB       |
| Tamanho limpo total      | ~628 MB       |

---

## 🛠️ Tecnologias

| Tecnologia       | Uso                                  |
|-----------------|--------------------------------------|
| Python 3.8+      | Linguagem principal                  |
| MongoDB 7        | Banco de dados NoSQL                 |
| Docker Compose   | Containerização do MongoDB           |
| PyMongo          | Driver MongoDB para Python           |
| JSON Lines       | Formato dos dados de entrada (.jsonl)|
| venv             | Isolamento de dependências Python    |

---

## 👤 Autor

**Gustavo Sousa** — FATESG · Inteligência Artificial
