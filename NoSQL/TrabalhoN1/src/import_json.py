"""
╔══════════════════════════════════════════════════════════════╗
║  IMPORTAÇÃO DE DADOS JSONL → MongoDB
║  Lê arquivos .jsonl de data/clean/ e importa para o MongoDB
║  Usa Docker container via localhost:27017
║
║  Coleções criadas: pessoa_clean, producao_clean, equipe_clean
║  Os dados já vêm deduplicados pelo clean_data.py, portanto
║  usamos insert_many puro (sem upsert) para máxima velocidade.
╚══════════════════════════════════════════════════════════════╝

Executar a partir da raiz do projeto:
    python src/import_json.py
"""

import emoji
import os
import json
import time
from pathlib import Path
from pymongo import MongoClient, ASCENDING

# ─────────────────────────── CONFIG ───────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MONGO_URI   = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME     = os.environ.get("DB_NAME", "DB_Producao_Artistica")
PASTA_DADOS = PROJECT_ROOT / "data" / "clean"
BATCH_SIZE  = 5_000          # docs por insert_many (maior = mais rápido)
LOG_INTERVAL = 100_000       # progresso a cada N docs

# Mapeamento: arquivo → (nome da coleção, índices a criar)
COLECOES = {
    "pessoa_clean.jsonl": (
        "pessoa_clean",
        [("id_pessoa", ASCENDING)],
    ),
    "producao_clean.jsonl": (
        "producao_clean",
        [("id_producao", ASCENDING)],
    ),
    "equipe_clean.jsonl": (
        "equipe_clean",
        [("id_producao", ASCENDING), ("id_pessoa", ASCENDING)],
    ),
}

# ─────────────────────────── CONEXÃO ─────────────────────────

def connect():
    """Conecta ao MongoDB e retorna o database."""
    print(emoji.emojize(f":globe_with_meridians: Conectando a {MONGO_URI}"))
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print(emoji.emojize(":check_mark_button: Conectado ao MongoDB!"))
    return client[DB_NAME]

# ─────────────────────── PROCESSAMENTO ────────────────────────

def importar_arquivo(db, arquivo: Path, nome_colecao: str, indices: list):
    """
    Importa um JSONL inteiro para uma coleção MongoDB.
    - Dropa a coleção antes (importação limpa, sem duplicatas)
    - Usa insert_many com ordered=False (máxima velocidade)
    - Não injeta campos extras (sem 'id' artificial)
    - Cria índices ao final
    """
    print(emoji.emojize(
        f"\n:open_file_folder: {arquivo.name} → coleção: {nome_colecao}"
    ))

    # Drop para garantir importação limpa
    db.drop_collection(nome_colecao)
    collection = db[nome_colecao]

    batch = []
    total = 0
    erros = 0
    t0 = time.perf_counter()

    with open(arquivo, "r", encoding="utf-8") as f:
        for line in f:
            try:
                doc = json.loads(line)
                batch.append(doc)

                if len(batch) >= BATCH_SIZE:
                    collection.insert_many(batch, ordered=False)
                    total += len(batch)
                    batch = []

                    if total % LOG_INTERVAL == 0:
                        elapsed = time.perf_counter() - t0
                        rate = total / elapsed if elapsed > 0 else 0
                        print(
                            f"    ... {total:>12,} docs  "
                            f"({rate:,.0f} docs/s)",
                            flush=True,
                        )

            except json.JSONDecodeError:
                erros += 1
            except Exception as e:
                erros += 1
                if erros <= 3:
                    print(f"    ⚠️ Erro: {e}")

    # Flush restante
    if batch:
        collection.insert_many(batch, ordered=False)
        total += len(batch)

    dt = time.perf_counter() - t0
    rate = total / dt if dt > 0 else 0

    print(emoji.emojize(
        f":check_mark_button: {nome_colecao}: "
        f"{total:,} docs em {dt:.1f}s ({rate:,.0f} docs/s)"
        + (f" | {erros} erros" if erros else "")
    ))

    # Criar índices para consultas rápidas
    print(f"    📇 Criando índice em {[i[0] for i in indices]} ...", end=" ", flush=True)
    t_idx = time.perf_counter()
    collection.create_index(indices, unique=(len(indices) == 1))
    print(f"OK ({time.perf_counter() - t_idx:.1f}s)")

    return total

# ─────────────────────────── MAIN ─────────────────────────────

def main():
    print("=" * 60)
    print("  IMPORTAÇÃO DE DADOS → MongoDB")
    print("=" * 60)

    if not PASTA_DADOS.exists():
        print(f"❌ Pasta não encontrada: {PASTA_DADOS}")
        print("   Execute primeiro: python src/clean_data.py")
        return

    # Verificar quais arquivos existem
    arquivos_disponiveis = []
    for nome_arquivo, (nome_colecao, indices) in COLECOES.items():
        caminho = PASTA_DADOS / nome_arquivo
        if caminho.exists():
            tamanho = caminho.stat().st_size / (1024 * 1024)
            arquivos_disponiveis.append((caminho, nome_colecao, indices, tamanho))
            print(f"  📄 {nome_arquivo:<25s} {tamanho:>8.1f} MB")
        else:
            print(f"  ⚠️ {nome_arquivo:<25s} NÃO ENCONTRADO")

    if not arquivos_disponiveis:
        print("\n❌ Nenhum arquivo para importar.")
        return

    db = connect()

    t_total = time.perf_counter()
    total_geral = 0

    for caminho, nome_colecao, indices, _ in arquivos_disponiveis:
        total_geral += importar_arquivo(db, caminho, nome_colecao, indices)

    dt = time.perf_counter() - t_total

    print("\n" + "=" * 60)
    print(f"  ✅ IMPORTAÇÃO CONCLUÍDA")
    print(f"     Total:    {total_geral:,} documentos")
    print(f"     Tempo:    {dt:.1f}s")
    print(f"     Coleções: {db.list_collection_names()}")
    print("=" * 60)


if __name__ == "__main__":
    main()