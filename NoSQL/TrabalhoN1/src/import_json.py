"""
╔══════════════════════════════════════════════════════════════╗
║  IMPORTAÇÃO DE DADOS JSONL → MongoDB
║  Lê arquivos .jsonl de data/clean/ e importa para o MongoDB
║  Usa Docker container via localhost:27017
╚══════════════════════════════════════════════════════════════╝

Executar a partir da raiz do projeto:
    python src/import_json.py
"""

import emoji
import os
import json
import re
import uuid
import time
from pathlib import Path
from pymongo import MongoClient, UpdateOne

# ─────────────────────────── CONFIG ───────────────────────────

# Raiz do projeto (pai de src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME      = os.environ.get("DB_NAME", "DB_Producao_Artistica")
PASTA_DADOS  = PROJECT_ROOT / "data" / "clean"
BATCH_SIZE   = 1000
UPSERT       = True

# ─────────────────────────── CONEXÃO ─────────────────────────

def connect():
    """Conecta ao MongoDB e retorna o database."""
    print(emoji.emojize(f":globe_with_meridians: Conectando a {MONGO_URI} ..."))
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Testar conexão
    client.admin.command("ping")
    print(emoji.emojize(f":check_mark_button: Conectado ao MongoDB!"))
    db = client[DB_NAME]
    return db

# ─────────────────────────── COLEÇÕES ─────────────────────────

def get_collection_name(file_path):
    """Gera nome da coleção a partir do nome do arquivo."""
    nome = os.path.basename(file_path)
    nome = os.path.splitext(nome)[0]
    # Remove sufixo _clean se presente
    nome = nome.replace("_clean", "")
    nome = re.sub(r'[^a-zA-Z0-9_]', '_', nome)
    return nome

# ─────────────────────────── FLUSH ────────────────────────────

def flush(collection, batch):
    """Envia lote para o MongoDB."""
    if UPSERT:
        collection.bulk_write(batch, ordered=False)
    else:
        collection.insert_many(batch)

# ─────────────────────── PROCESSAMENTO ────────────────────────

def process_file(db, file_path):
    """Importa um arquivo JSONL para uma coleção MongoDB."""
    collection_name = get_collection_name(file_path)
    collection = db[collection_name]

    print(emoji.emojize(
        f"\n:open_file_folder: Importando {os.path.basename(file_path)} → coleção: {collection_name}"
    ))

    batch = []
    count = 0
    errors = 0
    t0 = time.perf_counter()

    with open(file_path, "r", encoding='utf-8') as f:
        for line in f:
            try:
                doc = json.loads(line)

                if UPSERT:
                    doc["id"] = doc.get("id", str(uuid.uuid4()))
                    batch.append(
                        UpdateOne(
                            {"id": doc["id"]},
                            {"$set": doc},
                            upsert=True
                        )
                    )
                else:
                    batch.append(doc)

                if len(batch) >= BATCH_SIZE:
                    flush(collection, batch)
                    count += len(batch)
                    batch = []

                    if count % 100_000 == 0:
                        print(f"    ... {count:,} documentos importados", flush=True)

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"    ⚠️ Erro: {e}")

    if batch:
        flush(collection, batch)
        count += len(batch)

    dt = time.perf_counter() - t0
    print(emoji.emojize(
        f":check_mark_button: {collection_name}: {count:,} docs importados em {dt:.1f}s"
        + (f" ({errors} erros)" if errors else "")
    ))

# ─────────────────────────── MAIN ─────────────────────────────

def ingest_all():
    """Importa todos os arquivos .jsonl da pasta de dados."""
    if not PASTA_DADOS.exists():
        print(f"❌ Pasta não encontrada: {PASTA_DADOS}")
        print("   Execute primeiro: python src/clean_data.py")
        return

    arquivos = sorted([
        str(f) for f in PASTA_DADOS.iterdir()
        if f.suffix == ".jsonl"
    ])

    if not arquivos:
        print(f"❌ Nenhum arquivo .jsonl encontrado em {PASTA_DADOS}")
        return

    print(f"\n📂 Arquivos encontrados: {len(arquivos)}")
    for a in arquivos:
        print(f"   • {os.path.basename(a)}")

    db = connect()

    t_total = time.perf_counter()

    for arquivo in arquivos:
        process_file(db, arquivo)

    dt = time.perf_counter() - t_total

    print("\n" + "=" * 60)
    print(f"  ✅ IMPORTAÇÃO CONCLUÍDA em {dt:.1f}s")
    print(f"  📊 Coleções no banco: {db.list_collection_names()}")
    print("=" * 60)


if __name__ == "__main__":
    ingest_all()