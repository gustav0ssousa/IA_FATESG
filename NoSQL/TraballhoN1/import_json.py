import emoji
import os
from pathlib import Path
import json
from pymongo import MongoClient, UpdateOne
import re
import uuid

# CONFIG

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "DB_Producao_Artistica"
PASTA_DADOS = "./data"
BATCH_SIZE = 1000
UPSERT = True

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# CRIAÇÃO DAS COLEÇÕES COM BASE NO ARQUIVO

def get_collection_name(file_path):
    nome = os.path.basename(file_path)
    nome = os.path.splitext(nome)[0]
    nome = re.sub(r'[^a-zA-Z0-9_]', '_', nome)
    return f"raw_{nome}"

# PROCESSAMENTO DOS ARQUIVOS

def process_file(file_path):
    collection_name = get_collection_name(file_path)
    collection = db[collection_name]
    
    print(emoji.emojize(f":open_file_folder: Importando {file_path} → coleção: {collection_name}"))
    
    batch = []
    
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
                    batch = []
            
            except Exception as e:
                print(f"Erro: {e}")
    if batch:
        flush(collection, batch)
    
    print(emoji.emojize(f":check_mark_button: Finalizado: {collection_name}"))

# ENVIO EM LOTE

def flush(collection, batch):
    if UPSERT:
        collection.bulk_write(batch, ordered=False)
    
    else:
        collection.insert_many(batch)

# ARQUIVOS DA PASTA DATA

def ingest_all():
    arquivos = [
        os.path.join(PASTA_DADOS, f)
        for f in os.listdir(PASTA_DADOS)
        if f.endswith(".jsonl")
    ]
    
    for arquivo in arquivos:
        process_file(arquivo)

if __name__ == "__main__":
    ingest_all()

print(db.list_collection_names())