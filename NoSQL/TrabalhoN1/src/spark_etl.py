"""
╔══════════════════════════════════════════════════════════════╗
║  SCRIPT DE IMPORTACAO MONGODB COM APACHE SPARK               ║
║  Processa: data/clean/*_clean.jsonl                          ║
║  Saída:    MongoDB (coleções *_spark)                        ║
║  Método:   PySpark DataFrame + Mongo Spark Connector         ║
╚══════════════════════════════════════════════════════════════╝

Pré-requisitos:
- Instalar PySpark (`pip install pyspark`)
- Ter o MongoDB rodando localmente (na porta 27017) ou ajustar MONGO_URI
"""

import os
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession

# Force utf-8 printing for emojis on Windows PowerShell
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# ─────────────────────────── CONFIG ───────────────────────────

# Configurações do MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME   = os.getenv("DB_NAME", "DB_Producao_Artistica")

# Diretório dos arquivos (Assumindo que roda da raiz do projeto)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PASTA_LIMPOS  = PROJECT_ROOT / "data" / "clean"

# ─────────────────────────── MAIN ─────────────────────────────

def main():
    print("=" * 60)
    print("  INICIALIZANDO APACHE SPARK + MONGODB")
    print("=" * 60)

    # Verifica se os arquivos existem antes de iniciar o Spark (que é pesado)
    if not PASTA_LIMPOS.exists():
        print(f"❌ Pasta de dados não encontrada: {PASTA_LIMPOS}")
        print("   Execute o src/clean_data.py primeiro.")
        return

    # Workaround para evitar o erro de Subject.getSubject() no Java 24+
    # Forçar um usuário no Hadoop evita que ele tente consultar o Subject nativo da JVM
    os.environ["HADOOP_USER_NAME"] = "spark_user"
    
    # Remover qualquer argumento que tente ativar o Security Manager (já que foi removido no Java 23+)
    if "PYSPARK_SUBMIT_ARGS" in os.environ:
        del os.environ["PYSPARK_SUBMIT_ARGS"]

    # Inicia a sessão Spark com o conector Mongo
    # Importante: A configuração spark.jars.packages baixa automaticamente
    # o driver necessário para comunicar com o MongoDB.
    
    base_mongo_uri = f"{MONGO_URI.rstrip('/')}/{DB_NAME}"

    try:
        spark = SparkSession.builder \
            .appName("ETL_Producao_Artistica_Mongo") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0") \
            .config("spark.mongodb.read.connection.uri", base_mongo_uri) \
            .config("spark.mongodb.write.connection.uri", base_mongo_uri) \
            .getOrCreate()
    except Exception as e:
        print(f"❌ Erro ao iniciar a sessão PySpark: {e}")
        return
    
    spark.sparkContext.setLogLevel("WARN")

    t_total = time.perf_counter()

    # Mapeamento do arquivo limpo para a nova coleção do MongoDB (com _spark para separar da original)
    arquivos_alvo = [
        ("pessoa_clean.jsonl", "pessoa_spark"),
        ("producao_clean.jsonl", "producao_spark"),
        ("equipe_clean.jsonl", "equipe_spark")
    ]

    for nome_arquivo, nome_colecao in arquivos_alvo:
        filepath = PASTA_LIMPOS / nome_arquivo
        
        if not filepath.exists():
            print(f"⚠️ Aviso: Arquivo {filepath.name} não encontrado, pulando...")
            continue
            
        print(f"\n🔄 Carregando {filepath.name} no DataFrame PySpark...")
        t_inicio = time.perf_counter()

        try:
            # Lê os JSONL direto do disco para um DataFrame particionado
            df = spark.read.json(str(filepath))
            
            qtde_registros = df.count()
            print(f"   ✓ Lidos {qtde_registros:,} registros. Escrevendo no MongoDB (Collection: {nome_colecao})...")

            # Escreve o DataFrame no MongoDB. 
            # Modos: "append", "overwrite", "ignore", "errorifexists"
            df.write.format("mongodb") \
                .mode("overwrite") \
                .option("database", DB_NAME) \
                .option("collection", nome_colecao) \
                .save()

            dt = time.perf_counter() - t_inicio
            print(f"   ✅ Concluído em {dt:.1f}s")
        except Exception as e:
            print(f"   ❌ Erro ao processar arquivo {nome_arquivo}: {e}")

    dt_total = time.perf_counter() - t_total

    print("=" * 60)
    print(f"  ✅ INGESTÃO MONGODB VIA SPARK CONCLUÍDA EM {dt_total:.1f}s")
    print("=" * 60)

    # Encerra o Spark
    spark.stop()


if __name__ == "__main__":
    main()
