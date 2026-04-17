"""
╔══════════════════════════════════════════════════════════════╗
║  ETL COM APACHE SPARK - Produção Artística
║  Lê JSONL brutos, limpa com PySpark e exporta limpos
║  Método Distribuído e em Memória (DataFrames)
╚══════════════════════════════════════════════════════════════╝

Pré-requisito: pip install pyspark

Executar a partir da raiz do projeto:
    python src/spark_pipeline.py
"""

import os
import time
from pathlib import Path

# Tentará importar o PySpark, senão avisa o usuário.
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, trim, regexp_replace, when
    from pyspark.sql.types import IntegerType
except ImportError:
    print("❌ Falta dependência: PySpark não está instalado.")
    print("   Execute: pip install pyspark")
    exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PASTA_DADOS  = PROJECT_ROOT / "data"
PASTA_LIMPOS = PASTA_DADOS / "clean_spark"

def main():
    print("=" * 60)
    print("  ETL DE DADOS COM APACHE SPARK")
    print("=" * 60)

    if not PASTA_DADOS.exists():
        print(f"❌ Pasta de dados não encontrada: {PASTA_DADOS}")
        return

    # Iniciar SparkSession
    print("🚀 Inicializando Spark Session local...")
    spark = SparkSession.builder \
        .appName("Pipeline Producao Artistica Spark") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    # Silenciar logs default do Spark para ficar mais limpo no console
    spark.sparkContext.setLogLevel("ERROR")

    t_total = time.perf_counter()

    # ───────────────────── 1. PESSOA ──────────────────────
    print("\n🔄 Lendo e limpando: pessoa.jsonl ...")
    path_pessoa = str(PASTA_DADOS / "pessoa.jsonl")
    
    if not os.path.exists(path_pessoa):
        print(f"⚠️ {path_pessoa} não encontrado. Abortando.")
        return

    df_pessoa = spark.read.json(path_pessoa)
    
    # - Filtra nulos
    df_pessoa = df_pessoa.filter(col("id_pessoa").isNotNull())
    
    # - Normaliza 'nome' (espaços extras)
    if "nome" in df_pessoa.columns:
        df_pessoa = df_pessoa.withColumn("nome", trim(regexp_replace(col("nome"), "\\s+", " ")))
    
    # - Dedup
    df_pessoa = df_pessoa.dropDuplicates(["id_pessoa"])
    df_pessoa.cache()
    
    print(f"    ✅ Pessoa: {df_pessoa.count()} registros únicos após limpeza.")

    # ───────────────────── 2. PRODUÇÃO ────────────────────
    print("\n🔄 Lendo e limpando: producao.jsonl ...")
    path_producao = str(PASTA_DADOS / "producao.jsonl")
    
    if not os.path.exists(path_producao):
        print(f"⚠️ {path_producao} não encontrado. Abortando.")
        return

    df_producao = spark.read.json(path_producao)

    # - Filtra nulos
    df_producao = df_producao.filter(col("id_producao").isNotNull())

    # - Normaliza 'titulo'
    if "titulo" in df_producao.columns:
        df_producao = df_producao.withColumn("titulo", trim(regexp_replace(col("titulo"), "\\s+", " ")))

    # - Tratar 'ano': limpar não-numéricos, manter apenas anos 1800-2100
    if "ano" in df_producao.columns:
        df_producao = df_producao.withColumn("ano_str", regexp_replace(col("ano").cast("string"), "[^0-9]", ""))
        df_producao = df_producao.withColumn("ano_int", col("ano_str").cast(IntegerType()))
        df_producao = df_producao.withColumn("ano", 
            when((col("ano_int") >= 1800) & (col("ano_int") <= 2100), col("ano_int")).otherwise(None)
        ).drop("ano_str", "ano_int")
    
    # - Cast tipo_id para int
    if "tipo_id" in df_producao.columns:
        df_producao = df_producao.withColumn("tipo_id", col("tipo_id").cast(IntegerType()))

    # - Dedup
    df_producao = df_producao.dropDuplicates(["id_producao"])
    df_producao.cache()
    
    print(f"    ✅ Produção: {df_producao.count()} registros únicos após limpeza.")

    # ───────────────────── 3. EQUIPE ──────────────────────
    print("\n🔄 Lendo e limpando: equipe.jsonl (Base massiva) ...")
    path_equipe = str(PASTA_DADOS / "equipe.jsonl")
    
    if not os.path.exists(path_equipe):
        print(f"⚠️ {path_equipe} não encontrado. Abortando.")
        return

    df_equipe = spark.read.json(path_equipe)

    # - Filtra IDs nulos
    df_equipe = df_equipe.filter(col("id_producao").isNotNull() & col("id_pessoa").isNotNull())

    # - Integridade Referencial (Semi Join para remover órfãos)
    # Mantém a linha de equipe apenas se a pessoa EXISTIR em df_pessoa, e produção EXISTIR em df_producao.
    df_equipe = df_equipe.join(df_pessoa.select("id_pessoa"), "id_pessoa", "leftsemi")
    df_equipe = df_equipe.join(df_producao.select("id_producao"), "id_producao", "leftsemi")

    # - Normaliza 'papel'
    if "papel" in df_equipe.columns:
        df_equipe = df_equipe.withColumn("papel", trim(regexp_replace(col("papel"), "\\s+", " ")))

    # - Dedup
    df_equipe = df_equipe.dropDuplicates(["id_producao", "id_pessoa", "papel"])
    
    print(f"    ✅ Equipe: {df_equipe.count()} registros válidos após limpeza e joins.")

    # ───────────────────── EXPORTAÇÃO ──────────────────────
    print("\n💾 Exportando Datasets tratados em formato JSONL (particionado)...")
    
    # Exporta usando Iteradores Locais + Python standard I/O puro
    # *Workaround obrigatório* no Windows para não ser bloqueado pela falta do 'winutils.exe' do Hadoop
    import json
    
    PASTA_LIMPOS.mkdir(parents=True, exist_ok=True)
    
    def export_local(df, filepath):
        print(f"    ⏳ Gravando em disco: {filepath.name} ...")
        with open(filepath, "w", encoding="utf-8") as f:
            for row in df.toLocalIterator():
                f.write(json.dumps(row.asDict(), ensure_ascii=False) + "\n")

    export_local(df_pessoa, PASTA_LIMPOS / "pessoa_clean.jsonl")
    export_local(df_producao, PASTA_LIMPOS / "producao_clean.jsonl")
    export_local(df_equipe, PASTA_LIMPOS / "equipe_clean.jsonl")

    dt = time.perf_counter() - t_total

    print("\n" + "=" * 60)
    print(f"  ✨ PIPELINE SPARK CONCLUÍDO em {dt:.1f}s")
    print(f"  📁 Arquivos limpos exportados em: {PASTA_LIMPOS.resolve()}")
    print("=" * 60)
    
    # Parar sessão Spark
    spark.stop()

if __name__ == "__main__":
    main()
