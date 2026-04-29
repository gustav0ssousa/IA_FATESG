"""
╔══════════════════════════════════════════════════════════════╗
║  DASHBOARD STREAMLIT - Produção Artística (MongoDB)
║  Visualização interativa dos dados importados.
╚══════════════════════════════════════════════════════════════╝

Executar a partir da raiz do projeto:
    streamlit run src/app.py
"""

import os
import streamlit as st
import pandas as pd
from pymongo import MongoClient

# ─────────────────────────── CONFIG & SETUP ───────────────────────────

st.set_page_config(
    page_title="Dashboard | Produção Artística",
    page_icon="🎬",
    layout="wide"
)

# URI do MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME   = os.environ.get("DB_NAME", "DB_Producao_Artistica")
REQUIRED_COLLECTIONS = ["pessoa_clean", "producao_clean", "equipe_clean"]

@st.cache_resource
def get_db_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Testar a conexão
        client.admin.command("ping")
        return client[DB_NAME], None
    except Exception as e:
        return None, str(e)

db, connection_error = get_db_connection()

# ─────────────────────────── INTERFACE ───────────────────────────

st.title("🎬 Dashboard — DB Produção Artística")
st.markdown("Visualização e exploração do dataset mantido no **MongoDB**.")

if db is None:
    st.error("❌ Não foi possível conectar ao MongoDB.")
    if connection_error:
        st.code(connection_error)
    st.stop()
else:
    st.success("✅ Conectado ao MongoDB com sucesso!")

available_collections = set(db.list_collection_names())
missing_collections = [
    collection
    for collection in REQUIRED_COLLECTIONS
    if collection not in available_collections
]

if missing_collections:
    st.warning(
        "⚠️ O MongoDB está conectado, mas o banco ainda não possui "
        "as coleções esperadas do projeto."
    )
    st.write("Coleções ausentes:", ", ".join(missing_collections))
    st.write("Importe os dados com:")
    st.code(
        "docker compose -f docker/docker-compose.yml exec streamlit "
        "python src/import_json.py"
    )
    st.stop()

st.divider()

# -- SEÇÃO 1: MÉTRICAS GERAIS --
st.subheader("📊 Visão Geral")

col1, col2, col3 = st.columns(3)

# Contagem em tempo real das coleções. (Pode demorar um pouquinho em coleções muito massivas sem índice adequado, 
# mas usando estimated_document_count é super rápido O(1))
with st.spinner("Carregando métricas..."):
    total_pessoas    = db["pessoa_clean"].estimated_document_count()
    total_producoes  = db["producao_clean"].estimated_document_count()
    total_equipe     = db["equipe_clean"].estimated_document_count()

col1.metric("Pessoas Cadastradas", f"{total_pessoas:,}".replace(",", "."))
col2.metric("Produções Artísticas", f"{total_producoes:,}".replace(",", "."))
col3.metric("Participações (Equipe)", f"{total_equipe:,}".replace(",", "."))

st.divider()

# -- SEÇÃO 2: GRÁFICOS (CONSULTAS AGREGADAS) --

st.subheader("📈 Análises")
col_graf1, col_graf2 = st.columns(2)

# Gráfico 1: Títulos por Década/Ano (Agregação MongoDB)
with col_graf1:
    st.markdown("##### 📅 Produções por Ano (Top 20 anos com mais títulos)")
    with st.spinner("Agregando dados de anos..."):
        pipeline_anos = [
            {"$match": {"ano": {"$ne": None}}},
            {"$group": {"_id": "$ano", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": 20}
        ]
        res_anos = list(db["producao_clean"].aggregate(pipeline_anos))
        
        if res_anos:
            df_anos = pd.DataFrame(res_anos)
            df_anos.rename(columns={"_id": "Ano", "total": "Total"}, inplace=True)
            df_anos.sort_values(by="Ano", inplace=True)
            df_anos["Ano"] = df_anos["Ano"].astype(str) # Evitar que seja tratado como numero flutuante no eixo
            st.bar_chart(df_anos.set_index("Ano")["Total"])
        else:
            st.info("Nenhum dado de ano disponível.")

# Gráfico 2: Trabalhos mais comuns
with col_graf2:
    st.markdown("##### 🎭 Papéis Mais Comuns Atribuídos na Equipe")
    with st.spinner("Agregando dados de equipe..."):
        pipeline_papeis = [
            {"$match": {"papel": {"$ne": None}}},
            {"$group": {"_id": "$papel", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ]
        res_papeis = list(db["equipe_clean"].aggregate(pipeline_papeis))
        
        if res_papeis:
            df_papeis = pd.DataFrame(res_papeis)
            df_papeis.rename(columns={"_id": "Papel", "total": "Ocorrências"}, inplace=True)
            # Para st.bar_chart horizontal nativo precisaria de vega-lite, 
            # mas vamos manter o padrão nativo e trocar x e y se for Streamlit 1.35+. Em 1.20+ usamos st.bar_chart padrão.
            st.dataframe(df_papeis, use_container_width=True)
        else:
            st.info("Nenhum dado de papéis disponível.")

st.divider()

# -- SEÇÃO 3: BUSCA DE DADOS --
st.subheader("🔍 Explorador de Produções")

search_term = st.text_input("Busque produções pelo título (Ex: Matrix, Batman):", "")

if search_term:
    with st.spinner("Buscando..."):
        # Busca usando Regex no MongoDB (Case Insensitive)
        query = {"titulo": {"$regex": search_term, "$options": "i"}}
        limit_result = 50
        
        resultados = list(db["producao_clean"].find(query).limit(limit_result))
        
        if resultados:
            st.success(f"Foram encontradas {len(resultados)} produções contendo '{search_term}' (Listando até {limit_result}):")
            
            # Formatar para exibição
            df_resultados = pd.DataFrame(resultados)
            # Remover o _id nativo para ficar mais limpo
            if "_id" in df_resultados.columns:
                df_resultados.drop(columns=["_id"], inplace=True)
            
            st.dataframe(df_resultados)
        else:
            st.warning("Nenhuma produção encontrada com esse título.")
