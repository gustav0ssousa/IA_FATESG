import sqlite3
import base64
import os

# ==========================================
# 1. FUNÇÕES DE CONVERSÃO
# ==========================================

def image_to_base64(caminho_imagem):
    """Lê uma imagem e retorna a string em Base64."""
    try:
        with open(caminho_imagem, "rb") as image_file:
            # b64encode retorna bytes, então usamos .decode('utf-8') para virar texto (string)
            string_base64 = base64.b64encode(image_file.read()).decode('utf-8')
            return string_base64
    except FileNotFoundError:
        print(f"Erro: A imagem '{caminho_imagem}' não foi encontrada.")
        return None

def base64_to_image(string_base64, caminho_saida):
    """Recebe uma string Base64 e salva como um arquivo de imagem."""
    # Convertemos a string de volta para bytes antes de decodificar
    image_data = base64.b64decode(string_base64.encode('utf-8'))
    with open(caminho_saida, "wb") as file:
        file.write(image_data)
    print(f"Imagem recuperada e salva com sucesso em: {caminho_saida}")


# ==========================================
# 2. OPERAÇÕES DE BANCO DE DADOS (SQLite)
# ==========================================

def criar_banco():
    """Cria a conexão e a tabela se não existir."""
    conn = sqlite3.connect("meu_banco_imagens.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS imagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            dados_base64 TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn

# ==========================================
# 3. TESTANDO O FLUXO COMPLETO
# ==========================================

if __name__ == "__main__":
    imagem_original = "teste.jpg" # Substitua pelo nome de uma imagem real sua
    imagem_recuperada = "recuperada.jpg"
    
    # Cria uma imagem de teste genérica só para o script não falhar caso você não tenha uma
    if not os.path.exists(imagem_original):
        with open(imagem_original, "wb") as f:
            f.write(os.urandom(1024)) # Cria um arquivo aleatório de 1KB
            
    # Conecta no banco
    conexao = criar_banco()
    cursor = conexao.cursor()

    # --- PASSO A: Converter para Base64 ---
    print("Convertendo imagem para Base64...")
    meu_base64 = image_to_base64(imagem_original)

    if meu_base64:
        # --- PASSO B: Salvar no Banco ---
        print("Salvando no banco de dados...")
        cursor.execute("INSERT INTO imagens (nome, dados_base64) VALUES (?, ?)", 
                       ("Minha Primeira Imagem", meu_base64))
        conexao.commit()

        # --- PASSO C: Recuperar do Banco ---
        print("Recuperando do banco de dados...")
        # Pegando o último registro inserido
        cursor.execute("SELECT dados_base64 FROM imagens ORDER BY id DESC LIMIT 1")
        resultado = cursor.fetchone()

        if resultado:
            base64_recuperado = resultado[0]
            
            # --- PASSO D: Converter de volta para Imagem ---
            base64_to_image(base64_recuperado, imagem_recuperada)
            
    conexao.close()