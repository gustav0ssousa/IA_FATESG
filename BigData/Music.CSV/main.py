import requests
import csv
from spotipy.oauth2 import SpotifyClientCredentials

# 1. CREDENCIAIS
CLIENT_ID = 'c8ec8a0c4a9043cdabf1d1bbd7a804fa'
CLIENT_SECRET = 'af3a67dc544b4f618962760ba4b42762'

auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

def coletar_dados_artista(nome_artista, token):
    headers = {'Authorization': f'Bearer {token}'}
    
    # PASSO 1: Busca o Artista
    url_busca = f"https://api.spotify.com/v1/search?q={nome_artista}&type=artist&limit=1"
    res_busca = requests.get(url_busca, headers=headers).json()
    
    items_artista = res_busca.get('artists', {}).get('items', [])
    if not items_artista:
        print(f"⚠️ Artista {nome_artista} não encontrado na busca.")
        return None
        
    artist_id = items_artista[0]['id']

    # PASSO 2: Busca TUDO do artista (sem filtros de álbum/single)
    url_albuns = f"https://api.spotify.com/v1/artists/{artist_id}/albums?limit=5"
    res_albuns = requests.get(url_albuns, headers=headers).json()
    
    # --- LOG DE DIAGNÓSTICO ---
    print(f"\n🔍 Diagnóstico para {nome_artista}:")
    print(f"ID: {artist_id}")
    print(f"Itens encontrados no Spotify: {len(res_albuns.get('items', []))}")
    
    lista_final = []
    for item in res_albuns.get('items', []):
        album_id = item['id']
        album_nome = item['name']
        
        # PASSO 3: Busca Faixas
        url_faixas = f"https://api.spotify.com/v1/albums/{album_id}/tracks?limit=50"
        res_faixas = requests.get(url_faixas, headers=headers).json()
        
        faixas = res_faixas.get('items', [])
        for faixa in faixas:
            lista_final.append({
                'musica': faixa['name'],
                'artista': nome_artista,
                'album': album_nome,
                'data_lancamento': item.get('release_date', 'N/A'),
            })
        
    print(f"✅ Sucesso: {len(lista_final)} faixas extraídas.")
    return lista_final

# EXECUÇÃO
token = auth_manager.get_access_token(as_dict=False)
artistas_teste = ['Deftones', 'Travis Scott', 'Linkin Park', 'BTS', 'Banda Djavú', 'Limp Bizkit']
todos_dados = []

for art in artistas_teste:
    resultado = coletar_dados_artista(art, token)
    if resultado:
        todos_dados.extend(resultado)

if todos_dados:
    with open("resultado_final.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=['musica', 'artista', 'album', 'data_lancamento'])
        writer.writeheader()
        writer.writerows(todos_dados)
    print("\n📁 CSV gerado com sucesso!")