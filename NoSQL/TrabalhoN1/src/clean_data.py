"""
╔══════════════════════════════════════════════════════════════╗
║  SCRIPT DE LIMPEZA E TRATAMENTO DE DADOS - Produção Artística
║  Processa: pessoa.jsonl, producao.jsonl, equipe.jsonl
║  Saída:    data/clean/  (arquivos limpos)
║  Método:   Streaming line-by-line + dedup in-memory via sets
╚══════════════════════════════════════════════════════════════╝

Executar a partir da raiz do projeto:
    python src/clean_data.py
"""

import json
import re
import time
from pathlib import Path

# ─────────────────────────── CONFIG ───────────────────────────

# Raiz do projeto (pai de src/)
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
PASTA_DADOS   = PROJECT_ROOT / "data"
PASTA_LIMPOS  = PASTA_DADOS / "clean"
BATCH_WRITE   = 10_000       # linhas acumuladas antes de flush em disco
LOG_INTERVAL  = 500_000      # progresso a cada N linhas lidas

# ─────────────────────────── UTILIDADES ───────────────────────

class Stats:
    """Contadores de estatísticas por arquivo."""
    def __init__(self, nome: str):
        self.nome = nome
        self.lidos = 0
        self.escritos = 0
        self.duplicatas = 0
        self.nulos_removidos = 0
        self.correcoes = 0
        self.erros_parse = 0
        self.orfaos = 0
        self.t0 = time.perf_counter()

    def resumo(self) -> str:
        dt = time.perf_counter() - self.t0
        return (
            f"  📄 {self.nome}\n"
            f"     Lidos:           {self.lidos:>12,}\n"
            f"     Escritos:        {self.escritos:>12,}\n"
            f"     Duplicatas:      {self.duplicatas:>12,}\n"
            f"     Nulos removidos: {self.nulos_removidos:>12,}\n"
            f"     Correções:       {self.correcoes:>12,}\n"
            f"     Órfãos:          {self.orfaos:>12,}\n"
            f"     Erros parse:     {self.erros_parse:>12,}\n"
            f"     Tempo:           {dt:>11.1f}s\n"
        )


def progresso(stats: Stats):
    """Imprime progresso inline."""
    if stats.lidos % LOG_INTERVAL == 0 and stats.lidos > 0:
        print(f"    ... {stats.nome}: {stats.lidos:,} lidos, {stats.escritos:,} escritos", flush=True)


def normalize_string(s: str) -> str:
    """Normaliza strings: trim, colapsar espaços múltiplos."""
    if not isinstance(s, str):
        return s
    s = s.strip()
    s = re.sub(r'\s+', ' ', s)
    return s


def flush_buffer(f_out, buffer: list):
    """Escreve buffer acumulado no arquivo de saída."""
    if buffer:
        f_out.write('\n'.join(buffer))
        f_out.write('\n')
        buffer.clear()


# ───────────────────── 1. LIMPAR PESSOA ──────────────────────

def limpar_pessoa() -> set:
    """
    Limpeza de pessoa.jsonl:
      - Remove duplicatas de id_pessoa (mantém primeira ocorrência)
      - Normaliza campo 'nome'
      - Remove registros com id_pessoa nulo
    Retorna: set dos id_pessoa válidos (para checagem referencial)
    """
    arquivo_in  = PASTA_DADOS / "pessoa.jsonl"
    arquivo_out = PASTA_LIMPOS / "pessoa_clean.jsonl"
    stats = Stats("pessoa.jsonl")
    ids_vistos = set()
    buffer = []

    print(f"\n🔄 Processando {arquivo_in.name} ...")

    with open(arquivo_in, 'r', encoding='utf-8') as f_in, \
         open(arquivo_out, 'w', encoding='utf-8') as f_out:

        for line in f_in:
            stats.lidos += 1
            progresso(stats)

            try:
                doc = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                stats.erros_parse += 1
                continue

            # ID obrigatório
            id_pessoa = doc.get("id_pessoa")
            if id_pessoa is None:
                stats.nulos_removidos += 1
                continue

            # Dedup
            if id_pessoa in ids_vistos:
                stats.duplicatas += 1
                continue
            ids_vistos.add(id_pessoa)

            # Normalizar nome
            nome = doc.get("nome")
            if nome is not None:
                nome_limpo = normalize_string(nome)
                if nome_limpo != nome:
                    stats.correcoes += 1
                doc["nome"] = nome_limpo if nome_limpo else None

            # Remover campos None
            doc = {k: v for k, v in doc.items() if v is not None}

            buffer.append(json.dumps(doc, ensure_ascii=False))
            stats.escritos += 1

            if len(buffer) >= BATCH_WRITE:
                flush_buffer(f_out, buffer)

        flush_buffer(f_out, buffer)

    print(stats.resumo())
    return ids_vistos


# ───────────────────── 2. LIMPAR PRODUCAO ────────────────────

def limpar_producao() -> set:
    """
    Limpeza de producao.jsonl:
      - Remove duplicatas de id_producao
      - Normaliza 'titulo' (trim + colapsar espaços)
      - Corrige 'ano' com caracteres inválidos (ex: '#2004' → '2004')
      - Converte 'ano' para inteiro quando possível
      - Remove registros com id_producao nulo
    Retorna: set dos id_producao válidos
    """
    arquivo_in  = PASTA_DADOS / "producao.jsonl"
    arquivo_out = PASTA_LIMPOS / "producao_clean.jsonl"
    stats = Stats("producao.jsonl")
    ids_vistos = set()
    buffer = []

    print(f"\n🔄 Processando {arquivo_in.name} ...")

    with open(arquivo_in, 'r', encoding='utf-8') as f_in, \
         open(arquivo_out, 'w', encoding='utf-8') as f_out:

        for line in f_in:
            stats.lidos += 1
            progresso(stats)

            try:
                doc = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                stats.erros_parse += 1
                continue

            # ID obrigatório
            id_producao = doc.get("id_producao")
            if id_producao is None:
                stats.nulos_removidos += 1
                continue

            # Dedup
            if id_producao in ids_vistos:
                stats.duplicatas += 1
                continue
            ids_vistos.add(id_producao)

            # Normalizar titulo
            titulo = doc.get("titulo")
            if titulo is not None:
                titulo_limpo = normalize_string(titulo)
                if titulo_limpo != titulo:
                    stats.correcoes += 1
                doc["titulo"] = titulo_limpo if titulo_limpo else None
            else:
                stats.nulos_removidos += 1

            # Tratar ano: remover caracteres não-numéricos, converter para int
            ano = doc.get("ano")
            if ano is not None:
                ano_str = str(ano).strip()
                ano_limpo = re.sub(r'[^0-9]', '', ano_str)
                if ano_limpo:
                    ano_int = int(ano_limpo)
                    # Validar range razoável (cinema/produção artística)
                    if 1800 <= ano_int <= 2100:
                        doc["ano"] = ano_int
                        if ano_str != str(ano_int):
                            stats.correcoes += 1
                    else:
                        doc["ano"] = None
                        stats.correcoes += 1
                else:
                    doc["ano"] = None
                    stats.correcoes += 1

            # tipo_id: garantir inteiro
            tipo_id = doc.get("tipo_id")
            if tipo_id is not None:
                try:
                    doc["tipo_id"] = int(tipo_id)
                except (ValueError, TypeError):
                    doc["tipo_id"] = None
                    stats.correcoes += 1

            # Remover campos None
            doc = {k: v for k, v in doc.items() if v is not None}

            buffer.append(json.dumps(doc, ensure_ascii=False))
            stats.escritos += 1

            if len(buffer) >= BATCH_WRITE:
                flush_buffer(f_out, buffer)

        flush_buffer(f_out, buffer)

    print(stats.resumo())
    return ids_vistos


# ───────────────────── 3. LIMPAR EQUIPE ──────────────────────

def limpar_equipe(ids_pessoa: set, ids_producao: set):
    """
    Limpeza de equipe.jsonl (MAIOR ARQUIVO - ~12.8M linhas):
      - Remove duplicatas exatas (id_producao + id_pessoa + papel)
      - Remove órfãos (referências a pessoa/producao inexistentes)
      - Normaliza 'papel' (trim + colapsar espaços)
      - Remove registros com id_producao ou id_pessoa nulos
      - Trata papel=null (mantém registro, remove campo)
    """
    arquivo_in  = PASTA_DADOS / "equipe.jsonl"
    arquivo_out = PASTA_LIMPOS / "equipe_clean.jsonl"
    stats = Stats("equipe.jsonl")
    vistos = set()
    buffer = []

    print(f"\n🔄 Processando {arquivo_in.name} (arquivo grande, pode demorar) ...")

    with open(arquivo_in, 'r', encoding='utf-8') as f_in, \
         open(arquivo_out, 'w', encoding='utf-8') as f_out:

        for line in f_in:
            stats.lidos += 1
            progresso(stats)

            try:
                doc = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                stats.erros_parse += 1
                continue

            # IDs obrigatórios
            id_producao = doc.get("id_producao")
            id_pessoa   = doc.get("id_pessoa")

            if id_producao is None or id_pessoa is None:
                stats.nulos_removidos += 1
                continue

            # Integridade referencial
            if id_pessoa not in ids_pessoa:
                stats.orfaos += 1
                continue
            if id_producao not in ids_producao:
                stats.orfaos += 1
                continue

            # Normalizar papel
            papel = doc.get("papel")
            if papel is not None:
                papel_limpo = normalize_string(papel)
                if papel_limpo != papel:
                    stats.correcoes += 1
                papel = papel_limpo if papel_limpo else None
                doc["papel"] = papel

            # Dedup por chave composta
            chave = (id_producao, id_pessoa, papel)
            if chave in vistos:
                stats.duplicatas += 1
                continue
            vistos.add(chave)

            # Remover campos None
            doc = {k: v for k, v in doc.items() if v is not None}

            buffer.append(json.dumps(doc, ensure_ascii=False))
            stats.escritos += 1

            if len(buffer) >= BATCH_WRITE:
                flush_buffer(f_out, buffer)

        flush_buffer(f_out, buffer)

    print(stats.resumo())


# ─────────────────────────── MAIN ─────────────────────────────

def main():
    print("=" * 60)
    print("  LIMPEZA DE DADOS - Produção Artística")
    print("=" * 60)

    # Verificar se os dados existem
    if not PASTA_DADOS.exists():
        print(f"❌ Pasta de dados não encontrada: {PASTA_DADOS}")
        print("   Coloque os arquivos .jsonl em data/")
        return

    # Criar pasta de saída
    PASTA_LIMPOS.mkdir(parents=True, exist_ok=True)

    t_total = time.perf_counter()

    # 1) Pessoa primeiro (gera set de IDs válidos)
    ids_pessoa = limpar_pessoa()

    # 2) Produção (gera set de IDs válidos)
    ids_producao = limpar_producao()

    # 3) Equipe por último (usa os sets para validação referencial)
    limpar_equipe(ids_pessoa, ids_producao)

    dt = time.perf_counter() - t_total

    print("=" * 60)
    print(f"  ✅ CONCLUÍDO em {dt:.1f}s")
    print(f"  📁 Arquivos limpos em: {PASTA_LIMPOS.resolve()}")
    print("=" * 60)

    # Comparar tamanhos
    print("\n  📊 Comparação de tamanhos:")
    for nome_in, nome_out in [
        ("pessoa.jsonl", "pessoa_clean.jsonl"),
        ("producao.jsonl", "producao_clean.jsonl"),
        ("equipe.jsonl", "equipe_clean.jsonl"),
    ]:
        orig = PASTA_DADOS / nome_in
        limpo = PASTA_LIMPOS / nome_out
        if orig.exists() and limpo.exists():
            tam_orig  = orig.stat().st_size / (1024 * 1024)
            tam_limpo = limpo.stat().st_size / (1024 * 1024)
            reducao   = (1 - tam_limpo / tam_orig) * 100 if tam_orig > 0 else 0
            print(f"     {nome_in:<20s}  {tam_orig:>8.1f} MB → {tam_limpo:>8.1f} MB  ({reducao:+.1f}%)")


if __name__ == "__main__":
    main()
