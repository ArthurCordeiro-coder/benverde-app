import json
import os
import re
from decimal import Decimal
from typing import Any

import psycopg


# =========================
# CONFIGURAÇÃO DO BANCO
# =========================
DB_CONFIG = {
    PGHOST='ep-lively-sunset-acggdvv8-pooler.sa-east-1.aws.neon.tech'
    PGDATABASE='neondb'
    PGUSER='neondb_owner'
    PGPASSWORD='npg_VJeynq49WQGv'
    PGSSLMODE='require'
    PGCHANNELBINDING='require'
    port= 5432
}


# =========================
# AJUSTE DOS ARQUIVOS JSON
# =========================
# Coloque aqui os JSONs que você quer migrar
# chave = nome da tabela
# valor = caminho do arquivo json
ARQUIVOS_JSON = {
    "users": "users.json",
    "escolhas": "escolhas.json",
    "lockouts": "lockouts.json",
    "metas_local": "dados/cache/metas_local.json",
    "cache_pedidos": "dados/cache/cache_pedidos.json",
    "devcontainer": ".devcontainer/devcontainer.json",
    "estoque_manual": "dados/cache/estoque_manual.json",
}


def normalizar_nome_coluna(nome: str) -> str:
    """
    Converte nomes para algo mais seguro no PostgreSQL.
    Ex.: 'Loja Nome' -> 'loja_nome'
    """
    nome = nome.strip().lower()
    nome = re.sub(r"[^\w]+", "_", nome)
    nome = re.sub(r"_+", "_", nome)
    return nome.strip("_")


def inferir_tipo_postgres(valor: Any) -> str:
    """
    Inferência simples de tipo PostgreSQL com base no valor.
    """
    if isinstance(valor, bool):
        return "BOOLEAN"
    if isinstance(valor, int):
        return "BIGINT"
    if isinstance(valor, float) or isinstance(valor, Decimal):
        return "DOUBLE PRECISION"
    if valor is None:
        return "TEXT"
    if isinstance(valor, (dict, list)):
        return "JSONB"
    return "TEXT"


def consolidar_tipos(registros: list[dict]) -> dict[str, str]:
    """
    Analisa todos os registros e escolhe um tipo final por coluna.
    Se houver conflito, cai para TEXT ou JSONB.
    """
    tipos_por_coluna: dict[str, set[str]] = {}

    for registro in registros:
        for chave, valor in registro.items():
            col = normalizar_nome_coluna(chave)
            tipo = inferir_tipo_postgres(valor)
            tipos_por_coluna.setdefault(col, set()).add(tipo)

    tipos_finais: dict[str, str] = {}

    for coluna, tipos in tipos_por_coluna.items():
        if len(tipos) == 1:
            tipos_finais[coluna] = next(iter(tipos))
        elif "JSONB" in tipos:
            tipos_finais[coluna] = "JSONB"
        elif "TEXT" in tipos:
            tipos_finais[coluna] = "TEXT"
        elif "DOUBLE PRECISION" in tipos and "BIGINT" in tipos:
            tipos_finais[coluna] = "DOUBLE PRECISION"
        else:
            tipos_finais[coluna] = "TEXT"

    return tipos_finais


def criar_tabela(cur, nome_tabela: str, tipos_colunas: dict[str, str]) -> None:
    """
    Cria a tabela se ela não existir.
    Sempre adiciona uma coluna id serial.
    """
    colunas_sql = ['id SERIAL PRIMARY KEY']

    for coluna, tipo in tipos_colunas.items():
        colunas_sql.append(f'"{coluna}" {tipo}')

    sql = f'''
    CREATE TABLE IF NOT EXISTS "{nome_tabela}" (
        {", ".join(colunas_sql)}
    )
    '''
    cur.execute(sql)


def converter_valor_para_insert(valor: Any) -> Any:
    """
    Converte dict/list para JSON string para inserir em JSONB.
    """
    if isinstance(valor, (dict, list)):
        return json.dumps(valor, ensure_ascii=False)
    return valor


def inserir_registros(cur, nome_tabela: str, registros: list[dict]) -> None:
    """
    Insere os registros na tabela.
    """
    if not registros:
        return

    colunas_originais = list(registros[0].keys())
    mapa_colunas = {chave: normalizar_nome_coluna(chave) for chave in colunas_originais}
    colunas_sql = [f'"{mapa_colunas[chave]}"' for chave in colunas_originais]
    placeholders = ", ".join(["%s"] * len(colunas_originais))

    sql = f'''
    INSERT INTO "{nome_tabela}" ({", ".join(colunas_sql)})
    VALUES ({placeholders})
    '''

    for registro in registros:
        valores = [converter_valor_para_insert(registro.get(chave)) for chave in colunas_originais]
        cur.execute(sql, valores)


def carregar_json(caminho: str) -> list[dict]:
    """
    Carrega JSON esperando uma lista de objetos.
    Se vier um único objeto, transforma em lista.
    """
    with open(caminho, "r", encoding="utf-8") as f:
        conteudo = json.load(f)

    if isinstance(conteudo, dict):
        return [conteudo]

    if isinstance(conteudo, list):
        if all(isinstance(item, dict) for item in conteudo):
            return conteudo
        raise ValueError(f"O arquivo {caminho} contém uma lista, mas nem todos os itens são objetos.")

    raise ValueError(f"O arquivo {caminho} não contém nem objeto nem lista de objetos.")


def migrar_arquivo(conn, nome_tabela: str, caminho_json: str) -> None:
    """
    Migra um único arquivo JSON para uma tabela PostgreSQL.
    """
    if not os.path.exists(caminho_json):
        print(f"[AVISO] Arquivo não encontrado: {caminho_json}")
        return

    registros = carregar_json(caminho_json)

    if not registros:
        print(f"[AVISO] Arquivo vazio: {caminho_json}")
        return

    tipos_colunas = consolidar_tipos(registros)

    with conn.cursor() as cur:
        criar_tabela(cur, nome_tabela, tipos_colunas)
        inserir_registros(cur, nome_tabela, registros)

    conn.commit()
    print(f"[OK] Migrado: {caminho_json} -> tabela '{nome_tabela}' ({len(registros)} registros)")


def main():
    with psycopg.connect(**DB_CONFIG) as conn:
        for nome_tabela, caminho_json in ARQUIVOS_JSON.items():
            try:
                migrar_arquivo(conn, nome_tabela, caminho_json)
            except Exception as e:
                conn.rollback()
                print(f"[ERRO] Falha ao migrar '{caminho_json}' para '{nome_tabela}': {e}")


if __name__ == "__main__":
    main()
