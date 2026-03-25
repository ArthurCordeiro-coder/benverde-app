import os
import threading
from contextlib import contextmanager
from datetime import datetime

import psycopg
from psycopg import sql

_CONFIG_LOCK = threading.Lock()
_DB_CONFIG = None


def _get_config() -> dict:
    global _DB_CONFIG
    if _DB_CONFIG is not None:
        return _DB_CONFIG

    secrets = {}
    try:
        import streamlit as st  # noqa: F401

        secrets = st.secrets
    except ModuleNotFoundError:
        secrets = {}
    except Exception:
        secrets = {}

    def _resolve(key: str):
        return secrets.get(key) or os.environ.get(key)

    config = {
        "host": _resolve("PGHOST"),
        "dbname": _resolve("PGDATABASE"),
        "user": _resolve("PGUSER"),
        "password": _resolve("PGPASSWORD"),
        "sslmode": _resolve("PGSSLMODE"),
    }
    port = _resolve("PGPORT")
    if port:
        config["port"] = port
    channel_binding = _resolve("PGCHANNELBINDING")
    if channel_binding:
        config["channel_binding"] = channel_binding

    config = {k: v for k, v in config.items() if v}
    if "port" not in config:
        config["port"] = 5432

    with _CONFIG_LOCK:
        if _DB_CONFIG is None:
            _DB_CONFIG = config
        return _DB_CONFIG


@contextmanager
def get_connection():
    cfg = _get_config()
    if not cfg:
        raise RuntimeError("Banco de dados não configurado (falta PGHOST/PGUSER/PGPASSWORD/PGDATABASE)")
    conn = psycopg.connect(**cfg)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _format_identifier(name: str):
    return sql.Identifier(name)


def _ensure_tables():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            nome TEXT,
            salt TEXT,
            senha_hash TEXT,
            is_admin BOOLEAN,
            criado_em TIMESTAMPTZ,
            funcionalidade TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pending (
            username TEXT PRIMARY KEY,
            nome TEXT,
            salt TEXT,
            senha_hash TEXT,
            solicitado_em TIMESTAMPTZ,
            funcionalidade TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS lockouts (
            username TEXT PRIMARY KEY,
            tentativas INTEGER,
            bloqueado_ate TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS metas_local (
            produto TEXT PRIMARY KEY,
            meta BIGINT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS cache_estoque (
            key TEXT PRIMARY KEY,
            payload JSONB,
            updated_at TIMESTAMPTZ DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS cache_pedidos (
            key TEXT PRIMARY KEY,
            payload JSONB,
            updated_at TIMESTAMPTZ DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pedidos_importados (
            key TEXT PRIMARY KEY,
            payload JSONB,
            updated_at TIMESTAMPTZ DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS estoque_manual (
            id SERIAL PRIMARY KEY,
            data TIMESTAMPTZ,
            tipo TEXT,
            produto TEXT,
            quant DOUBLE PRECISION,
            unidade TEXT,
            loja TEXT,
            arquivo TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS caixas_lojas (
            id SERIAL PRIMARY KEY,
            data DATE,
            loja TEXT,
            n_loja INTEGER,
            caixas_benverde INTEGER,
            caixas_ccj INTEGER,
            ccj_banca INTEGER,
            ccj_mercadoria INTEGER,
            ccj_retirada INTEGER,
            caixas_bananas INTEGER,
            total INTEGER,
            entregue TEXT
        )
        """,
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)


_ensure_tables()

_CACHE_TABLES = {"cache_estoque", "cache_pedidos"}


def fetch_cache(table_name: str) -> dict:
    if table_name not in _CACHE_TABLES:
        return {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT key, payload FROM {table}").format(table=_format_identifier(table_name))
            )
            return {row[0]: row[1] for row in cur.fetchall()}


def upsert_cache(table_name: str, entries: dict) -> None:
    if table_name not in _CACHE_TABLES or not entries:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            stmt = sql.SQL(
                """
                INSERT INTO {table} (key, payload, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (key) DO UPDATE
                  SET payload = EXCLUDED.payload,
                      updated_at = EXCLUDED.updated_at
                """
            ).format(table=_format_identifier(table_name))
            for key, payload in entries.items():
                cur.execute(stmt, (key, payload))


def clear_cache_table(table_name: str) -> None:
    if table_name not in _CACHE_TABLES:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {table}").format(table=_format_identifier(table_name))
            )


def load_metas() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT produto, meta FROM metas_local ORDER BY produto")
            return [{"Produto": row[0], "Meta": row[1]} for row in cur.fetchall()]


def replace_metas(records: list[dict]) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM metas_local")
            for item in records:
                cur.execute(
                    "INSERT INTO metas_local (produto, meta) VALUES (%s, %s)",
                    (item.get("Produto"), item.get("Meta")),
                )


def insert_movimentacoes(records: list[dict]) -> None:
    if not records:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            for rec in records:
                cur.execute(
                    """
                    INSERT INTO estoque_manual (data, tipo, produto, quant, unidade, loja, arquivo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rec.get("data"),
                        rec.get("tipo"),
                        rec.get("produto"),
                        rec.get("quant"),
                        rec.get("unidade"),
                        rec.get("loja"),
                        rec.get("arquivo"),
                    ),
                )


def fetch_movimentacoes() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, data, tipo, produto, quant, unidade, loja, arquivo
                FROM estoque_manual
                ORDER BY id
                """
            )
            return [
                {
                    "id": row[0],
                    "data": row[1],
                    "tipo": row[2],
                    "produto": row[3],
                    "quant": row[4],
                    "unidade": row[5],
                    "loja": row[6],
                    "arquivo": row[7],
                }
                for row in cur.fetchall()
            ]


def delete_movimentacao(entry_id: int) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM estoque_manual WHERE id = %s", (entry_id,))


def insert_caixa(registro: dict) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO caixas_lojas (
                    data, loja, n_loja,
                    caixas_benverde, caixas_ccj, ccj_banca,
                    ccj_mercadoria, ccj_retirada, caixas_bananas,
                    total, entregue
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    registro.get("data"),
                    registro.get("loja"),
                    registro.get("n_loja"),
                    registro.get("caixas_benverde"),
                    registro.get("caixas_ccj"),
                    registro.get("ccj_banca"),
                    registro.get("ccj_mercadoria"),
                    registro.get("ccj_retirada"),
                    registro.get("caixas_bananas"),
                    registro.get("total"),
                    registro.get("entregue"),
                ),
            )


def fetch_caixas() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data, loja, n_loja, caixas_benverde, caixas_ccj,
                       ccj_banca, ccj_mercadoria, ccj_retirada,
                       caixas_bananas, total, entregue
                FROM caixas_lojas
                ORDER BY data DESC NULLS LAST, loja, n_loja
                """
            )
            return [
                {
                    "data": row[0],
                    "loja": row[1],
                    "n_loja": row[2],
                    "caixas_benverde": row[3],
                    "caixas_ccj": row[4],
                    "ccj_banca": row[5],
                    "ccj_mercadoria": row[6],
                    "ccj_retirada": row[7],
                    "caixas_bananas": row[8],
                    "total": row[9],
                    "entregue": row[10],
                }
                for row in cur.fetchall()
            ]


def fetch_pedidos_importados() -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload FROM pedidos_importados WHERE key = 'default'")
            row = cur.fetchone()
            return row[0] if row else []


def save_pedidos_importados(records: list[dict]) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pedidos_importados WHERE key = 'default'")
            cur.execute(
                """
                INSERT INTO pedidos_importados (key, payload, updated_at)
                VALUES ('default', %s, now())
                """,
                (records,),
            )
