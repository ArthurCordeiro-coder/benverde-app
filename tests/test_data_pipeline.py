"""
tests/test_data_pipeline.py
Testes para data_pipeline.py

Rodar com:
    python -m pytest tests/test_data_pipeline.py -v
    # ou sem pytest:
    python tests/test_data_pipeline.py
"""

import sys
import os
import json
import datetime

# Garante que o diretório pai esteja no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from data_pipeline import (
    estruturar_precos,
    estruturar_metas,
    estruturar_progresso,
    montar_dados_para_llm,
    _ultima_data_do_dict,
    _norm_key,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _df_precos():
    return pd.DataFrame({
        "Produto Buscado": ["BANANA NANICA", "BANANA DA TERRA", "TOMATE"],
        "Preço (Semar)":   [3.49, 4.99, 7.80],
        "Status (Semar)":  ["OK", "OK", "OK"],
        "Preço (Rossi)":   [3.59, None, 8.20],
        "Status (Rossi)":  ["OK", "Indisponível", "OK"],
    })


def _df_metas():
    return pd.DataFrame({
        "Produto": ["BANANA NANICA", "TOMATE", "CENOURA"],
        "Meta":    [500, 1000, 300],
    })


def _df_progresso():
    return pd.DataFrame({
        "Produtos":       ["BANANA NANICA", "TOMATE"],
        "meta":           [500.0, 1000.0],
        "pedido":         [420.0, 850.0],
        "Progresso":      [84.0, 85.0],
        "status da meta": ["META EM ANDAMENTO", "META EM ANDAMENTO"],
    })


def _historico():
    return [
        {"data": datetime.datetime(2026, 2, 22), "tipo": "saida",
         "produto": "BANANA NANICA", "quant": 52.0},
        {"data": datetime.datetime(2026, 2, 23), "tipo": "entrada",
         "produto": "BANANA PRATA",  "quant": 130.0},
    ]


# ---------------------------------------------------------------------------
# Testes: _norm_key
# ---------------------------------------------------------------------------

def test_norm_key_maiuscula():
    assert _norm_key("banana nanica") == "BANANA NANICA"

def test_norm_key_acento():
    assert _norm_key("Pimentão") == "PIMENTAO"

def test_norm_key_espacos_duplos():
    assert _norm_key("TOMATE  ITALIANO") == "TOMATE ITALIANO"

def test_norm_key_none():
    assert _norm_key(None) is None




# ---------------------------------------------------------------------------
# Testes: _parse_preco_raw
# ---------------------------------------------------------------------------

def test_parse_preco_raw_milhar_decimal_brasileiro():
    from data_pipeline import _parse_preco_raw
    assert _parse_preco_raw("R$ 1.234,56") == 1234.56

def test_parse_preco_raw_decimal_com_ponto():
    from data_pipeline import _parse_preco_raw
    assert _parse_preco_raw("4.89") == 4.89

# ---------------------------------------------------------------------------
# Testes: _ultima_data_do_dict
# ---------------------------------------------------------------------------

def test_ultima_data_retorna_mais_recente():
    d = {"22-02-2026": "a", "23-02-2026": "b", "21-02-2026": "c"}
    assert _ultima_data_do_dict(d) == "23-02-2026"

def test_ultima_data_uma_chave():
    d = {"01-01-2026": "x"}
    assert _ultima_data_do_dict(d) == "01-01-2026"

def test_ultima_data_fallback_alfabetico():
    # chaves não parsáveis como data → ordenação alfabética
    d = {"abc": 1, "xyz": 2, "mno": 3}
    assert _ultima_data_do_dict(d) == "xyz"


# ---------------------------------------------------------------------------
# Testes: estruturar_precos
# ---------------------------------------------------------------------------

def test_estruturar_precos_retorna_dict():
    resultado = estruturar_precos(_df_precos())
    assert isinstance(resultado, dict)

def test_estruturar_precos_banana_nanica():
    resultado = estruturar_precos(_df_precos())
    assert "BANANA NANICA" in resultado
    assert resultado["BANANA NANICA"]["Semar"] == 3.49
    assert resultado["BANANA NANICA"]["Rossi"] == 3.59

def test_estruturar_precos_ignora_indisponivel():
    # BANANA DA TERRA: Rossi está Indisponível — não deve aparecer
    resultado = estruturar_precos(_df_precos())
    assert "BANANA DA TERRA" in resultado
    assert "Rossi" not in resultado["BANANA DA TERRA"]

def test_estruturar_precos_sem_nan():
    resultado = estruturar_precos(_df_precos())
    for produto, lojas in resultado.items():
        for loja, preco in lojas.items():
            assert preco == preco, f"NaN encontrado em {produto}/{loja}"  # NaN != NaN

def test_estruturar_precos_df_vazio():
    assert estruturar_precos(pd.DataFrame()) == {}

def test_estruturar_precos_df_none():
    assert estruturar_precos(None) == {}


# ---------------------------------------------------------------------------
# Testes: estruturar_metas
# ---------------------------------------------------------------------------

def test_estruturar_metas_retorna_dict():
    resultado = estruturar_metas(_df_metas())
    assert isinstance(resultado, dict)

def test_estruturar_metas_valores():
    resultado = estruturar_metas(_df_metas())
    assert resultado["BANANA NANICA"] == 500
    assert resultado["TOMATE"] == 1000

def test_estruturar_metas_df_vazio():
    assert estruturar_metas(pd.DataFrame()) == {}


# ---------------------------------------------------------------------------
# Testes: montar_dados_para_llm
# ---------------------------------------------------------------------------

def _montar_mock():
    precos_dict = {"23-02-2026": _df_precos(), "22-02-2026": _df_precos()}
    return montar_dados_para_llm(
        precos_dict=precos_dict,
        metas_df=_df_metas(),
        progresso_df=_df_progresso(),
        saldo_estoque=248.0,
        historico_estoque=_historico(),
    )


def test_montar_retorna_dict():
    assert isinstance(_montar_mock(), dict)

def test_montar_json_serializavel():
    dados = _montar_mock()
    # Não deve lançar exceção
    serializado = json.dumps(dados, ensure_ascii=False)
    assert isinstance(serializado, str)

def test_montar_sem_dataframe():
    dados = _montar_mock()
    def checar_sem_df(obj, caminho=""):
        assert not isinstance(obj, pd.DataFrame), f"DataFrame encontrado em: {caminho}"
        if isinstance(obj, dict):
            for k, v in obj.items():
                checar_sem_df(v, f"{caminho}.{k}")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                checar_sem_df(v, f"{caminho}[{i}]")
    checar_sem_df(dados)

def test_montar_sem_nan():
    import math
    dados = _montar_mock()
    # Verifica que nenhum valor float é NaN (não busca substring em strings)
    def _checar_nan(obj, caminho=""):
        if isinstance(obj, float) and math.isnan(obj):
            raise AssertionError(f"NaN float encontrado em: {caminho}")
        elif isinstance(obj, dict):
            for k, v in obj.items():
                _checar_nan(v, f"{caminho}.{k}")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                _checar_nan(v, f"{caminho}[{i}]")
    _checar_nan(dados)
    # json.dumps com allow_nan=False lança ValueError se houver NaN/Inf
    json.dumps(dados, ensure_ascii=False, allow_nan=False)

def test_montar_usa_ultima_data():
    precos_dict = {"23-02-2026": _df_precos(), "20-02-2026": _df_precos()}
    dados = montar_dados_para_llm(precos_dict, _df_metas(), _df_progresso(), 0.0, [])
    assert dados["data_referencia"] == "23-02-2026"

def test_montar_estoque_float():
    dados = _montar_mock()
    assert isinstance(dados["estoque_banana_kg"], float)

def test_montar_precos_dict_vazio():
    dados = montar_dados_para_llm({}, _df_metas(), _df_progresso(), 0.0, [])
    assert dados["precos"] == {}
    assert dados["data_referencia"] is None

def test_montar_historico_limitado():
    # historico_estoque_ultimos deve ter no máximo 5 itens
    hist_longo = _historico() * 10  # 20 itens
    dados = montar_dados_para_llm(
        {"23-02-2026": _df_precos()}, _df_metas(), _df_progresso(), 0.0, hist_longo
    )
    assert len(dados["historico_estoque_ultimos"]) <= 5


# ---------------------------------------------------------------------------
# Runner manual (sem pytest)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    testes = [
        test_norm_key_maiuscula,
        test_norm_key_acento,
        test_norm_key_espacos_duplos,
        test_norm_key_none,
        test_ultima_data_retorna_mais_recente,
        test_ultima_data_uma_chave,
        test_ultima_data_fallback_alfabetico,
        test_estruturar_precos_retorna_dict,
        test_estruturar_precos_banana_nanica,
        test_estruturar_precos_ignora_indisponivel,
        test_estruturar_precos_sem_nan,
        test_estruturar_precos_df_vazio,
        test_estruturar_precos_df_none,
        test_estruturar_metas_retorna_dict,
        test_estruturar_metas_valores,
        test_estruturar_metas_df_vazio,
        test_montar_retorna_dict,
        test_montar_json_serializavel,
        test_montar_sem_dataframe,
        test_montar_sem_nan,
        test_montar_usa_ultima_data,
        test_montar_estoque_float,
        test_montar_precos_dict_vazio,
        test_montar_historico_limitado,
    ]

    passou = 0
    falhou = 0
    for t in testes:
        try:
            t()
            print(f"  OK  {t.__name__}")
            passou += 1
        except Exception as e:
            print(f"  FALHOU  {t.__name__}: {e}")
            falhou += 1

    print(f"\n{passou} passou | {falhou} falhou")
