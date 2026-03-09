"""
pages/3_Registro_Caixas.py
Página isolada para as lojas registrarem suas caixas.

Acesso via: http://localhost:8501/3_Registro_Caixas
NÃO acessa session_state do dashboard. Leitura/escrita apenas em caixas_lojas.json.
"""

import os
import sys
from datetime import datetime

import streamlit as st

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from data_processor import salvar_registro_caixas, load_registros_caixas, _DEFAULT_CAIXAS_JSON

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Registro de Caixas — Benverde",
    page_icon="📦",
    layout="centered",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    [data-testid='stSidebarNav'] { display: none !important; }
    [data-testid='stSidebarCollapsedControl'] { display: none !important; }
    section[data-testid='stSidebar'] { display: none !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

    .page-header {
        background: linear-gradient(135deg, #1a4731 0%, #2d7a4f 100%);
        padding: 1.2rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
    }
    .page-header h1 { color: white; font-size: 1.5rem; font-weight: 700; margin: 0; }
    .page-header p  { color: rgba(255,255,255,0.8); margin: 0; font-size: 0.88rem; }

    .stButton > button {
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.9rem;
    }
    .btn-registrar > button {
        background-color: #2d7a4f !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Constantes — lojas
# ---------------------------------------------------------------------------
_LOJAS = [
    (1,  "SUZANO"),
    (4,  "SÃO PAULO"),
    (5,  "GUAIANAZES"),
    (6,  "MAUA"),
    (7,  "MOGI DAS CRUZES"),
    (8,  "MOGI DAS CRUZES"),
    (10, "TAUBATE"),
    (11, "PINDAMONHANGABA"),
    (12, "SÃO SEBASTIÃO"),
    (13, "CARAGUATATUBA"),
    (14, "UBATUBA"),
    (16, "PINDAMONHANGABA"),
    (17, "POÁ"),
    (18, "TAUBATE"),
    (19, "NOVA LORENA"),
    (20, "GUARATINGUETA"),
    (21, "BERTIOGA"),
    (22, "MOGI DAS CRUZES"),
    (23, "FERRAZ DE VASCONCELOS"),
    (25, "SÃO SEBASTIÃO"),
    (26, "UBATUBA"),
    (27, "SUZANO"),
    (29, "ARUJA"),
    (30, "SÃO JOSÉ DOS CAMPOS"),
    (31, "SUZANO"),
    (32, "ITAQUAQUECETUBA"),
    (33, "ITAQUAQUECETUBA"),
]

_OPCOES_LOJAS = [f"Loja {n} — {cidade}" for n, cidade in _LOJAS]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="page-header">
  <h1>📦 Registro de Caixas</h1>
  <p>Benverde Hortifrúti · Informe as caixas da sua loja</p>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Selectbox de loja
# ---------------------------------------------------------------------------
loja_selecionada = st.selectbox(
    "🏪 Selecione sua loja",
    options=_OPCOES_LOJAS,
    key="loja_sel",
)

idx_loja = _OPCOES_LOJAS.index(loja_selecionada)
n_loja, nome_loja = _LOJAS[idx_loja]

# ---------------------------------------------------------------------------
# Formulário
# ---------------------------------------------------------------------------
st.markdown(f"#### 📦 Registro de Caixas — {nome_loja}")

with st.form("form_caixas", clear_on_submit=True):
    caixas_benverde = st.number_input("Caixas Benverde", min_value=0, step=1, value=0)
    caixas_bananas  = st.number_input("Caixas Bananas",  min_value=0, step=1, value=0)
    caixas_ccj      = st.number_input("Caixas CCJ",      min_value=0, step=1, value=0)

    st.markdown("↳ **Distribuição das Caixas CCJ:**")
    ccj_col1, ccj_col2, ccj_col3 = st.columns(3)
    ccj_banca      = ccj_col1.number_input("Caixas na banca",       min_value=0, step=1, value=0)
    ccj_mercadoria = ccj_col2.number_input("Caixas c/ mercadoria",  min_value=0, step=1, value=0)
    ccj_retirada   = ccj_col3.number_input("Caixas p/ retirada",    min_value=0, step=1, value=0)

    entregue = st.radio("Entregue?", options=["sim", "não"], horizontal=True)

    st.markdown('<div class="btn-registrar">', unsafe_allow_html=True)
    submitted = st.form_submit_button("💾 Registrar", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

if submitted:
    soma_ccj = int(ccj_banca) + int(ccj_mercadoria) + int(ccj_retirada)
    if soma_ccj > int(caixas_ccj):
        st.error(
            f"❌ A soma das sub-categorias CCJ ({soma_ccj}) não pode ser maior "
            f"que o total de Caixas CCJ ({int(caixas_ccj)}). Corrija e registre novamente."
        )
    else:
        total = int(caixas_benverde) + int(caixas_ccj) + int(caixas_bananas)
        registro = {
            "data":            datetime.now().date().isoformat(),
            "loja":            nome_loja,
            "n_loja":          int(n_loja),
            "caixas_benverde": int(caixas_benverde),
            "caixas_ccj":      int(caixas_ccj),
            "ccj_banca":       int(ccj_banca),
            "ccj_mercadoria":  int(ccj_mercadoria),
            "ccj_retirada":    int(ccj_retirada),
            "caixas_bananas":  int(caixas_bananas),
            "total":           total,
            "entregue":        entregue,
        }
        try:
            salvar_registro_caixas(registro, _DEFAULT_CAIXAS_JSON)
            st.success("✅ Registro salvo!")
            st.rerun()
        except Exception as exc:
            st.error(f"❌ Erro ao salvar: {exc}")

# ---------------------------------------------------------------------------
# Histórico da loja (últimos 10 registros)
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(f"#### 📋 Últimos registros — {nome_loja}")

try:
    df_todos = load_registros_caixas(_DEFAULT_CAIXAS_JSON)
    if not df_todos.empty:
        df_loja = df_todos[df_todos["n_loja"] == n_loja].copy()
        df_loja = df_loja.sort_values("data", ascending=False).head(10)
        if not df_loja.empty:
            df_loja["data"] = df_loja["data"].apply(
                lambda d: datetime.strptime(str(d), "%Y-%m-%d").strftime("%d/%m/%Y")
                if len(str(d)) == 10 else str(d)
            )
            df_loja = df_loja.rename(columns={
                "data": "Data", "loja": "Loja", "n_loja": "Nº",
                "caixas_benverde": "Benverde",
                "caixas_ccj": "CCJ", "ccj_banca": "CCJ Banca",
                "ccj_mercadoria": "CCJ Mercadoria", "ccj_retirada": "CCJ Retirada",
                "caixas_bananas": "Bananas", "total": "Total", "entregue": "Entregue?",
            })
            st.dataframe(df_loja, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum registro encontrado para esta loja.")
    else:
        st.info("Nenhum registro encontrado ainda.")
except Exception as exc:
    st.warning(f"Não foi possível carregar histórico: {exc}")
