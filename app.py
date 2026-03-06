"""
app.py — ponto de entrada do Mita IA.

Redireciona automaticamente para o dashboard principal (pages/1_Dashboard.py).
Não contém dados, lógica de negócio nem configurações sensíveis.
"""

import streamlit as st

st.set_page_config(
    page_title="Mita IA",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Esconde navegação automática entre páginas
st.markdown(
    "<style>[data-testid='stSidebarNav']{display:none}</style>",
    unsafe_allow_html=True,
)

if __name__ != "__mp_main__":
    st.switch_page("pages/bv_9m4k2r.py")
