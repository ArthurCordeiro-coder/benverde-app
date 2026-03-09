"""
app.py
Aplicativo Streamlit para gerenciamento da empresa hortifrúti Benverde.

Integra data_processor.py (carregamento de dados) e claude_chat.py
(chat com IA) em uma interface com 4 abas:
    1. Chat IA          — conversa com Grok sobre os dados da empresa.
    2. Metas e Vendas   — progresso de metas, pedidos filtráveis.
    3. Estoque Bananas  — saldo, movimentações, gráfico entradas/saídas.
    4. Preços           — pesquisa de preços nos supermercados por data.

Uso:
    streamlit run app.py
"""

import io
import os
import logging
import re
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

from data_processor import (
    load_precos,
    calcular_estoque,
    load_pedidos_pdfs,
    load_metas_local,
    salvar_metas_local,
    load_movimentacoes_manuais,
    load_registros_caixas,
    _DEFAULT_CAIXAS_JSON,
    _worker_pedido,
    extrair_pedido_semar,
)
from claude_chat import chat_com_grok, chat_com_grok_historico, extrair_metas_de_imagem, extrair_metas_de_planilha
from auth import (
    verificar_login,
    registrar_usuario,
    get_user,
    carregar_pending,
    aprovar_usuario,
    rejeitar_usuario,
)


# ---------------------------------------------------------------------------
# Temas — Liquid Glass (escuro / claro)
# ---------------------------------------------------------------------------

_TEMA_DARK = {
    # Fundo e glass
    "app_bg":         "radial-gradient(ellipse at 25% 15%, #1a4731 0%, #0d2518 55%, #060e09 100%)",
    "glass_bg":       "rgba(255,255,255,0.07)",
    "glass_border":   "rgba(255,255,255,0.15)",
    "sidebar_bg":     "rgba(13,37,24,0.75)",
    # Texto
    "texto":          "rgba(255,255,255,0.85)",
    "texto_suave":    "rgba(255,255,255,0.50)",
    "texto_th":       "rgba(255,255,255,0.70)",
    # Tabelas HTML
    "tabela_borda":   "rgba(255,255,255,0.10)",
    "th_bg":          "rgba(255,255,255,0.06)",
    "tr_par":         "rgba(255,255,255,0.04)",
    "tr_impar":       "rgba(255,255,255,0.02)",
    "thead_caixas":   "rgba(45,122,79,0.55)",
    "banana_row":     "rgba(245,200,66,0.10)",
    # Gráficos Plotly
    "plot_font":      "rgba(255,255,255,0.75)",
    "gridcolor":      "rgba(255,255,255,0.08)",
    "zeroline":       "rgba(255,255,255,0.12)",
    "cell_color":     ["rgba(255,255,255,0.04)", "rgba(255,255,255,0.02)"],
    "cell_font":      "rgba(255,255,255,0.80)",
    "header_table":   "rgba(45,122,79,0.60)",
    "paper_bgcolor":  "rgba(0,0,0,0)",
    "plot_bgcolor":   "rgba(0,0,0,0)",
    # Plotly go.Table papel sólido (para exportação)
    "table_paper":    "rgba(13,37,24,0.95)",
    "table_title":    "#4caf7d",
    # Badges inline (caixas)
    "badge_sim_bg":   "rgba(16,185,129,0.20)",
    "badge_sim_fg":   "#6ee7b7",
    "badge_sim_bd":   "rgba(16,185,129,0.30)",
    "badge_nao_bg":   "rgba(239,68,68,0.15)",
    "badge_nao_fg":   "#fca5a5",
    "badge_nao_bd":   "rgba(239,68,68,0.30)",
    # Toggle label
    "toggle_label":   "☀️ Modo claro",
    "icone":          "🌙",
    # Dialog
    "dialog_bg":      "rgba(10,28,18,0.97)",
}

_TEMA_LIGHT = {
    # Fundo e glass
    "app_bg":         "radial-gradient(ellipse at 25% 15%, #e8f5ee 0%, #c8e6d4 55%, #f0faf4 100%)",
    "glass_bg":       "rgba(255,255,255,0.55)",
    "glass_border":   "rgba(26,71,49,0.15)",
    "sidebar_bg":     "rgba(232,245,238,0.80)",
    # Texto
    "texto":          "#1a4731",
    "texto_suave":    "#4b7a62",
    "texto_th":       "#1a4731",
    # Tabelas HTML
    "tabela_borda":   "rgba(26,71,49,0.15)",
    "th_bg":          "rgba(26,71,49,0.07)",
    "tr_par":         "rgba(255,255,255,0.70)",
    "tr_impar":       "rgba(232,245,238,0.50)",
    "thead_caixas":   "#1a4731",
    "banana_row":     "rgba(245,200,66,0.18)",
    # Gráficos Plotly
    "plot_font":      "#1a4731",
    "gridcolor":      "rgba(26,71,49,0.10)",
    "zeroline":       "rgba(26,71,49,0.18)",
    "cell_color":     ["rgba(255,255,255,0.80)", "rgba(232,245,238,0.60)"],
    "cell_font":      "#1a4731",
    "header_table":   "rgba(26,71,49,0.75)",
    "paper_bgcolor":  "rgba(0,0,0,0)",
    "plot_bgcolor":   "rgba(0,0,0,0)",
    # Plotly go.Table papel sólido (para exportação)
    "table_paper":    "white",
    "table_title":    "#1a4731",
    # Badges inline (caixas)
    "badge_sim_bg":   "#d1fae5",
    "badge_sim_fg":   "#065f46",
    "badge_sim_bd":   "rgba(16,185,129,0.40)",
    "badge_nao_bg":   "#fee2e2",
    "badge_nao_fg":   "#991b1b",
    "badge_nao_bd":   "rgba(239,68,68,0.40)",
    # Toggle label
    "toggle_label":   "🌙 Modo escuro",
    "icone":          "☀️",
    # Dialog
    "dialog_bg":      "rgba(235,248,240,0.98)",
}


def _get_tema() -> dict:
    """Retorna o dicionário de cores do tema ativo."""
    return _TEMA_DARK if st.session_state.get("tema_escuro", True) else _TEMA_LIGHT


def _plotly_base(t: dict) -> dict:
    """Kwargs base para fig.update_layout()."""
    return dict(
        paper_bgcolor=t["paper_bgcolor"],
        plot_bgcolor=t["plot_bgcolor"],
        font=dict(color=t["plot_font"], family="DM Sans"),
    )


def _plotly_axes(t: dict) -> dict:
    """Kwargs base para update_xaxes/update_yaxes."""
    return dict(
        gridcolor=t["gridcolor"],
        zerolinecolor=t["zeroline"],
    )


def _render_css_tema() -> None:
    """Aplica CSS dinâmico com base no tema ativo (escuro / claro)."""
    t = _get_tema()
    st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {{
        font-family: 'DM Sans', sans-serif;
    }}

    :root {{
        --verde-escuro:  #1a4731;
        --verde-medio:   #2d7a4f;
        --verde-claro:   #4caf7d;
        --amarelo:       #f5c842;
        --laranja:       #e8843a;
        --glass-bg:      {t['glass_bg']};
        --glass-border:  {t['glass_border']};
        --texto:         {t['texto']};
        --texto-suave:   {t['texto_suave']};
    }}

    .stApp {{
        background: {t['app_bg']} !important;
        background-attachment: fixed !important;
    }}

    [data-testid="stHeader"] {{
        background: transparent !important;
    }}

    [data-testid="stSidebar"] {{
        background: {t['sidebar_bg']} !important;
        backdrop-filter: blur(20px) saturate(160%) !important;
        -webkit-backdrop-filter: blur(20px) saturate(160%) !important;
        border-right: 1px solid var(--glass-border) !important;
    }}
    [data-testid="stSidebar"] *:not(button):not(input):not(select) {{
        color: {t['texto']} !important;
    }}
    [data-testid="stSidebar"] .stButton > button {{
        background: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        color: {t['texto']} !important;
    }}
    [data-testid="stSidebar"] .stButton > button:hover {{
        background: rgba(76,175,125,0.2) !important;
        border-color: #4caf7d !important;
    }}

    .block-container {{ padding-top: 1.5rem; }}

    .app-header {{
        background: var(--glass-bg);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid var(--glass-border);
        padding: 1.5rem 2rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        display: flex;
        align-items: center;
        gap: 1rem;
        box-shadow: 0 4px 24px rgba(0,0,0,0.15),
                    inset 0 1px 0 rgba(255,255,255,0.1);
    }}
    .app-header h1 {{ color: {t['texto']}; font-size: 1.75rem; font-weight: 700; margin: 0; }}
    .app-header p  {{ color: {t['texto_suave']}; margin: 0; font-size: 0.9rem; }}

    .metric-card {{
        background: var(--glass-bg);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid var(--glass-border);
        border-radius: 14px;
        padding: 1.2rem 1.4rem;
        box-shadow: 0 4px 16px rgba(0,0,0,0.1),
                    inset 0 1px 0 rgba(255,255,255,0.08);
    }}
    .metric-card .label {{ color: {t['texto_suave']}; font-size: 0.78rem;
        font-weight: 500; text-transform: uppercase; letter-spacing: 0.06em; }}
    .metric-card .value {{ color: #4caf7d; font-size: 2rem; font-weight: 700; }}
    .metric-card .delta {{ color: {t['texto_suave']}; font-size: 0.82rem; }}
    .metric-card .delta.pos {{ color: #4caf7d; }}
    .metric-card .delta.neg {{ color: var(--laranja); }}

    .progress-wrap {{
        background: {t['glass_border']};
        border-radius: 99px;
        height: 10px;
        overflow: hidden;
    }}
    .progress-bar {{ height: 10px; border-radius: 99px; transition: width 0.4s ease; }}

    .badge {{ display: inline-block; padding: 0.2rem 0.65rem;
              border-radius: 99px; font-size: 0.72rem; font-weight: 500;
              letter-spacing: 0.04em; text-transform: uppercase; }}
    .badge-green  {{ background: rgba(16,185,129,0.20); color: #6ee7b7;
                     border: 1px solid rgba(16,185,129,0.30); }}
    .badge-yellow {{ background: rgba(245,200,66,0.15); color: #d97706;
                     border: 1px solid rgba(245,200,66,0.30); }}
    .badge-red    {{ background: rgba(239,68,68,0.15); color: #fca5a5;
                     border: 1px solid rgba(239,68,68,0.30); }}
    .badge-blue   {{ background: rgba(99,102,241,0.15); color: #c7d2fe;
                     border: 1px solid rgba(99,102,241,0.30); }}
    .banana-row   {{ background: {t['banana_row']} !important; }}

    .sidebar-status {{
        background: rgba(76,175,125,0.12);
        border-left: 3px solid #4caf7d;
        padding: 0.75rem 1rem;
        border-radius: 0 8px 8px 0;
        font-size: 0.82rem;
        color: {t['texto']};
        margin-bottom: 0.75rem;
    }}
    .sidebar-status strong {{ color: #4caf7d; }}

    .chat-container {{ max-height: 520px; overflow-y: auto; padding: 0.5rem; }}
    [data-testid="stChatMessage"] p,
    [data-testid="stChatMessage"] li,
    [data-testid="stChatMessage"] span {{
        font-size: 1.05rem !important; line-height: 1.75 !important;
    }}
    [data-testid="stChatMessage"] strong {{
        font-size: 1.05rem !important; color: #4caf7d;
    }}
    [data-testid="stChatMessage"] ul,
    [data-testid="stChatMessage"] ol {{
        padding-left: 1.4rem; margin: 0.5rem 0;
    }}
    [data-testid="stChatMessage"] li {{ margin-bottom: 0.35rem; }}
    [data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) {{
        background: rgba(76,175,125,0.10);
        border: 1px solid rgba(76,175,125,0.20);
        border-radius: 12px; padding: 0.5rem 0.75rem; margin-bottom: 0.5rem;
    }}

    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px; background: var(--glass-bg);
        padding: 4px; border-radius: 12px;
        border: 1px solid var(--glass-border);
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius: 10px; padding: 0.45rem 1.1rem;
        font-weight: 500; font-size: 0.88rem;
        color: {t['texto_suave']} !important;
        background: transparent !important;
    }}
    .stTabs [aria-selected="true"] {{
        background: rgba(76,175,125,0.20) !important;
        color: #4caf7d !important;
        box-shadow: 0 1px 4px rgba(0,0,0,0.15);
    }}
    .stTabs [data-baseweb="tab-panel"] {{ background: transparent !important; }}

    .stButton > button {{
        border-radius: 12px; font-weight: 500; font-size: 0.88rem;
        background: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        color: {t['texto']} !important;
        transition: all 0.2s ease !important;
    }}
    .stButton > button:hover {{
        background: rgba(255,255,255,0.13) !important;
        border-color: rgba(76,175,125,0.4) !important;
        transform: translateY(-1px);
    }}
    .stButton > button[kind="primary"] {{
        background: #2d7a4f !important;
        border-color: #2d7a4f !important;
        color: white !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        background: #4caf7d !important;
        border-color: #4caf7d !important;
    }}

    .stTextInput > div > div > input,
    .stTextInput input,
    .stSelectbox > div > div,
    .stMultiSelect > div > div {{
        background: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        border-radius: 10px !important;
        color: {t['texto']} !important;
        caret-color: {t['texto']} !important;
    }}
    .stTextInput input::placeholder {{ color: {t['texto_suave']} !important; }}
    .stTextInput input:focus {{
        border-color: #4caf7d !important;
        box-shadow: 0 0 0 2px rgba(76,175,125,0.2) !important;
    }}

    div[data-testid="stMetricValue"] {{
        font-family: 'DM Mono', monospace;
        font-size: 1.8rem !important;
        color: #4caf7d !important;
    }}
    div[data-testid="stMetric"] {{
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        border-radius: 14px; padding: 1rem 1.2rem;
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
    }}

    [data-testid="stExpander"] {{
        background: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        border-radius: 12px !important;
        backdrop-filter: blur(12px);
    }}

    [data-testid="stAlert"] {{
        border-radius: 12px !important;
        backdrop-filter: blur(8px);
    }}

    div[data-testid="stDialog"] > div > div {{
        max-width: 900px; width: 90vw;
        background: {t['dialog_bg']} !important;
        backdrop-filter: blur(24px) !important;
        border: 1px solid var(--glass-border) !important;
        border-radius: 20px !important;
    }}
    /* Força legibilidade de todo o conteúdo do dialog */
    div[data-testid="stDialog"] p,
    div[data-testid="stDialog"] span,
    div[data-testid="stDialog"] label,
    div[data-testid="stDialog"] h1,
    div[data-testid="stDialog"] h2,
    div[data-testid="stDialog"] h3,
    div[data-testid="stDialog"] h4,
    div[data-testid="stDialog"] h5,
    div[data-testid="stDialog"] small,
    div[data-testid="stDialog"] [data-testid="stMarkdownContainer"] * {{
        color: {t['texto']} !important;
    }}
    div[data-testid="stDialog"] [data-testid="stCaptionContainer"] * {{
        color: {t['texto_suave']} !important;
    }}
    div[data-testid="stDialog"] input,
    div[data-testid="stDialog"] textarea {{
        background: {t['glass_bg']} !important;
        color: {t['texto']} !important;
        border-color: var(--glass-border) !important;
    }}
    div[data-testid="stDialog"] .stButton > button {{
        color: {t['texto']} !important;
    }}

    /* Força cor de texto geral conforme tema do app */
    .stApp, .stApp p, .stApp span, .stApp label,
    .stApp h1, .stApp h2, .stApp h3, .stApp h4,
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] span {{
        color: {t['texto']} !important;
    }}

    /* Fundo dos widgets nativos */
    [data-testid="stMetric"],
    [data-testid="stExpander"],
    .stSelectbox > div > div,
    .stMultiSelect > div > div {{
        background: var(--glass-bg) !important;
        border-color: var(--glass-border) !important;
    }}

    /* Texto dentro dos widgets nativos */
    [data-testid="stMetricLabel"],
    [data-testid="stMetricDelta"] {{
        color: {t['texto_suave']} !important;
    }}

    /* Botões da sidebar — texto sempre branco
       independente do tema (fundo é sempre escuro) */
    [data-testid="stSidebar"] .stButton > button,
    [data-testid="stSidebar"] .stButton > button * {{
        color: white !important;
    }}
</style>
""", unsafe_allow_html=True)



# ---------------------------------------------------------------------------
# Configuração global
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Mita IA",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)
# Esconde navegação automática entre páginas (multipage)
st.markdown(
    "<style>[data-testid='stSidebarNav']{display:none}</style>",
    unsafe_allow_html=True,
)
# ---------------------------------------------------------------------------
# CSS customizado — tema Liquid Glass + Benverde
# ---------------------------------------------------------------------------
# CSS dinâmico: _render_css_tema() é chamada em main()


# ---------------------------------------------------------------------------
# Tela de login — Liquid Glass
# ---------------------------------------------------------------------------

def _render_pagina_login() -> None:
    """Renderiza a tela de login/cadastro com visual Liquid Glass + Benverde."""
    st.markdown("""
    <style>
    /* 1. FUNDO GERAL — gradiente verde escuro radial em toda a página */
    .stApp {
        background: radial-gradient(ellipse at 30% 20%,
            #1a4731 0%, #0d2518 60%, #070f0a 100%) !important;
        background-attachment: fixed !important;
    }

    /* 2. REMOVER fundo branco/cinza do bloco principal */
    .stApp > header,
    [data-testid="stHeader"] {
        background: transparent !important;
    }

    /* 3. CONTAINER CENTRAL — simula o card glass
       O .block-container é o wrapper real dos widgets no Streamlit */
    .block-container {
        max-width: 420px !important;
        margin: 6vh auto !important;
        padding: 2.5rem 2rem !important;
        background: rgba(255, 255, 255, 0.07) !important;
        backdrop-filter: blur(24px) saturate(180%) !important;
        -webkit-backdrop-filter: blur(24px) saturate(180%) !important;
        border: 1px solid rgba(255, 255, 255, 0.15) !important;
        border-radius: 24px !important;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.5),
                    inset 0 1px 0 rgba(255, 255, 255, 0.12) !important;
    }

    /* 4. INPUTS — fundo escuro translúcido, texto branco */
    .stTextInput > div > div > input,
    .stTextInput input {
        background: rgba(255, 255, 255, 0.08) !important;
        border: 1px solid rgba(255, 255, 255, 0.2) !important;
        border-radius: 12px !important;
        color: white !important;
        caret-color: white !important;
        padding: 0.65rem 1rem !important;
    }
    .stTextInput > div > div > input::placeholder,
    .stTextInput input::placeholder {
        color: rgba(255, 255, 255, 0.35) !important;
    }
    .stTextInput > div > div > input:focus,
    .stTextInput input:focus {
        border-color: #4caf7d !important;
        box-shadow: 0 0 0 2px rgba(76, 175, 125, 0.25) !important;
        background: rgba(255, 255, 255, 0.12) !important;
    }

    /* Ícone do olho (mostrar senha) */
    .stTextInput [data-testid="stTextInputRootElement"] button,
    .stTextInput button {
        color: rgba(255, 255, 255, 0.5) !important;
        background: transparent !important;
    }

    /* 5. LABELS dos inputs */
    .stTextInput label,
    .stTextInput > label {
        color: rgba(255, 255, 255, 0.75) !important;
        font-size: 0.82rem !important;
        font-weight: 500 !important;
        letter-spacing: 0.04em !important;
    }

    /* 6. BOTÃO PRIMÁRIO — verde Benverde */
    .stButton > button[kind="primary"],
    .stButton > button {
        background: #2d7a4f !important;
        border: none !important;
        border-radius: 12px !important;
        color: white !important;
        font-weight: 600 !important;
        font-size: 0.95rem !important;
        padding: 0.65rem 1.5rem !important;
        width: 100% !important;
        transition: background 0.2s ease, transform 0.1s ease !important;
    }
    .stButton > button:hover {
        background: #4caf7d !important;
        transform: translateY(-1px) !important;
    }
    .stButton > button:active {
        transform: translateY(0) !important;
    }

    /* 7. TABS — transparente com acento verde */
    .stTabs [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid rgba(255, 255, 255, 0.12) !important;
        gap: 0.5rem !important;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent !important;
        color: rgba(255, 255, 255, 0.5) !important;
        font-weight: 500 !important;
        border-radius: 8px 8px 0 0 !important;
        padding: 0.5rem 1.25rem !important;
        border: none !important;
    }
    .stTabs [aria-selected="true"] {
        background: rgba(76, 175, 125, 0.15) !important;
        color: #4caf7d !important;
        border-bottom: 2px solid #4caf7d !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        background: transparent !important;
        padding-top: 1.25rem !important;
    }

    /* 8. ALERTAS — erro com tom avermelhado translúcido */
    .stAlert[data-baseweb="notification"] {
        background: rgba(239, 68, 68, 0.12) !important;
        border: 1px solid rgba(239, 68, 68, 0.35) !important;
        border-radius: 10px !important;
        color: #fca5a5 !important;
    }

    /* 9. CAPTION / texto pequeno */
    .stCaption, [data-testid="stCaptionContainer"] {
        color: rgba(255, 255, 255, 0.4) !important;
        font-size: 0.78rem !important;
    }

    /* 10. ESCONDE toolbar, menu hamburguer e rodapé do Streamlit */
    [data-testid="stToolbar"],
    [data-testid="stDecoration"],
    #MainMenu,
    footer {
        display: none !important;
    }

    /* 11. Força fundo escuro nos inputs sobrescrevendo estilos inline */
    .stTextInput > div > div,
    .stTextInput > div > div > div,
    [data-testid="stTextInputRootElement"],
    [data-testid="stTextInputRootElement"] > div {
        background: rgba(255, 255, 255, 0.08) !important;
        border-radius: 12px !important;
    }

    /* Sobrescreve o fundo branco injetado inline pelo tema light */
    .stTextInput input[style],
    .stTextInput input {
        background-color: transparent !important;
        color: white !important;
        caret-color: white !important;
    }

    /* Wrapper externo do input também precisa ser escuro */
    [data-testid="stTextInputRootElement"] {
        background: rgba(255, 255, 255, 0.08) !important;
        border: 1px solid rgba(255, 255, 255, 0.2) !important;
        border-radius: 12px !important;
    }
    [data-testid="stTextInputRootElement"]:focus-within {
        border-color: #4caf7d !important;
        box-shadow: 0 0 0 2px rgba(76, 175, 125, 0.25) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="text-align:center;margin-bottom:1.5rem">
        <div style="font-size:3rem;line-height:1.1">🌿</div>
        <h1 style="color:white;font-size:1.8rem;font-weight:700;margin:0.3rem 0 0.2rem">
            Mita
        </h1>
        <p style="color:rgba(255,255,255,0.6);font-size:0.9rem;margin:0">
            Gerenciamento Benverde
        </p>
    </div>
    """, unsafe_allow_html=True)

    aba_entrar, aba_cadastro = st.tabs(["Entrar", "Criar conta"])

    with aba_entrar:
        username_in = st.text_input(
            "Username", key="lg_user", placeholder="seu_usuario",
            label_visibility="visible",
        )
        senha_in = st.text_input(
            "Senha", type="password", key="lg_senha", placeholder="••••••",
        )
        if st.button("Entrar", type="primary", key="btn_entrar", use_container_width=True):
            if username_in and senha_in:
                ok, msg = verificar_login(username_in.strip(), senha_in)
                if ok:
                    st.session_state["autenticado"]     = True
                    st.session_state["username_logado"] = username_in.strip()
                    st.rerun()
                else:
                    st.error(msg)
            else:
                st.error("Preencha todos os campos.")
        st.caption("🔒 Recuperação de senha — em breve")

    with aba_cadastro:
        nome_in  = st.text_input("Nome completo", key="cad_nome", placeholder="Maria da Silva")
        user2_in = st.text_input("Username",      key="cad_user", placeholder="maria_silva")
        sen1_in  = st.text_input("Senha",          type="password", key="cad_sen1", placeholder="••••••")
        sen2_in  = st.text_input("Confirmar senha", type="password", key="cad_sen2", placeholder="••••••")
        if st.button("Solicitar acesso", type="primary", key="btn_cadastrar", use_container_width=True):
            if not (nome_in and user2_in and sen1_in and sen2_in):
                st.error("Preencha todos os campos.")
            elif sen1_in != sen2_in:
                st.error("As senhas não coincidem.")
            else:
                ok, msg = registrar_usuario(user2_in.strip(), nome_in.strip(), sen1_in)
                if ok and msg == "admin_criado":
                    st.success("Conta criada! Faça login.")
                elif ok and msg == "pendente":
                    st.info("Solicitação enviada! Aguarde aprovação do administrador.")
                else:
                    st.error(msg)


# ---------------------------------------------------------------------------
# Auth gate — bloqueia todo o resto até autenticação
# ---------------------------------------------------------------------------

if "autenticado" not in st.session_state:
    st.session_state["autenticado"]     = False
    st.session_state["username_logado"] = None

if not st.session_state["autenticado"]:
    _render_pagina_login()
    st.stop()


# ---------------------------------------------------------------------------
# Defaults de caminhos
# ---------------------------------------------------------------------------
_DEFAULT_CAIXAS_JSON     = "dados/cache/caixas_lojas.json"
_DEFAULT_PASTA_PRECOS    = "dados/precos"
_DEFAULT_PASTA_PEDIDOS   = "dados/pedidos_nfe"
_DEFAULT_PASTA_ENTRADAS  = "dados/entradas_bananas"
_DEFAULT_PASTA_SAIDAS    = "dados/saidas_bananas"
_DEFAULT_CACHE_PEDIDOS   = "dados/cache/cache_pedidos.json"
_DEFAULT_CACHE_ESTOQUE   = "dados/cache/cache_estoque.json"
_DEFAULT_METAS_LOCAL     = "dados/cache/metas_local.json"
_DEFAULT_ESTOQUE_MANUAL  = "dados/cache/estoque_manual.json"
_DEFAULT_PASTA_SEMAR     = "dados/pedidos_semar"
_DEFAULT_CACHE_SEMAR     = "dados/cache/cache_semar.json"


# ---------------------------------------------------------------------------
# Inicialização do session_state
# ---------------------------------------------------------------------------

def _init_state() -> None:
    """Inicializa todas as chaves do session_state com valores padrão."""
    defaults = {
        # Dados carregados
        "precos":             {},
        "progresso":          None,
        "pedidos":            None,
        "metas":              None,
        "saldo_estoque":      0.0,
        "historico_estoque":  [],
        # Metadados
        "ultima_atualizacao": None,
        "n_csvs":             0,
        "n_pdfs":             0,
        "erro_carregamento":  None,
        "dados_carregados":   False,
        # Chat
        "chat_historico":     [],   # exibição (mensagens originais)
        "chat_historico_api": [],   # API (com contexto de dados injetado)
        # Caminhos
        "path_precos":        _DEFAULT_PASTA_PRECOS,
        "path_pasta_pedidos": _DEFAULT_PASTA_PEDIDOS,
        "path_entradas":      _DEFAULT_PASTA_ENTRADAS,
        "path_saidas":        _DEFAULT_PASTA_SAIDAS,
        "path_cache_pedidos": _DEFAULT_CACHE_PEDIDOS,
        "path_cache_estoque":  _DEFAULT_CACHE_ESTOQUE,
        "path_metas_local":    _DEFAULT_METAS_LOCAL,
        "path_estoque_manual": _DEFAULT_ESTOQUE_MANUAL,
        "caixas_lojas":        pd.DataFrame(),
        "path_caixas_json":    _DEFAULT_CAIXAS_JSON,
        "path_pasta_semar":    _DEFAULT_PASTA_SEMAR,
        "path_cache_semar":    _DEFAULT_CACHE_SEMAR,
        # Tema
        "tema_escuro":         True,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ---------------------------------------------------------------------------
# Carregamento de dados
# ---------------------------------------------------------------------------

def carregar_dados() -> None:
    """Carrega (ou recarrega) todos os dados.

    Usa cache incremental para PDFs: somente arquivos novos são processados.
    """
    erros: list[str] = []

    # ---- Preços ----
    try:
        precos = load_precos(st.session_state["path_precos"])
        st.session_state["precos"] = precos
        st.session_state["n_csvs"] = len(precos)
        logger.info("Preços carregados: %d arquivo(s).", len(precos))
    except Exception as exc:
        erros.append(f"Preços: {exc}")
        logger.error("Falha ao carregar preços: %s", exc)

    # ---- Pedidos NF-e (PDFs) + Metas locais ----
    try:
        df_pedidos = load_pedidos_pdfs(
            st.session_state["path_pasta_pedidos"],
            st.session_state["path_cache_pedidos"],
        )
        df_metas = load_metas_local(st.session_state["path_metas_local"])

        # Pedidos Semar (mesmo formato, concatenar com NF-e)
        try:
            from data_processor import load_pedidos_semar
            df_semar = load_pedidos_semar(
                st.session_state["path_pasta_semar"],
                st.session_state["path_cache_semar"],
            )
            if not df_semar.empty:
                df_pedidos = pd.concat([df_pedidos, df_semar], ignore_index=True)
                logger.info("Pedidos Semar concatenados: %d linhas.", len(df_semar))
        except Exception as exc:
            logger.warning("Falha ao carregar pedidos Semar: %s", exc)

        st.session_state["pedidos"] = df_pedidos
        st.session_state["metas"]   = df_metas

        # Calcula progresso cruzando pedidos com metas
        if df_pedidos is not None and not df_pedidos.empty and \
           df_metas is not None and not df_metas.empty:
            st.session_state["progresso"] = _calcular_progresso(df_pedidos, df_metas)
        else:
            st.session_state["progresso"] = None

        logger.info("Pedidos NF-e: %d linhas | Metas: %d produto(s).",
                    len(df_pedidos), len(df_metas))
    except Exception as exc:
        erros.append(f"Pedidos/Metas: {exc}")
        logger.error("Falha ao carregar pedidos/metas: %s", exc)

    # ---- Estoque de bananas (com cache) ----
    try:
        saldo, hist = calcular_estoque(
            st.session_state["path_entradas"],
            st.session_state["path_saidas"],
            caminho_cache=st.session_state["path_cache_estoque"],
        )
        # Integra movimentações manuais registradas na página de estoque
        path_manual = st.session_state.get("path_estoque_manual", "estoque_manual.json")
        manuais = load_movimentacoes_manuais(path_manual)
        if manuais:
            from datetime import datetime as _dt
            for m in manuais:
                data_val = m.get("data")
                if isinstance(data_val, str):
                    try:
                        data_val = _dt.fromisoformat(data_val)
                    except Exception:
                        data_val = None
                hist.append({
                    "data":        data_val,
                    "tipo":        m.get("tipo", "entrada"),
                    "produto":     m.get("produto", ""),
                    "quant":       float(m.get("quant", 0.0)),
                    "unidade":     m.get("unidade", "KG"),
                    "valor_unit":  m.get("valor_unit", 0.0),
                    "valor_total": m.get("valor_total", 0.0),
                    "arquivo":     m.get("arquivo", "manual"),
                    "loja":        m.get("loja", ""),
                })
            saldo += sum(
                m["quant"] if m.get("tipo") == "entrada" else -m["quant"]
                for m in manuais
            )
        st.session_state["saldo_estoque"]     = saldo
        st.session_state["historico_estoque"] = hist
        st.session_state["n_pdfs"]            = len(hist)
        logger.info("Estoque calculado: saldo=%.2f kg, %d registros (%d manuais).",
                    saldo, len(hist), len(manuais))
    except Exception as exc:
        erros.append(f"Estoque: {exc}")
        logger.error("Falha ao calcular estoque: %s", exc)

    # ---- Caixas das lojas ----
    try:
        df_caixas = load_registros_caixas(st.session_state["path_caixas_json"])
        st.session_state["caixas_lojas"] = df_caixas
    except Exception as exc:
        erros.append(f"Caixas: {exc}")
        logger.error("Falha ao carregar caixas: %s", exc)

    st.session_state["ultima_atualizacao"] = datetime.now()
    st.session_state["dados_carregados"]   = len(erros) < 3
    st.session_state["erro_carregamento"]  = "; ".join(erros) if erros else None


def _calcular_progresso(df_pedidos: pd.DataFrame, df_metas: pd.DataFrame) -> pd.DataFrame:
    """Cruza pedidos com metas locais e calcula progresso percentual.

    Args:
        df_pedidos: DataFrame de pedidos NF-e (coluna "Produto" e "QUANT").
        df_metas:   DataFrame de metas (colunas "Produto" e "Meta").

    Returns:
        DataFrame com: Produtos | meta | pedido | Progresso | status da meta
    """
    import re as _re
    import unicodedata as _ud

    _RE_SUFIXO_UNID = _re.compile(
        r'\s+(KG|UN|CX|FD|PCT|SC|BAG)\b.*$', _re.IGNORECASE
    )

    def _sem_acento(s: str) -> str:
        """Remove acentos para comparação: 'PIMENTÃO' → 'PIMENTAO'."""
        return ''.join(
            c for c in _ud.normalize('NFD', s)
            if _ud.category(c) != 'Mn'
        )

    def _normalizar_produto(nome: str) -> str:
        """Remove sufixos de unidade/embalagem do nome do produto.

        Exemplos:
            'BATATA DOCE BRANCA KG CX 20' → 'BATATA DOCE BRANCA'
            'BANANA NANICA KG'            → 'BANANA NANICA'
            'CEBOLA UN 10'                → 'CEBOLA'
        """
        return _RE_SUFIXO_UNID.sub('', nome.strip().upper()).strip()

    df_ped = df_pedidos.copy()
    df_ped["Produto"] = df_ped["Produto"].astype(str).apply(_normalizar_produto)

    vendas = (
        df_ped.groupby("Produto")["QUANT"]
        .sum()
        .reset_index()
        .rename(columns={"Produto": "Produto", "QUANT": "pedido"})
    )
    vendas["Produto"] = vendas["Produto"].str.strip().str.upper()

    # Merge sem acento: 'PIMENTÃO' (meta) casa com 'PIMENTAO' (PDF)
    vendas["_chave"]        = vendas["Produto"].apply(_sem_acento)
    df_metas_m              = df_metas.copy()
    df_metas_m["_chave"]    = df_metas_m["Produto"].apply(_sem_acento)

    df = df_metas_m.merge(vendas[["_chave", "pedido"]], on="_chave", how="left")
    df.drop(columns=["_chave"], inplace=True)
    df["pedido"] = df["pedido"].fillna(0)
    df["Progresso"] = (
        (df["pedido"] / df["Meta"].replace(0, float("nan"))) * 100
    ).round(1).fillna(0)
    df["status da meta"] = df["Progresso"].apply(
        lambda p: "META CONCLUÍDA" if p >= 100 else "META EM ANDAMENTO"
    )
    return df.rename(columns={"Produto": "Produtos", "Meta": "meta"})[
        ["Produtos", "meta", "pedido", "Progresso", "status da meta"]
    ]


def _dados_para_chat() -> dict:
    """Monta o dicionário de dados para enviar ao chat IA."""
    return {
        "precos":            st.session_state["precos"],
        "progresso":         st.session_state["progresso"],
        "pedidos":           st.session_state["pedidos"],
        "metas":             st.session_state["metas"],
        "saldo_estoque":     st.session_state["saldo_estoque"],
        "historico_estoque": st.session_state["historico_estoque"],
    }


# ---------------------------------------------------------------------------
# Componentes visuais auxiliares
# ---------------------------------------------------------------------------

def _barra_progresso_html(pct: float, meta_status: str) -> str:
    """Gera HTML de barra de progresso colorida conforme percentual e status.

    Args:
        pct: Percentual de 0 a 100+.
        meta_status: Texto do status ("META CONCLUÍDA", "META EM ANDAMENTO", etc.).

    Returns:
        String HTML da barra.
    """
    pct_clip = min(pct, 100.0)
    if pct >= 100 or "CONCLUÍDA" in meta_status.upper():
        cor = "#2d7a4f"   # verde
    elif pct >= 70:
        cor = "#f5c842"   # amarelo
    else:
        cor = "#e8843a"   # laranja

    return (
        f'<div class="progress-wrap">'
        f'<div class="progress-bar" style="width:{pct_clip:.1f}%;background:{cor};"></div>'
        f'</div>'
        f'<small style="color:rgba(255,255,255,0.45)">{pct:.1f}%</small>'
    )


def _badge_status(status: str) -> str:
    """Gera HTML de badge colorido para o status da meta."""
    s = status.upper()
    if "CONCLUÍDA" in s or "CONCLUIDA" in s:
        cls = "badge-green"
        label = "✓ Concluída"
    elif "ANDAMENTO" in s:
        cls = "badge-yellow"
        label = "↗ Em andamento"
    else:
        cls = "badge-blue"
        label = status.title()
    return f'<span class="badge {cls}">{label}</span>'


def _cor_preco(preco, referencia) -> str:
    """Retorna cor HTML comparando preço com referência."""
    try:
        if float(preco) < float(referencia):
            return "color:#2d7a4f;font-weight:350"   # mais barato = verde
        if float(preco) > float(referencia):
            return "color:#e8843a;font-weight:350"   # mais caro = laranja
    except (TypeError, ValueError):
        pass
    return ""


# ---------------------------------------------------------------------------
# Estado global thread-safe para busca de preços em background
# ---------------------------------------------------------------------------

_PRECOS_STATE: dict[str, dict] = {}
_PRECOS_LOCK  = threading.Lock()


def _rodar_busca_precos(session_id: str, script_path: str) -> None:
    """Executa buscar_precos.py em background e atualiza _PRECOS_STATE."""
    proc = None
    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            env={**os.environ,
                 "PYTHONIOENCODING": "utf-8",
                 "PYTHONUTF8": "1",
                 "PYTHONUNBUFFERED": "1"},
            cwd=os.path.dirname(script_path),
        )

        with _PRECOS_LOCK:
            if session_id not in _PRECOS_STATE:
                proc.kill()
                return
            _PRECOS_STATE[session_id]["proc"] = proc

        for linha in iter(proc.stdout.readline, ""):
            with _PRECOS_LOCK:
                if session_id not in _PRECOS_STATE:
                    proc.kill()
                    return
            linha = linha.rstrip()
            if not linha:
                continue
            m = re.search(r"\[(\d+)/(\d+)\]", linha)
            with _PRECOS_LOCK:
                if session_id not in _PRECOS_STATE:
                    proc.kill()
                    return
                _PRECOS_STATE[session_id]["status_txt"] = linha
                if m:
                    atual, total = int(m.group(1)), int(m.group(2))
                    if total > 0:
                        _PRECOS_STATE[session_id]["progresso"] = min(atual / total, 0.99)

        try:
            proc.wait(timeout=660)
        except subprocess.TimeoutExpired:
            proc.kill()
            with _PRECOS_LOCK:
                if session_id in _PRECOS_STATE:
                    _PRECOS_STATE[session_id].update(
                        rodando=False, sucesso=False,
                        erro="Timeout: processo encerrado após 11 minutos.",
                    )
            return

        stderr_txt = proc.stderr.read()

        with _PRECOS_LOCK:
            if session_id not in _PRECOS_STATE:
                return
            if proc.returncode == 0:
                _PRECOS_STATE[session_id].update(
                    rodando=False, sucesso=True, progresso=1.0, erro="",
                )
            else:
                _PRECOS_STATE[session_id].update(
                    rodando=False, sucesso=False,
                    erro=stderr_txt or "(sem detalhes)",
                )

    except Exception as exc:
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass
        with _PRECOS_LOCK:
            if session_id in _PRECOS_STATE:
                _PRECOS_STATE[session_id].update(
                    rodando=False, sucesso=False, erro=str(exc),
                )


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def _render_sidebar() -> None:
    """Renderiza a barra lateral com configurações e status."""
    with st.sidebar:
        # ---- Toggle de tema ----
        t = _get_tema()
        col_tog, col_label = st.sidebar.columns([1, 4])
        with col_tog:
            if st.button(t["icone"], key="btn_tema", help=t["toggle_label"]):
                st.session_state["tema_escuro"] = not st.session_state.get("tema_escuro", True)
                st.rerun()
        with col_label:
            st.caption(t["toggle_label"])
        st.sidebar.divider()

        st.markdown("## 🌿 Mita")
        st.markdown("---")

        # ---- Link para as outras abas ----
        st.markdown("""
        <style>
        .botao-link a {
            display: block;
            padding: 8px 16px;
            background-color: #216b3a;
            color: white !important;
            border-radius: 16px;
            text-decoration: none;
            text-align: center;
            font-weight: 350;
            margin-bottom: 16px;
        }
        .botao-link a:hover {
            background-color: #2e8b4f;
        }
        </style>

        <div class="botao-link">
            <a href="https://benverde.streamlit.app/Registro_Caixas">📦 Registro das Caixas</a>
        </div>
        <div class="botao-link">
            <a href="https://benverde.streamlit.app/Registro_Estoque">🍌 Estoque de banana</a>
        </div>
    """, unsafe_allow_html=True)

        # ---- Status de dados ----
        st.markdown("### 📊 Status dos Dados")
        if st.session_state["ultima_atualizacao"]:
            ts = st.session_state["ultima_atualizacao"].strftime("%d/%m %H:%M")
            st.markdown(
                f'<div class="sidebar-status">'
                f'⏱ <strong>Última atualização:</strong><br>{ts}</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="sidebar-status">'
                f'📄 <strong>CSVs de preços:</strong> {st.session_state["n_csvs"]}<br>'
                f'📦 <strong>Registros de estoque:</strong> {st.session_state["n_pdfs"]}'
                f'</div>',
                unsafe_allow_html=True,
            )
            if st.session_state["erro_carregamento"]:
                st.warning(f"⚠️ {st.session_state['erro_carregamento']}")
        else:
            st.info("Dados ainda não carregados.")

        st.markdown("---")

        # ---- Buscar preços ----
        # Modo automático: usa a página "💰 Busca de Preços" para escolha interativa.
        _SCRIPT_PRECOS = "verificação dos preços dos produtos\buscar_precos.py"

        st.page_link("pages/4_Busca_Precos.py", label="💰 Busca de Preços (interativa)", icon="🛒", use_container_width=True)
        st.caption("⚡ Ou atualize automaticamente sem seleção manual:")

        # Obtém session_id para indexar o estado global
        _ctx = st.runtime.scriptrunner.get_script_run_ctx()
        _sid = _ctx.session_id if _ctx else "default"

        # Snapshot thread-safe do estado atual (nunca segurar o lock ao chamar Streamlit)
        with _PRECOS_LOCK:
            _ps = dict(_PRECOS_STATE.get(_sid, {}))

        if not _ps:
            # CASO 1 — Ocioso: mostra o botão
            if st.button("🔍 Atualizar Preços (auto)", width="stretch", type="primary"):
                with _PRECOS_LOCK:
                    if _PRECOS_STATE.get(_sid, {}).get("rodando"):
                        pass  # duplo clique: ignora
                    else:
                        _PRECOS_STATE[_sid] = {
                            "rodando":    True,
                            "progresso":  0.01,
                            "status_txt": "⏳ Iniciando...",
                            "erro":       "",
                            "sucesso":    None,
                            "proc":       None,
                        }
                        threading.Thread(
                            target=_rodar_busca_precos,
                            args=(_sid, _SCRIPT_PRECOS),
                            daemon=True,
                        ).start()
                st.rerun()

        elif _ps.get("rodando"):
            # CASO 2 — Em execução: mostra progresso e botão cancelar
            st.progress(_ps.get("progresso", 0.01))
            st.caption(_ps.get("status_txt", ""))
            if st.button("⏹ Cancelar", width="stretch"):
                with _PRECOS_LOCK:
                    _entry = _PRECOS_STATE.pop(_sid, {})
                _proc = _entry.get("proc")
                if _proc is not None:
                    try:
                        _proc.kill()
                    except Exception:
                        pass
                st.rerun()
            time.sleep(1.0)
            st.rerun()

        else:
            # CASO 3 — Concluído: exibe resultado e volta ao botão
            with _PRECOS_LOCK:
                _entry = _PRECOS_STATE.pop(_sid, {})
            if _entry.get("sucesso"):
                st.success("✅ Busca de preços concluída!")
            else:
                _err = _entry.get("erro", "(sem detalhes)")
                st.error(f"❌ Erro na busca de preços.\n\n```\n{_err}\n```")
            st.rerun()

        # ---- Limpar histórico do chat ----
        if st.button("🗑 Limpar histórico do chat", width="stretch"):
            st.session_state["chat_historico"] = []
            st.session_state["chat_historico_api"] = []
            st.success("Histórico limpo.")

        # ---- Painel admin: aprovações pendentes ----
        _user_logado = get_user(st.session_state.get("username_logado", ""))
        if _user_logado and _user_logado.get("is_admin"):
            _pendentes = carregar_pending()
            if _pendentes:
                with st.expander(f"👤 Aprovações pendentes ({len(_pendentes)})"):
                    for _p in _pendentes:
                        _data = _p.get("solicitado_em", "")[:10]
                        st.markdown(f"**{_p['nome']}** (`{_p['username']}`) — {_data}")
                        _col_ap, _col_rej = st.columns(2)
                        if _col_ap.button("✅ Aprovar", key=f"ap_{_p['username']}", use_container_width=True):
                            aprovar_usuario(_p["username"])
                            st.rerun()
                        if _col_rej.button("❌ Rejeitar", key=f"rej_{_p['username']}", use_container_width=True):
                            rejeitar_usuario(_p["username"])
                            st.rerun()

        st.markdown("---")
        st.caption("Mita IA · v1.5")


# ---------------------------------------------------------------------------
# Aba 1 — Chat IA
# ---------------------------------------------------------------------------

def _sanitizar_resposta_chat(texto: str) -> str:
    """Corrige conflitos de formatação LaTeX que o Streamlit interpreta errado."""
    import re

    # Corrige "R4,89" → "R$ 4,89" (IA às vezes omite o cifrão)
    texto = re.sub(r'\bR(\d)', r'R$ \1', texto)

    # Remove caracteres invisíveis
    texto = texto.replace('\u200b', '').replace('\u00a0', ' ')

    # Escapa TODOS os cifrões que não foram escapados ainda
    # Substitui $ que não seja precedido por \ 
    texto = re.sub(r'(?<!\\)\$', r'\\$', texto)

    return texto

def _render_aba_chat() -> None:
    """Renderiza a aba de chat com a IA."""
    st.markdown("### 🤖 Assistente de Gerência")
    st.caption("Pergunte sobre preços, estoque, metas e vendas. A IA responde com base nos dados atuais.")

    if not st.session_state["dados_carregados"]:
        st.warning("⚠️ Dados não carregados. Clique em **Atualizar Dados** para começar.")
        return

    # Exibe histórico de mensagens
    historico = st.session_state["chat_historico"]
    chat_container = st.container()
    with chat_container:
        for msg in historico:
            with st.chat_message(msg["role"], avatar="🌿" if msg["role"] == "assistant" else "👤"):
                conteudo = _sanitizar_resposta_chat(msg["content"])
                st.markdown(conteudo)

    # Sugestões rápidas
    col1, col2, col3, col4 = st.columns(4)
    sugestoes = [
        "📦 Estoque atual de bananas?",
        "🎯 Status das metas do mês?",
        "💰 Preço da banana nanica hoje?",
        "📈 Resumo geral da empresa?",
    ]
    textos_sugestao = [
        "Qual o estoque atual de bananas e como está o saldo?",
        "Como está o progresso das metas este mês? Alguma em risco?",
        "Qual o preço da banana nanica nos concorrentes hoje?",
        "Me dê um resumo geral: metas, estoque e preços mais importantes.",
    ]
    for col, sugestao, texto in zip([col1, col2, col3, col4], sugestoes, textos_sugestao):
        if col.button(sugestao, width="stretch", key=f"sug_{sugestao[:10]}"):
            st.session_state["_mensagem_pendente"] = texto
            st.rerun()

    # Processa mensagem de sugestão se pendente
    if "_mensagem_pendente" in st.session_state and st.session_state["_mensagem_pendente"]:
        mensagem_auto = st.session_state.pop("_mensagem_pendente")
        _processar_mensagem_chat(mensagem_auto)
        st.rerun()

    # Input principal
    if mensagem := st.chat_input("Digite sua pergunta sobre a empresa..."):
        _processar_mensagem_chat(mensagem)
        st.rerun()


def _processar_mensagem_chat(mensagem: str) -> None:
    """Envia mensagem à IA e atualiza o histórico no session_state.

    Args:
        mensagem: Texto digitado pelo usuário.
    """
    dados = _dados_para_chat()

    # Histórico da API: contém o contexto de dados injetado (não exibido ao usuário)
    hist_api = st.session_state.get("chat_historico_api", [])

    with st.spinner("🤔 Consultando IA..."):
        try:
            resposta, hist_api_novo = chat_com_grok_historico(mensagem, dados, hist_api)
        except Exception as exc:
            logger.error("Erro inesperado no chat: %s", exc)
            resposta = "Erro inesperado ao consultar a IA. Tente novamente."
            hist_api_novo = hist_api

    # Salva histórico da API (com contexto) para manter multi-turno
    st.session_state["chat_historico_api"] = hist_api_novo

    # Histórico de exibição: guarda apenas a pergunta original e a resposta
    st.session_state["chat_historico"].append({"role": "user", "content": mensagem})
    st.session_state["chat_historico"].append({"role": "assistant", "content": resposta})


# ---------------------------------------------------------------------------
# Aba 2 — Metas e Vendas
# ---------------------------------------------------------------------------

def _gerar_tabela_exportavel(df_prog: pd.DataFrame) -> "go.Figure":
    """Gera figura Plotly (Table) pronta para exportar em JPEG ou PDF.

    Args:
        df_prog: DataFrame de progresso de metas.

    Returns:
        ``go.Figure`` com tabela estilizada.
    """
    t = _get_tema()
    col_prod   = "Produtos"       if "Produtos"       in df_prog.columns else df_prog.columns[0]
    col_meta   = "meta"           if "meta"           in df_prog.columns else None
    col_pedido = "pedido"         if "pedido"         in df_prog.columns else None
    col_prog   = "Progresso"      if "Progresso"      in df_prog.columns else None
    col_status = "status da meta" if "status da meta" in df_prog.columns else None

    headers = ["Produto"]
    values  = [df_prog[col_prod].tolist()]
    cell_colors = [[t["tr_par"]] * len(df_prog)]  # cor padrão da coluna produto

    if col_meta:
        headers.append("Meta (kg)")
        values.append(df_prog[col_meta].apply(lambda v: f"{v:,.0f}").tolist())
        cell_colors.append([t["tr_par"]] * len(df_prog))

    if col_pedido:
        headers.append("Pedido (kg)")
        values.append(df_prog[col_pedido].apply(lambda v: f"{v:,.0f}").tolist())
        cell_colors.append([t["tr_par"]] * len(df_prog))

    if col_prog:
        headers.append("Progresso (%)")
        values.append(df_prog[col_prog].apply(lambda v: f"{v:.1f}%").tolist())
        prog_cores = [
            "#d1fae5" if float(v) >= 100 else "#fef9c3" if float(v) >= 70 else "#fee2e2"
            for v in df_prog[col_prog]
        ]
        cell_colors.append(prog_cores)

    if col_status:
        headers.append("Status")
        values.append(df_prog[col_status].tolist())
        status_cores = [
            "#d1fae5" if "CONCLU" in str(s).upper() else "#fef9c3"
            for s in df_prog[col_status]
        ]
        cell_colors.append(status_cores)

    fig = go.Figure(data=[go.Table(
        columnwidth=[250] + [120] * (len(headers) - 1),
        header=dict(
            values=headers,
            fill_color=t["header_table"],
            font=dict(color="white", size=11, family="DM Sans"),
            align="left",
            height=36,
            line_color=t["tabela_borda"],
        ),
        cells=dict(
            values=values,
            fill_color=t["cell_color"],
            font=dict(color=t["cell_font"], size=11, family="DM Sans"),
            align="left",
            height=30,
            line_color=t["tabela_borda"],
        ),
    )])
    fig.update_layout(
        title=dict(
            text="Progresso de Metas — Benverde",
            font=dict(size=16, family="DM Sans", color=t["table_title"]),
            x=0.01,
        ),
        margin=dict(l=20, r=20, t=60, b=20),
        paper_bgcolor=t["table_paper"],
        font=dict(color=t["plot_font"], family="DM Sans"),
    )
    return fig


@st.cache_data(show_spinner=False)
def _exportar_tabela_cache(tabela_json: str) -> tuple:
    """Renderiza tabela de progresso em JPEG e PDF via matplotlib (sem Chrome)."""
    import io
    import json
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker

    dados = json.loads(tabela_json)
    cols  = dados["cols"]
    rows  = dados["rows"]

    n_rows = len(rows)
    fig_h  = max(4, n_rows * 0.38 + 1.2)
    fig, ax = plt.subplots(figsize=(16, fig_h))
    ax.axis("off")
    fig.patch.set_facecolor("#0d2518")

    tbl = ax.table(
        cellText=rows,
        colLabels=cols,
        cellLoc="left",
        loc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.auto_set_column_width(range(len(cols)))

    # Estilo cabeçalho
    for j in range(len(cols)):
        cell = tbl[0, j]
        cell.set_facecolor("#2d7a4f")
        cell.set_text_props(color="white", fontweight="bold")
        cell.set_edgecolor("#4caf7d")

    # Estilo linhas alternadas
    for i in range(1, n_rows + 1):
        bg = "#1a3d2b" if i % 2 == 0 else "#122d1f"
        for j in range(len(cols)):
            cell = tbl[i, j]
            cell.set_facecolor(bg)
            cell.set_text_props(color="rgba(255,255,255,0.85)")
            cell.set_edgecolor("#1e4a30")

    ax.set_title(
        "Progresso de Metas — Benverde",
        color="#4caf7d", fontsize=13, fontweight="bold", pad=10,
    )

    buf = io.BytesIO()
    fig.savefig(buf, format="jpeg", bbox_inches="tight", dpi=150,
                facecolor=fig.get_facecolor())
    jpeg = buf.getvalue()

    buf = io.BytesIO()
    fig.savefig(buf, format="pdf", bbox_inches="tight",
                facecolor=fig.get_facecolor())
    pdf = buf.getvalue()

    plt.close(fig)
    return jpeg, pdf


_CSV_RENOMEAR = {
    "nome do produto": "Produto",
    "produto":         "Produto",
    "valortotal":      "VALOR TOTAL",
    "valor total":     "VALOR TOTAL",
    "valorunit":       "VALOR UNIT",
    "valor unit":      "VALOR UNIT",
    "valor unitário":  "VALOR UNIT",
    "unid":            "UNID",
    "quant":           "QUANT",
    "data":            "Data",
    "loja":            "Loja",
}


def _ler_csv(fonte) -> pd.DataFrame:
    """Lê CSV com separador auto-detectado e normaliza nomes de coluna."""
    for sep in (",", ";", "\t", "|"):
        try:
            df = pd.read_csv(fonte, sep=sep, encoding="utf-8", on_bad_lines="skip", engine="python")
            if df.shape[1] > 1:
                break
        except Exception:
            try:
                if hasattr(fonte, "seek"):
                    fonte.seek(0)
                df = pd.read_csv(fonte, sep=sep, encoding="latin-1", on_bad_lines="skip", engine="python")
                if df.shape[1] > 1:
                    break
            except Exception:
                continue
    else:
        return pd.DataFrame()

    if hasattr(fonte, "seek"):
        fonte.seek(0)

    df.columns = df.columns.str.strip()
    df = df.rename(columns={c: _CSV_RENOMEAR[c.lower().strip()] for c in df.columns if c.lower().strip() in _CSV_RENOMEAR})
    return df


def _df_de_upload(arquivo) -> pd.DataFrame:
    """Processa um arquivo uploaded (PDF/CSV/ZIP) e retorna DataFrame de pedidos."""
    nome = arquivo.name.lower()
    dfs  = []

    def _pdf_bytes_para_df(dados: bytes, nome_arq: str) -> pd.DataFrame:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(dados)
            tmp_path = tmp.name
        try:
            _, dt, loja, produtos = _worker_pedido(tmp_path)
            if not produtos:
                return pd.DataFrame()
            return pd.DataFrame([{
                "Data": dt, "Loja": loja,
                "Produto":     p["produto"],
                "UNID":        p.get("unidade", "KG"),
                "QUANT":       p["quant"],
                "VALOR TOTAL": p.get("valor_total", 0.0),
                "VALOR UNIT":  p.get("valor_unit",  0.0),
            } for p in produtos])
        finally:
            os.unlink(tmp_path)

    if nome.endswith(".pdf"):
        dfs.append(_pdf_bytes_para_df(arquivo.read(), arquivo.name))

    elif nome.endswith(".csv"):
        dfs.append(_ler_csv(arquivo))

    elif nome.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(arquivo.read())) as zf:
            for membro in zf.namelist():
                m = membro.lower()
                dados = zf.read(membro)
                if m.endswith(".pdf"):
                    dfs.append(_pdf_bytes_para_df(dados, membro))
                elif m.endswith(".csv"):
                    dfs.append(_ler_csv(io.BytesIO(dados)))

    dfs = [d for d in dfs if not d.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

@st.dialog("⚙️ Gerenciar Metas")
def _render_form_metas() -> None:
    """Formulário para cadastrar, editar e remover metas — manual ou via imagem."""
    caminho_json = st.session_state["path_metas_local"]

    st.markdown("#### ⚙️ Gerenciar Metas")

    # ================================================================
    # SEÇÃO A — Upload de imagem (OCR via IA)
    # ================================================================
    st.markdown("##### 🖼️ Importar metas (imagem ou planilha)")
    st.caption("Faça upload da tabela de metas em PNG, JPEG ou XLSX. A IA extrai e normaliza os dados automaticamente.")

    arquivo_img = st.file_uploader(
        "Selecionar arquivo",
        type=["png", "jpg", "jpeg", "xlsx"],
        key="upload_metas_img",
        label_visibility="collapsed",
    )

    if arquivo_img is not None:
        eh_xlsx = arquivo_img.name.lower().endswith(".xlsx")
        col_prev, col_btn = st.columns([3, 1])

        with col_prev:
            if eh_xlsx:
                try:
                    df_prev = pd.read_excel(io.BytesIO(arquivo_img.getvalue()), dtype=str)
                    st.dataframe(df_prev.head(8), hide_index=True, use_container_width=True)
                except Exception:
                    st.warning("⚠️ Não foi possível pré-visualizar a planilha.")
            else:
                st.image(arquivo_img, caption="Pré-visualização", width="stretch")

        with col_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🤖 Extrair com IA", type="primary", width="stretch",
                         key="btn_extrair_img"):
                with st.spinner("🔍 Analisando..."):
                    if eh_xlsx:
                        try:
                            df_xlsx_ia = pd.read_excel(io.BytesIO(arquivo_img.getvalue()), dtype=str)
                            metas_extraidas = extrair_metas_de_planilha(df_xlsx_ia)
                        except Exception as exc:
                            logger.error("Erro ao ler xlsx: %s", exc)
                            metas_extraidas = []
                    else:
                        mime = "image/png" if arquivo_img.name.lower().endswith(".png") else "image/jpeg"
                        metas_extraidas = extrair_metas_de_imagem(arquivo_img.getvalue(), mime)

                if not metas_extraidas:
                    st.error("❌ Não foi possível extrair metas. Verifique a imagem e tente novamente.")
                else:
                    # Carrega metas existentes e substitui/adiciona
                    df_atual = load_metas_local(caminho_json)
                    lista    = df_atual.to_dict("records")
                    mapa     = {item["Produto"]: i for i, item in enumerate(lista)}

                    novos = 0
                    atualizados = 0
                    for item in metas_extraidas:
                        if item["Produto"] in mapa:
                            lista[mapa[item["Produto"]]]["Meta"] = item["Meta"]
                            atualizados += 1
                        else:
                            lista.append(item)
                            novos += 1

                    salvar_metas_local(lista, caminho_json)
                    st.session_state["metas"] = load_metas_local(caminho_json)

                    if st.session_state["pedidos"] is not None \
                            and not st.session_state["pedidos"].empty:
                        st.session_state["progresso"] = _calcular_progresso(
                            st.session_state["pedidos"],
                            st.session_state["metas"],
                        )

                    st.success(
                        f"✅ **{len(metas_extraidas)} produto(s)** importados — "
                        f"{novos} novo(s), {atualizados} atualizado(s)."
                    )

                    # Mostra prévia do que foi extraído
                    with st.expander("Ver dados extraídos", expanded=True):
                        st.dataframe(
                            pd.DataFrame(metas_extraidas),
                            hide_index=True,
                            width="stretch",
                        )
                    st.rerun()

    st.markdown("---")

    # ================================================================
    # SEÇÃO B — Adição / atualização manual
    # ================================================================
    st.markdown("##### ✏️ Adicionar / atualizar manualmente")

    with st.form("form_meta", clear_on_submit=True):
        col1, col2, col3 = st.columns([3, 2, 1])
        produto_input = col1.text_input("Produto", placeholder="Ex.: BANANA NANICA")
        meta_input    = col2.number_input("Meta (kg)", min_value=0, value=0, step=1)
        submitted     = col3.form_submit_button("➕ Salvar", width="stretch")

        if submitted:
            produto_norm = produto_input.strip().upper()
            if not produto_norm:
                st.warning("Informe o nome do produto.")
            else:
                df_atual = load_metas_local(caminho_json)
                lista    = df_atual.to_dict("records")
                existe   = False
                for item in lista:
                    if item["Produto"] == produto_norm:
                        item["Meta"] = int(meta_input)
                        existe = True
                        break
                if not existe:
                    lista.append({"Produto": produto_norm, "Meta": int(meta_input)})
                salvar_metas_local(lista, caminho_json)
                st.session_state["metas"] = load_metas_local(caminho_json)
                if st.session_state["pedidos"] is not None \
                        and not st.session_state["pedidos"].empty:
                    st.session_state["progresso"] = _calcular_progresso(
                        st.session_state["pedidos"],
                        st.session_state["metas"],
                    )
                acao = "Atualizada" if existe else "Adicionada"
                st.success(f"✅ {acao}: **{produto_norm}** — Meta: {int(meta_input):,} kg")
                st.rerun()

    # ================================================================
    # SEÇÃO C — Tabela de metas cadastradas com botão remover
    # ================================================================
    df_metas = load_metas_local(caminho_json)

    if df_metas.empty:
        st.info("Nenhuma meta cadastrada ainda.")
        return

    st.markdown("---")
    st.markdown("##### 📋 Metas cadastradas")

    # Controle de qual linha está em modo de edição
    if "meta_editando_idx" not in st.session_state:
        st.session_state["meta_editando_idx"] = None

    lista = df_metas.to_dict("records")
    for idx, row in enumerate(lista):
        editando = st.session_state["meta_editando_idx"] == idx

        if editando:
            # ── Linha em modo edição ──────────────────────────────────────
            with st.form(key=f"form_edit_{idx}", border=True):
                ec1, ec2, ec3, ec4 = st.columns([4, 2, 1, 1])
                novo_nome = ec1.text_input(
                    "Produto", value=row["Produto"], label_visibility="collapsed"
                )
                nova_meta = ec2.number_input(
                    "Meta", value=int(row["Meta"]), min_value=0, step=1,
                    label_visibility="collapsed"
                )
                salvar = ec3.form_submit_button("💾", help="Salvar")
                cancelar = ec4.form_submit_button("✖", help="Cancelar")

            if salvar:
                novo_nome_norm = novo_nome.strip().upper()
                if novo_nome_norm:
                    lista[idx]["Produto"] = novo_nome_norm
                    lista[idx]["Meta"]    = int(nova_meta)
                    salvar_metas_local(lista, caminho_json)
                    st.session_state["metas"] = load_metas_local(caminho_json)
                    if st.session_state["pedidos"] is not None \
                            and not st.session_state["pedidos"].empty:
                        st.session_state["progresso"] = _calcular_progresso(
                            st.session_state["pedidos"],
                            st.session_state["metas"],
                        )
                st.session_state["meta_editando_idx"] = None
                st.rerun()

            if cancelar:
                st.session_state["meta_editando_idx"] = None
                st.rerun()

        else:
            # ── Linha em modo visualização ────────────────────────────────
            col1, col2, col3, col4 = st.columns([4, 2, 1, 1])
            col1.markdown(f"**{row['Produto']}**")
            col2.markdown(f"{row['Meta']:,} kg")
            if col3.button("✏️", key=f"edit_meta_{idx}", help=f"Editar {row['Produto']}"):
                st.session_state["meta_editando_idx"] = idx
                st.rerun()
            if col4.button("🗑", key=f"del_meta_{idx}", help=f"Remover {row['Produto']}"):
                lista.pop(idx)
                salvar_metas_local(lista, caminho_json)
                st.session_state["metas"] = load_metas_local(caminho_json)
                if st.session_state["pedidos"] is not None \
                        and not st.session_state["pedidos"].empty:
                    st.session_state["progresso"] = _calcular_progresso(
                        st.session_state["pedidos"],
                        st.session_state["metas"],
                    )
                st.rerun()


def _render_aba_metas() -> None:
    """Renderiza a aba de metas e progresso de vendas."""
    st.markdown("### 🎯 Metas e Vendas")

    df_prog  = st.session_state["progresso"]
    df_metas = st.session_state["metas"]
    df_ped   = st.session_state["pedidos"]


    # ---- Upload de pedidos ----
    @st.dialog("📂 Importar Pedidos")
    def _render_importar_pedidos():
        arquivos = st.file_uploader(
        "PDF, ZIP ou CSV",
        type=["pdf", "zip", "csv"],
        accept_multiple_files=True,
        key="upload_pedidos",
        label_visibility="collapsed",
    )
        if arquivos and st.button("📥 Processar", type="primary", key="btn_proc_upload"):
            dfs = []
            erros = []
            with st.spinner("Processando..."):
                for arq in arquivos:
                    try:
                        df = _df_de_upload(arq)
                        if not df.empty:
                            dfs.append(df)
                    except Exception as exc:
                        erros.append(f"{arq.name}: {exc}")
            if dfs:
                novo = pd.concat(dfs, ignore_index=True)
                existente = st.session_state.get("pedidos")
                if existente is not None and not existente.empty:
                    novo = pd.concat([existente, novo], ignore_index=True)
                novo["QUANT"] = pd.to_numeric(novo.get("QUANT", 0), errors="coerce")
                if "Produto" not in novo.columns and "produto" in novo.columns:
                    novo = novo.rename(columns={"produto": "Produto"})
                st.session_state["pedidos"] = novo
                if st.session_state.get("metas") is not None \
                        and not st.session_state["metas"].empty:
                    st.session_state["progresso"] = _calcular_progresso(
                        novo, st.session_state["metas"]
                    )
                st.success(f"✅ {len(novo)} linha(s) carregadas.")
            if erros:
                st.error("\n".join(erros))

    st.markdown("---")

    if df_prog is None or df_prog.empty:
        st.warning("⚠️ Sem dados de progresso. Cadastre metas acima e clique em **Atualizar Dados**.")
        return

    # ---- Gerenciar Metas (expander) ----
    col1, col2 = st.columns([1, 1])
    with col1:
       if st.button("⚙️ Gerenciar Metas", use_container_width=True):
          _render_form_metas()
    with col2:
       if st.button("📂 Importar Pedidos", use_container_width=True):
          _render_importar_pedidos()

    # ---- Botões de exportação ----
    st.markdown("#### 📋 Progresso por Produto")
    col_exp_j, col_exp_p, col_exp_c, col_spacer = st.columns([1, 1, 1, 7])

    # CSV — sempre disponível
    _csv_bytes = df_prog.to_csv(index=False).encode("utf-8")
    col_exp_c.download_button(
        "⬇ CSV", _csv_bytes, "metas_progresso.csv", "text/csv",
        width="stretch",
    )

    try:
        import json as _json
        _col_prod   = "Produtos"       if "Produtos"       in df_prog.columns else df_prog.columns[0]
        _col_meta   = "meta"           if "meta"           in df_prog.columns else None
        _col_pedido = "pedido"         if "pedido"         in df_prog.columns else None
        _col_prog   = "Progresso"      if "Progresso"      in df_prog.columns else None
        _col_status = "status da meta" if "status da meta" in df_prog.columns else None
        _export_cols = [c for c in [_col_prod, _col_meta, _col_pedido, _col_prog, _col_status] if c]
        _df_exp = df_prog[_export_cols].copy()
        _df_exp.columns = [c.capitalize() for c in _df_exp.columns]
        _tabela_json = _json.dumps({
            "cols": list(_df_exp.columns),
            "rows": _df_exp.astype(str).values.tolist(),
        })
        _jpeg, _pdf = _exportar_tabela_cache(_tabela_json)
        col_exp_j.download_button(
            "⬇ JPEG", _jpeg, "metas_progresso.jpg", "image/jpeg",
            width="stretch",
        )
        col_exp_p.download_button(
            "⬇ PDF", _pdf, "metas_progresso.pdf", "application/pdf",
            width="stretch",
        )
    except Exception as _exc:
        import traceback as _tb
        logger.error("Exportação de tabela falhou: %s", _exc)
        col_exp_j.caption("JPEG indisponível")
        col_exp_p.caption("PDF indisponível")
        st.error(f"`{type(_exc).__name__}: {_exc}`\n\n```\n{_tb.format_exc()}\n```")

    # ---- Métricas resumidas ----
    total_produtos  = len(df_prog)
    concluidas      = int(df_prog["status da meta"].astype(str).str.upper().str.contains("CONCLU").sum()) \
                      if "status da meta" in df_prog.columns else 0
    em_andamento    = total_produtos - concluidas
    prog_media      = df_prog["Progresso"].dropna().mean() if "Progresso" in df_prog.columns else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de produtos", total_produtos)
    c2.metric("Metas concluídas", concluidas, delta=f"{concluidas/total_produtos*100:.0f}%" if total_produtos else None)
    c3.metric("Em andamento", em_andamento)
    c4.metric("Progresso médio", f"{prog_media:.1f}%")

    st.markdown("---")

    # ---- Tabela de progresso com barras ----
    st.markdown("#### 📋 Progresso por Produto")

    col_busca, col_filtro = st.columns([3, 1])
    busca_produto = col_busca.text_input("🔍 Buscar produto", placeholder="Ex.: BANANA", key="busca_prog")
    filtro_status = col_filtro.selectbox(
        "Status",
        ["Todos", "META CONCLUÍDA", "META EM ANDAMENTO"],
        key="filtro_status_meta",
    )

    df_exibir = df_prog.copy()
    if busca_produto:
        col_prod = "Produtos" if "Produtos" in df_exibir.columns else df_exibir.columns[0]
        df_exibir = df_exibir[df_exibir[col_prod].astype(str).str.upper().str.contains(busca_produto.upper())]
    if filtro_status != "Todos" and "status da meta" in df_exibir.columns:
        df_exibir = df_exibir[df_exibir["status da meta"].astype(str).str.upper().str.contains(
            filtro_status.replace("META ", "").split()[0]
        )]

    # Renderiza tabela com HTML para barras de progresso
    if not df_exibir.empty:
        t = _get_tema()
        linhas_html = ""
        col_prod    = "Produtos"       if "Produtos"       in df_exibir.columns else df_exibir.columns[0]
        col_meta    = "meta"           if "meta"           in df_exibir.columns else None
        col_pedido  = "pedido"         if "pedido"         in df_exibir.columns else None
        col_prog    = "Progresso"      if "Progresso"      in df_exibir.columns else None
        col_status  = "status da meta" if "status da meta" in df_exibir.columns else None

        for _, row in df_exibir.iterrows():
            produto = str(row.get(col_prod, ""))
            meta    = row.get(col_meta, 0)   or 0
            pedido  = row.get(col_pedido, 0) or 0
            pct     = float(row.get(col_prog, 0) or 0)
            status  = str(row.get(col_status, ""))

            is_banana = "BANANA" in produto.upper()
            row_bg    = f"background:{t['banana_row']};" if is_banana else ""

            linhas_html += (
                f"<tr style='{row_bg}'>"
                f"<td style='padding:0.6rem 0.8rem;font-weight:500'>"
                f"{'🍌 ' if is_banana else ''}{produto}</td>"
                f"<td style='padding:0.6rem 0.8rem;text-align:right'>{meta:,.0f}</td>"
                f"<td style='padding:0.6rem 0.8rem;text-align:right'>{pedido:,.0f}</td>"
                f"<td style='padding:0.6rem 0.8rem;min-width:180px'>"
                f"{_barra_progresso_html(pct, status)}</td>"
                f"<td style='padding:0.6rem 0.8rem'>{_badge_status(status)}</td>"
                f"</tr>"
            )

        tabela_html = f"""
        <div style="overflow-x:auto;border:1px solid {t['tabela_borda']};border-radius:10px;margin-top:0.5rem">
        <table style="width:100%;border-collapse:collapse;font-size:0.88rem">
            <thead>
                <tr style="background:{t['th_bg']};border-bottom:1px solid {t['tabela_borda']}">
                    <th style="padding:0.7rem 0.8rem;text-align:left;font-weight:600;color:{t['texto_th']}">Produto</th>
                    <th style="padding:0.7rem 0.8rem;text-align:right;font-weight:600;color:{t['texto_th']}">Meta</th>
                    <th style="padding:0.7rem 0.8rem;text-align:right;font-weight:600;color:{t['texto_th']}">Pedido</th>
                    <th style="padding:0.7rem 0.8rem;text-align:left;font-weight:600;color:{t['texto_th']}">Progresso</th>
                    <th style="padding:0.7rem 0.8rem;text-align:left;font-weight:600;color:{t['texto_th']}">Status</th>
                </tr>
            </thead>
            <tbody>{linhas_html}</tbody>
        </table>
        </div>
        """
        st.markdown(tabela_html, unsafe_allow_html=True)
    else:
        st.info("Nenhum produto encontrado com os filtros aplicados.")

    st.markdown("---")

    # ---- Pedidos recentes ----
    st.markdown("#### 🛒 Pedidos Recentes")
    if df_ped is not None and not df_ped.empty:
        col_f1, col_f2, col_f3 = st.columns([2, 2, 1])

        busca_ped = col_f1.text_input("🔍 Produto", placeholder="Ex.: BANANA NANICA", key="busca_ped")
        lojas_disponiveis = ["Todas"] + sorted(df_ped["Loja"].dropna().unique().tolist()) \
                            if "Loja" in df_ped.columns else ["Todas"]
        filtro_loja = col_f2.selectbox("Loja", lojas_disponiveis, key="filtro_loja_ped")
        n_linhas = col_f3.number_input("Linhas", min_value=10, max_value=500, value=50, step=10, key="n_ped")

        df_ped_exibir = df_ped.copy()
        if "Data" in df_ped_exibir.columns:
            df_ped_exibir = df_ped_exibir.sort_values("Data", ascending=False)

        col_personalizar = next(
            (c for c in ["Personalizar", "Produto"] if c in df_ped_exibir.columns),
            None,
        )
        if busca_ped and col_personalizar:
            df_ped_exibir = df_ped_exibir[
                df_ped_exibir[col_personalizar].astype(str).str.upper().str.contains(busca_ped.upper())
            ]
        if filtro_loja != "Todas" and "Loja" in df_ped_exibir.columns:
            df_ped_exibir = df_ped_exibir[df_ped_exibir["Loja"] == filtro_loja]

        df_ped_exibir = df_ped_exibir.head(int(n_linhas))

        st.dataframe(
            df_ped_exibir,
            width="stretch",
            hide_index=True,
            height=350,
        )
        st.caption(f"Exibindo {len(df_ped_exibir)} de {len(df_ped)} pedidos.")
    else:
        st.info("Dados de pedidos não disponíveis.")

    # ---- Gráfico: vendas por produto (top 10) ----
    if df_ped is not None and not df_ped.empty:
        st.markdown("---")
        st.markdown("#### 📊 Top 10 Produtos por Volume Vendido")
        col_prod_ped = "Personalizar" if "Personalizar" in df_ped.columns else None
        col_quant    = "QUANT"        if "QUANT"        in df_ped.columns else None

        if col_prod_ped and col_quant:
            top10 = (
                df_ped.groupby(col_prod_ped)[col_quant]
                .sum()
                .nlargest(10)
                .reset_index()
                .rename(columns={col_prod_ped: "Produto", col_quant: "Quantidade"})
            )
            cores = [
                "#f5c842" if "BANANA" in p.upper() else "#4caf7d"
                for p in top10["Produto"]
            ]
            fig = go.Figure(go.Bar(
                x=top10["Quantidade"],
                y=top10["Produto"],
                orientation="h",
                marker_color=cores,
                text=top10["Quantidade"].apply(lambda v: f"{v:,.1f}"),
                textposition="outside",
            ))
            t = _get_tema()
            fig.update_layout(
                height=380,
                margin=dict(l=0, r=60, t=10, b=10),
                xaxis_title="Quantidade",
                yaxis=dict(autorange="reversed"),
                **_plotly_base(t),
            )
            fig.update_xaxes(showgrid=True, **_plotly_axes(t))
            st.plotly_chart(fig, width="stretch")


# ---------------------------------------------------------------------------
# Aba 3 — Estoque de Bananas
# ---------------------------------------------------------------------------

def _render_aba_estoque() -> None:
    """Renderiza a aba de estoque de bananas."""
    st.markdown("### 🍌 Estoque de Bananas")

    saldo    = st.session_state["saldo_estoque"]
    historico = st.session_state["historico_estoque"]

    # ---- Métricas principais ----
    total_entradas = sum(r["quant"] for r in historico if r.get("tipo") == "entrada")
    total_saidas   = sum(r["quant"] for r in historico if r.get("tipo") in ("saida", "bonificação"))
    n_movimentacoes = len(historico)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 Saldo atual (kg)", f"{saldo:,.1f}", delta=f"{saldo - total_saidas:+.1f} vs saídas")
    c2.metric("📥 Total entradas (kg)", f"{total_entradas:,.1f}")
    c3.metric("📤 Total saídas (kg)", f"{total_saidas:,.1f}")
    c4.metric("📄 Movimentações", n_movimentacoes)

    if saldo <= 0:
        st.error("🚨 **Atenção: estoque zerado ou negativo!** Verifique os dados.")
    elif saldo < 50:
        st.warning(f"⚠️ Estoque baixo: apenas **{saldo:.1f} kg**. Considere reposição.")
    else:
        st.success(f"✅ Estoque saudável: **{saldo:.1f} kg** disponíveis.")

    st.markdown("---")

    if not historico:
        st.info("Nenhuma movimentação encontrada. Verifique as pastas de PDFs e clique em **Atualizar Dados**.")
        return

    # ---- Gráfico de movimentações ----
    st.markdown("#### 📈 Movimentações ao Longo do Tempo")

    df_hist = pd.DataFrame(historico)
    df_hist["data_fmt"] = pd.to_datetime(df_hist["data"], errors="coerce")
    df_hist = df_hist.dropna(subset=["data_fmt"])

    # --- Limpeza de dados sujos vindos do PDF ---
    # Produto: extrai apenas o primeiro nome de banana válido da string concatenada
    # Ex.: "BANANA NANICA BANANA PRATA" → "BANANA NANICA"
    _VARIEDADES = ["BANANA DA TERRA", "BANANA MACA", "BANANA MAÇÃ",
                   "BANANA PRATA", "BANANA NANICA", "BANANA"]
    def _limpar_produto(nome: str) -> str:
        nome = str(nome).strip().upper()
        # Procura a primeira variedade conhecida que aparece no início
        for var in _VARIEDADES:
            if nome.startswith(var):
                return var
        # Fallback: pega só até o segundo "BANANA" se houver concatenação
        import re as _re
        m = _re.match(r'^(BANANA[\w\s]+?)(?=BANANA|\d|$)', nome)
        return m.group(1).strip() if m else nome

    df_hist["produto"] = df_hist["produto"].apply(_limpar_produto)

    # Unidade: extrai apenas KG/UN/CX da string que pode conter código NCM
    # Ex.: "08039000 040 5102 KG 16 08039000..." → "KG"
    def _limpar_unidade(unid: str) -> str:
        import re as _re
        m = _re.search(r'\b(KG|UN|CX|FD|PCT|SC)\b', str(unid).upper())
        return m.group(1) if m else str(unid).strip()[:5]

    df_hist["unidade"] = df_hist["unidade"].apply(_limpar_unidade)

    if not df_hist.empty:
        # Agrega por dia e tipo
        df_agg = (
            df_hist.groupby([df_hist["data_fmt"].dt.date, "tipo"])["quant"]
            .sum()
            .reset_index()
            .rename(columns={"data_fmt": "Data", "quant": "Quantidade (kg)"})
        )
        df_agg["Data"] = pd.to_datetime(df_agg["Data"])

        df_entradas = df_agg[df_agg["tipo"] == "entrada"]
        df_saidas   = df_agg[df_agg["tipo"].isin(["saida", "bonificação"])]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_entradas["Data"], y=df_entradas["Quantidade (kg)"],
            name="Entradas", marker_color="#2d7a4f",
            text=df_entradas["Quantidade (kg)"].apply(lambda v: f"{v:.0f}"),
            textposition="outside",
        ))
        fig.add_trace(go.Bar(
            x=df_saidas["Data"], y=df_saidas["Quantidade (kg)"],
            name="Saídas", marker_color="#e8843a",
            text=df_saidas["Quantidade (kg)"].apply(lambda v: f"{v:.0f}"),
            textposition="outside",
        ))

        # Linha de saldo acumulado
        df_saldo = df_agg.copy()
        df_saldo["sinal"] = df_saldo["tipo"].map({"entrada": 1, "saida": -1, "bonificação": -1})
        df_saldo["contribuicao"] = df_saldo["Quantidade (kg)"] * df_saldo["sinal"]
        df_saldo_dia = df_saldo.groupby("Data")["contribuicao"].sum().cumsum().reset_index()
        df_saldo_dia.columns = ["Data", "Saldo acumulado"]

        fig.add_trace(go.Scatter(
            x=df_saldo_dia["Data"], y=df_saldo_dia["Saldo acumulado"],
            name="Saldo acumulado", mode="lines+markers",
            line=dict(color="#f5c842", width=2.5, dash="dot"),
            marker=dict(size=7),
            yaxis="y2",
        ))

        t = _get_tema()
        fig.update_layout(
            height=380,
            barmode="group",
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            **_plotly_base(t),
            yaxis=dict(title="Quantidade (kg)", showgrid=True, **_plotly_axes(t)),
            yaxis2=dict(title="Saldo (kg)", overlaying="y", side="right", showgrid=False),
            xaxis=dict(showgrid=False),
        )
        st.plotly_chart(fig, width="stretch")

    st.markdown("---")

    # ---- Breakdown por variedade ----
    st.markdown("#### 🧩 Estoque por Variedade de Banana")
    df_variedade = (
        df_hist.groupby(["produto", "tipo"])["quant"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )
    if "entrada" in df_variedade.columns and "saida" in df_variedade.columns:
        df_variedade["saldo"] = df_variedade["entrada"] - df_variedade["saida"]
    elif "entrada" in df_variedade.columns:
        df_variedade["saldo"] = df_variedade["entrada"]
    else:
        df_variedade["saldo"] = 0

    df_variedade = df_variedade.sort_values("saldo", ascending=False)

    fig_var = px.pie(
        df_variedade,
        names="produto",
        values="saldo",
        color_discrete_sequence=["#2d7a4f", "#4caf7d", "#f5c842", "#e8843a", "#a5d6b0"],
        hole=0.45,
    )
    fig_var.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=20, b=0),
        showlegend=True,
        **_plotly_base(t),
    )
    fig_var.update_traces(textinfo="percent+label")

    col_pizza, col_tabela = st.columns([1, 1])
    with col_pizza:
        st.plotly_chart(fig_var, width="stretch")
    with col_tabela:
        st.dataframe(
            df_variedade.rename(columns={"produto": "Produto", "entrada": "Entradas (kg)",
                                          "saida": "Saídas (kg)", "saldo": "Saldo (kg)"}),
            hide_index=True,
            width="stretch",
            height=280,
        )

    st.markdown("---")

    # ---- Tabela completa de movimentações ----
    st.markdown("#### 📋 Histórico de Movimentações")
    col_f1, col_f2 = st.columns([2, 1])
    filtro_tipo_est = col_f1.selectbox(
        "Tipo", ["Todas", "Entradas", "Saídas"], key="filtro_tipo_est"
    )
    busca_produto_est = col_f2.text_input(
        "🔍 Produto", placeholder="BANANA...", key="busca_est"
    )

    df_hist_exibir = df_hist.copy()
    if filtro_tipo_est == "Entradas":
        df_hist_exibir = df_hist_exibir[df_hist_exibir["tipo"] == "entrada"]
    elif filtro_tipo_est == "Saídas":
        df_hist_exibir = df_hist_exibir[df_hist_exibir["tipo"].isin(["saida", "bonificação"])]
    if busca_produto_est:
        df_hist_exibir = df_hist_exibir[
            df_hist_exibir["produto"].str.upper().str.contains(busca_produto_est.upper())
        ]

    df_hist_exibir = df_hist_exibir.sort_values("data_fmt", ascending=False)
    if "loja" not in df_hist_exibir.columns:
        df_hist_exibir["loja"] = ""
    colunas_exibir = [c for c in ["data_fmt", "tipo", "produto", "quant", "unidade",
                                   "loja", "arquivo"]
                      if c in df_hist_exibir.columns]
    df_hist_exibir = df_hist_exibir[colunas_exibir].rename(columns={
        "data_fmt": "Data", "tipo": "Tipo", "produto": "Produto",
        "quant": "Qtd (kg)", "unidade": "Unid",
        "loja": "Loja", "arquivo": "Arquivo",
    })

    st.dataframe(df_hist_exibir, width="stretch", hide_index=True, height=320)
    st.caption(f"Total: {len(df_hist_exibir)} movimentações.")


# ---------------------------------------------------------------------------
# Aba 4 — Preços Concorrentes
# ---------------------------------------------------------------------------

def _render_aba_precos() -> None:
    """Renderiza a aba de pesquisa de preços nos supermercados."""
    st.markdown("### 💰 Preços no Semar e Concorrentes")

    precos = st.session_state["precos"]

    if not precos:
        st.warning("⚠️ Nenhuma pesquisa de preços encontrada. Verifique a pasta de CSVs e clique em **Atualizar Dados**.")
        return

    datas_disponiveis = list(precos.keys())
    opcoes = ["📊 GERAL (média de todas as datas)"] + datas_disponiveis

    col_sel, col_info = st.columns([2, 2])
    data_selecionada = col_sel.selectbox(
        "📅 Selecionar data da pesquisa",
        opcoes,
        key="sel_data_preco",
    )
    col_info.markdown(
        f"<br><span class='badge badge-blue'>{len(datas_disponiveis)} pesquisa(s) disponível(is)</span>",
        unsafe_allow_html=True,
    )

    # --- Monta df_preco conforme seleção ---
    if data_selecionada.startswith("📊 GERAL"):
        # Empilha todos os DataFrames e calcula média das colunas de preço
        col_prod_ref = None
        frames = []
        for df_tmp in precos.values():
            if df_tmp is not None and not df_tmp.empty:
                frames.append(df_tmp)
                if col_prod_ref is None:
                    col_prod_ref = "Produto Buscado" if "Produto Buscado" in df_tmp.columns \
                                   else df_tmp.columns[0]

        if not frames:
            st.info("Nenhum dado disponível.")
            return

        df_todos = pd.concat(frames, ignore_index=True)
        col_precos_med = [c for c in df_todos.columns
                          if "preço" in c.lower() or "preco" in c.lower()]

        # Remove linhas com produto "sujo":
        # - nome muito longo (>50 chars) = linha CSV mal parseada
        # - contém vírgula = múltiplos produtos concatenados
        # - contém dígitos misturados = preço vazando no nome
        import re as _re
        mask_limpo = (
            df_todos[col_prod_ref].astype(str).str.len().le(50)
            & ~df_todos[col_prod_ref].astype(str).str.contains(",")
            & ~df_todos[col_prod_ref].astype(str)
               .str.contains(r'\d{1,2}[.,]\d{2}', regex=True)
        )
        df_todos = df_todos[mask_limpo].copy()

        # Normaliza nome do produto (strip + upper) para agrupar correto
        df_todos[col_prod_ref] = df_todos[col_prod_ref].str.strip().str.upper()

        # Média dos preços por produto (ignora zeros e NaN)
        df_preco = (
            df_todos
            .replace(0, float("nan"))
            .groupby(col_prod_ref, as_index=False)[col_precos_med]
            .mean()
            .round(2)
        )

        # Reconstrói coluna de produto buscado normalizada
        df_preco.insert(0, "Produto Buscado", df_preco.pop(col_prod_ref))

        # Adiciona coluna de status sintético
        for c in col_precos_med:
            status_col = c.replace("Preço", "Status").replace("preço", "Status")
            if status_col not in df_preco.columns:
                df_preco[status_col] = df_preco[c].apply(
                    lambda v: "OK" if pd.notna(v) and v > 0 else "Sem dados"
                )

        st.info(
            f"📊 **Média geral** calculada sobre **{len(frames)} pesquisa(s)** "
            f"· {len(df_preco)} produto(s)"
        )
    else:
        df_preco = precos.get(data_selecionada)
        if df_preco is None or df_preco.empty:
            st.info("Nenhum dado para a data selecionada.")
            return


    # Filtro de produto
    col_b1, col_b2 = st.columns([3, 1])
    busca_preco = col_b1.text_input("🔍 Buscar produto", placeholder="Ex.: BANANA", key="busca_preco")
    mostrar_apenas_bananas = col_b2.checkbox("🍌 Só bananas", key="chk_banana_preco")

    df_exibir = df_preco.copy()
    col_prod_buscado = "Produto Buscado" if "Produto Buscado" in df_exibir.columns else df_exibir.columns[0]

    if mostrar_apenas_bananas:
        df_exibir = df_exibir[df_exibir[col_prod_buscado].astype(str).str.upper().str.contains("BANANA")]
    elif busca_preco:
        df_exibir = df_exibir[df_exibir[col_prod_buscado].astype(str).str.upper().str.contains(busca_preco.upper())]

    st.markdown(f"**{len(df_exibir)} produto(s)** para {data_selecionada}")

    # ---- Tabela de preços com destaque de bananas ----
    col_precos = [c for c in df_exibir.columns if "preço" in c.lower() or "preco" in c.lower()]
    col_status  = [c for c in df_exibir.columns if "status" in c.lower()]

    if not df_exibir.empty:
        # Monta HTML da tabela com destacamento de linhas de banana e comparação de preços
        colunas_exibir_h = [col_prod_buscado] + col_precos + col_status
        colunas_exibir_h = [c for c in colunas_exibir_h if c in df_exibir.columns]

        t = _get_tema()
        header_html = "".join(
            f"<th style='padding:0.7rem 0.8rem;text-align:{'right' if c in col_precos else 'left'};"
            f"font-weight:600;color:{t['texto_th']};white-space:nowrap'>{c}</th>"
            for c in colunas_exibir_h
        )

        linhas_preco_html = ""
        for _, row in df_exibir.iterrows():
            produto      = str(row.get(col_prod_buscado, ""))
            is_banana    = "BANANA" in produto.upper()
            row_bg       = f"background:{t['banana_row']};" if is_banana else ""
            # Pega o menor preço válido da linha como referência
            valores_preco = [float(row[c]) for c in col_precos
                             if c in row and pd.notna(row[c]) and row[c] != 0]
            ref_preco = min(valores_preco) if valores_preco else None

            celulas = ""
            for c in colunas_exibir_h:
                val = row.get(c, "")
                if c in col_precos and pd.notna(val) and val != 0:
                    estilo_cor = _cor_preco(val, ref_preco) if ref_preco else ""
                    prefix = "🍌 " if is_banana and c == col_prod_buscado else ""
                    celulas += (
                        f"<td style='padding:0.55rem 0.8rem;text-align:right;{estilo_cor}'>"
                        f"R$ {float(val):,.2f}</td>"
                    )
                elif c in col_status:
                    s = str(val).strip()
                    if s.lower() == "ok":
                        badge = "<span class='badge badge-green'>OK</span>"
                    elif "indispon" in s.lower():
                        badge = "<span class='badge badge-red'>Indisponível</span>"
                    else:
                        badge = f"<span class='badge badge-blue'>{s}</span>"
                    celulas += f"<td style='padding:0.55rem 0.8rem'>{badge}</td>"
                else:
                    prefix = "🍌 " if is_banana and c == col_prod_buscado else ""
                    celulas += (
                        f"<td style='padding:0.55rem 0.8rem;font-weight:{'500' if is_banana else '400'}'>"
                        f"{prefix}{val}</td>"
                    )

            linhas_preco_html += f"<tr style='{row_bg}'>{celulas}</tr>"

        tabela_preco_html = f"""
        <div style="overflow-x:auto;border:1px solid {t['tabela_borda']};border-radius:10px;margin-top:0.5rem">
        <table style="width:100%;border-collapse:collapse;font-size:0.86rem">
            <thead>
                <tr style="background:{t['th_bg']};border-bottom:1px solid {t['tabela_borda']}">
                    {{header_html}}
                </tr>
            </thead>
            <tbody>{{linhas_preco_html}}</tbody>
        </table>
        </div>
        <p style='font-size:0.78rem;color:{t['texto_suave']};margin-top:0.4rem'>
            🟢 Menor preço &nbsp; 🟠 Maior preço &nbsp; 🟡 Fundo = banana
        </p>
        """
        st.markdown(tabela_preco_html, unsafe_allow_html=True)
    else:
        st.info("Nenhum produto encontrado com os filtros aplicados.")

    # ---- Gráfico: comparativo de preços de bananas ----
    st.markdown("---")
    st.markdown("#### 📊 Comparativo de Preços — Bananas")

    df_bananas = df_preco[df_preco[col_prod_buscado].astype(str).str.upper().str.contains("BANANA")].copy()

    if not df_bananas.empty and col_precos:
        df_melt = df_bananas[[col_prod_buscado] + col_precos].melt(
            id_vars=col_prod_buscado,
            value_vars=col_precos,
            var_name="Supermercado",
            value_name="Preço (R$)",
        ).dropna(subset=["Preço (R$)"])
        df_melt = df_melt[df_melt["Preço (R$)"] > 0]

        # Limpa nome do supermercado (remove "Preço (" e ")")
        df_melt["Supermercado"] = (
            df_melt["Supermercado"]
            .str.replace(r"Preço\s*\(?\s*", "", regex=True)
            .str.replace(r"\)\s*$", "", regex=True)
            .str.strip()
        )

        if not df_melt.empty:
            fig_comp = px.bar(
                df_melt,
                x=col_prod_buscado,
                y="Preço (R$)",
                color="Supermercado",
                barmode="group",
                color_discrete_sequence=["#2d7a4f", "#f5c842", "#e8843a", "#4caf7d"],
                text="Preço (R$)",
            )
            fig_comp.update_traces(texttemplate="R$%{text:.2f}", textposition="outside")
            t_p = _get_tema()
            fig_comp.update_layout(
                height=360,
                margin=dict(l=0, r=0, t=20, b=80),
                **_plotly_base(t_p),
                xaxis=dict(showgrid=False, tickangle=-20),
                yaxis=dict(showgrid=True, title="Preço (R$)", **_plotly_axes(t_p)),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_comp, width="stretch")
    else:
        st.info("Nenhuma banana encontrada para o gráfico comparativo.")


# ---------------------------------------------------------------------------
# Aba 5 — Caixas das Lojas
# ---------------------------------------------------------------------------

def _tabela_caixas_fig(df_exibir: pd.DataFrame) -> "go.Figure":
    """Gera figura Plotly da tabela de caixas para exportação."""
    t = _get_tema()
    colunas_header = ["Data", "Loja", "Nº Loja",
                      "Benverde", "CCJ", "Bananas", "Total", "Entregue?"]
    colunas_dados  = ["data", "loja", "n_loja",
                      "caixas_benverde", "caixas_ccj", "caixas_bananas", "total", "entregue"]

    valores = []
    for col in colunas_dados:
        if col in df_exibir.columns:
            if col == "data":
                vals = df_exibir[col].apply(
                    lambda d: (
                        __import__("datetime").datetime.strptime(str(d), "%Y-%m-%d").strftime("%d/%m/%Y")
                        if len(str(d)) == 10 else str(d)
                    )
                ).tolist()
            else:
                vals = df_exibir[col].tolist()
            valores.append(vals)
        else:
            valores.append([""] * len(df_exibir))

    n = len(df_exibir)
    cores_linha = [t["tr_par"] if i % 2 == 0 else t["tr_impar"] for i in range(n)]
    cell_colors = [cores_linha] * (len(colunas_header) - 1)
    # Coluna Entregue com badge colorido
    entregue_cores = [
        "#d1fae5" if str(v).lower() == "sim" else "#fee2e2"
        for v in df_exibir["entregue"].tolist()
    ] if "entregue" in df_exibir.columns else cores_linha
    cell_colors.append(entregue_cores)

    fig = go.Figure(data=[go.Table(
        columnwidth=[90, 160, 70, 100, 80, 100, 70, 90],
        header=dict(
            values=colunas_header,
            fill_color=t["header_table"],
            font=dict(color="white", size=11, family="DM Sans"),
            align="center",
            height=34,
            line_color=t["tabela_borda"],
        ),
        cells=dict(
            values=valores,
            fill_color=t["cell_color"],
            font=dict(color=t["cell_font"], size=11, family="DM Sans"),
            align=["center", "left", "center", "center", "center", "center", "center", "center"],
            height=28,
            line_color=t["tabela_borda"],
        ),
    )])
    fig.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor=t["table_paper"],
        font=dict(color=t["plot_font"], family="DM Sans"),
        title=dict(
            text="Relatório de Caixas — Benverde",
            font=dict(size=14, family="DM Sans", color=t["table_title"]),
            x=0.01,
        ),
    )
    return fig


def _render_aba_caixas() -> None:
    """Renderiza a aba de caixas das lojas Semar."""
    st.markdown("### 📦 Caixas das Lojas Semar")

    df = st.session_state.get("caixas_lojas")
    if df is None or (hasattr(df, "empty") and df.empty):
        st.warning("⚠️ Nenhum registro encontrado. Aguarde os registros das lojas ou clique em **Atualizar Dados**.")
        return

    # ---- BLOCO 1 — Métricas ----
    df_nao = df[df["entregue"].astype(str).str.lower() == "não"]
    total_benverde = int(df_nao["caixas_benverde"].sum())
    total_ccj      = int(df_nao["caixas_ccj"].sum())
    total_bananas  = int(df_nao["caixas_bananas"].sum())

    if not df_nao.empty:
        idx_max = df_nao.groupby("n_loja")["total"].sum().idxmax()
        loja_max_row = df_nao[df_nao["n_loja"] == idx_max].iloc[0]
        loja_max_total = int(df_nao[df_nao["n_loja"] == idx_max]["total"].sum())
        loja_max_label = f"Loja {idx_max} — {loja_max_row['loja']} ({loja_max_total} cx)"
    else:
        loja_max_label = "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 Caixas Benverde a buscar", total_benverde)
    c2.metric("📦 Caixas CCJ a buscar",      total_ccj)
    c3.metric("🍌 Caixas Bananas a buscar",  total_bananas)
    c4.metric("🏆 Loja com mais caixas",     loja_max_label)

    st.markdown("---")

    # ---- BLOCO 2 — Filtros ----
    datas_unicas = sorted(df["data"].dropna().unique().tolist(), reverse=True)
    datas_fmt    = [
        __import__("datetime").datetime.strptime(d, "%Y-%m-%d").strftime("%d/%m/%Y")
        if len(str(d)) == 10 else str(d)
        for d in datas_unicas
    ]
    lojas_unicas = (
        df[["n_loja", "loja"]].drop_duplicates()
        .sort_values("n_loja")
        .apply(lambda r: f"Loja {r['n_loja']} — {r['loja']}", axis=1)
        .tolist()
    )

    col1, col2, col3 = st.columns(3)
    sel_data    = col1.selectbox("📅 Data",      ["Todas as datas"] + datas_fmt,  key="fil_cx_data")
    sel_loja    = col2.selectbox("🏪 Loja",      ["Todas as lojas"] + lojas_unicas, key="fil_cx_loja")
    sel_entregue = col3.selectbox("Entregue?",   ["Todos", "sim", "não"],           key="fil_cx_entregue")

    df_filtrado = df.copy()
    if sel_data != "Todas as datas":
        data_iso = __import__("datetime").datetime.strptime(sel_data, "%d/%m/%Y").strftime("%Y-%m-%d")
        df_filtrado = df_filtrado[df_filtrado["data"].astype(str) == data_iso]
    if sel_loja != "Todas as lojas":
        n_sel = int(sel_loja.split(" — ")[0].replace("Loja ", ""))
        df_filtrado = df_filtrado[df_filtrado["n_loja"] == n_sel]
    if sel_entregue != "Todos":
        df_filtrado = df_filtrado[df_filtrado["entregue"].astype(str).str.lower() == sel_entregue]

    df_filtrado = df_filtrado.sort_values(
        ["data", "n_loja"], ascending=[False, True]
    ).reset_index(drop=True)

    # ---- BLOCO 3 — Tabela HTML estilizada ----
    if df_filtrado.empty:
        st.info("Nenhum registro com os filtros aplicados.")
    else:
        t = _get_tema()
        linhas_html = ""
        for i, row in df_filtrado.iterrows():
            bg = t["tr_par"] if i % 2 == 0 else t["tr_impar"]
            data_fmt = (
                __import__("datetime").datetime.strptime(str(row["data"]), "%Y-%m-%d").strftime("%d/%m/%Y")
                if len(str(row["data"])) == 10 else str(row["data"])
            )
            entregue_val = str(row.get("entregue", "")).lower()
            if entregue_val == "sim":
                badge = f"<span style='background:{t['badge_sim_bg']};color:{t['badge_sim_fg']};border:1px solid {t['badge_sim_bd']};padding:2px 10px;border-radius:99px;font-size:0.78rem;font-weight:600'>sim</span>"
            else:
                badge = f"<span style='background:{t['badge_nao_bg']};color:{t['badge_nao_fg']};border:1px solid {t['badge_nao_bd']};padding:2px 10px;border-radius:99px;font-size:0.78rem;font-weight:600'>não</span>"

            linhas_html += (
                f"<tr style='background:{bg}'>"
                f"<td style='padding:0.5rem 0.8rem'>{data_fmt}</td>"
                f"<td style='padding:0.5rem 0.8rem'>{row.get('loja','')}</td>"
                f"<td style='padding:0.5rem 0.8rem;text-align:center'>{row.get('n_loja','')}</td>"
                f"<td style='padding:0.5rem 0.8rem;text-align:center'>{row.get('caixas_benverde',0)}</td>"
                f"<td style='padding:0.5rem 0.8rem;text-align:center'>{row.get('caixas_ccj',0)}</td>"
                f"<td style='padding:0.5rem 0.8rem;text-align:center'>{row.get('caixas_bananas',0)}</td>"
                f"<td style='padding:0.5rem 0.8rem;text-align:center;font-weight:700'>{row.get('total',0)}</td>"
                f"<td style='padding:0.5rem 0.8rem;text-align:center'>{badge}</td>"
                f"</tr>"
            )

        tabela_html = f"""
        <div style="overflow-x:auto;border:1px solid {t['tabela_borda']};border-radius:10px;margin-top:0.5rem">
        <table style="width:100%;border-collapse:collapse;font-size:0.87rem">
            <thead>
                <tr style="background:{t['thead_caixas']}">
                    <th style="padding:0.65rem 0.8rem;text-align:left;color:white;font-weight:600">DATA</th>
                    <th style="padding:0.65rem 0.8rem;text-align:left;color:white;font-weight:600">LOJA</th>
                    <th style="padding:0.65rem 0.8rem;text-align:center;color:white;font-weight:600">Nº LOJA</th>
                    <th style="padding:0.65rem 0.8rem;text-align:center;color:white;font-weight:600">CAIXAS BENVERDE</th>
                    <th style="padding:0.65rem 0.8rem;text-align:center;color:white;font-weight:600">CAIXAS CCJ</th>
                    <th style="padding:0.65rem 0.8rem;text-align:center;color:white;font-weight:600">CAIXAS BANANAS</th>
                    <th style="padding:0.65rem 0.8rem;text-align:center;color:white;font-weight:600">TOTAL</th>
                    <th style="padding:0.65rem 0.8rem;text-align:center;color:white;font-weight:600">ENTREGUE?</th>
                </tr>
            </thead>
            <tbody>{linhas_html}</tbody>
        </table>
        </div>
        """
        st.markdown(tabela_html, unsafe_allow_html=True)
        st.caption(f"Total: {len(df_filtrado)} registro(s)")

    # ---- BLOCO 4 — Exportação ----
    st.markdown("---")
    col_png, col_jpg, col_esp = st.columns([1, 1, 6])
    try:
        import kaleido  # noqa: F401
        fig_exp = _tabela_caixas_fig(df_filtrado if not df_filtrado.empty else df)
        col_png.download_button(
            "⬇ PNG",
            fig_exp.to_image(format="png", width=1600, scale=2),
            "relatorio_caixas.png", "image/png",
            width="stretch",
        )
        col_jpg.download_button(
            "⬇ JPG",
            fig_exp.to_image(format="jpeg", width=1600, scale=2),
            "relatorio_caixas.jpg", "image/jpeg",
            width="stretch",
        )
    except ImportError:
        col_png.caption("⚠️ Exportação indisponível. Execute: `pip install kaleido`")


# ---------------------------------------------------------------------------
# Layout principal
# ---------------------------------------------------------------------------

def main() -> None:
    """Ponto de entrada do aplicativo Streamlit."""
    _init_state()
    _render_css_tema()
    _render_sidebar()

    # ---- Header ----
    st.markdown("""
    <div class="app-header">
        <div style="font-size:2.4rem;line-height:1">🌿</div>
        <div>
            <h1>Mita</h1>
            <p>Oie! eu sou a Mita, sua gerente de dados da Benverde! como posso te ajudar hoje?</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ---- Botão global Atualizar ----
    col_btn, col_status_inline = st.columns([2, 6])
    atualizar = col_btn.button("🔄 Atualizar Dados", type="primary", width="stretch")
    if st.session_state["ultima_atualizacao"]:
        col_status_inline.caption(
            f"Última atualização: **{st.session_state['ultima_atualizacao'].strftime('%d/%m/%Y às %H:%M:%S')}**"
            + (f"  ·  ⚠️ {st.session_state['erro_carregamento']}"
               if st.session_state["erro_carregamento"] else "")
        )
    else:
        col_status_inline.caption("Dados ainda não carregados.")

    if atualizar:
        with st.spinner("⏳ Carregando dados... aguarde."):
            carregar_dados()
        if st.session_state["dados_carregados"]:
            st.success("✅ Dados atualizados com sucesso!")
        else:
            st.error("❌ Falha ao carregar dados. Verifique os caminhos na sidebar.")
        st.rerun()

    # Carrega automaticamente na primeira visita
    if not st.session_state["dados_carregados"] and st.session_state["ultima_atualizacao"] is None:
        with st.spinner("⏳ Carregando dados pela primeira vez..."):
            carregar_dados()
        st.rerun()

    st.markdown("<div style='margin-top:0.5rem'></div>", unsafe_allow_html=True)

    # ---- Abas ----
    aba_chat, aba_metas, aba_estoque, aba_precos, aba_caixas = st.tabs([
        "🤖 Mita Chat",
        "🎯 Metas e Vendas",
        "🍌 Estoque de Bananas",
        "💰 Preços Concorrentes",
        "📦 Caixas das Lojas",
    ])

    with aba_chat:
        _render_aba_chat()

    with aba_metas:
        _render_aba_metas()

    with aba_estoque:
        _render_aba_estoque()

    with aba_precos:
        _render_aba_precos()

    with aba_caixas:
        _render_aba_caixas()


if __name__ == "__main__":
    main()
