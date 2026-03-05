Em bv_9m4k2r.py, implemente um toggle de tema claro/escuro
na sidebar. A preferência fica em st.session_state e o CSS/
cores são aplicados condicionalmente em todo o app.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PASSO 1 — DICIONÁRIOS DE TEMA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Logo após os imports, crie dois dicts globais:

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
}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PASSO 2 — HELPER get_tema()
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Crie a função:

def _get_tema() -> dict:
    if st.session_state.get("tema_escuro", True):
        return _TEMA_DARK
    return _TEMA_LIGHT

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PASSO 3 — _init_state
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Em _init_state(), adicione o default:
    "tema_escuro": True

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PASSO 4 — CSS DINÂMICO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Substitua o grande bloco st.markdown(<style>)
global por uma função:

def _render_css_tema() -> None:
    t = _get_tema()
    st.markdown(f"""
<style>
    @import url('...');  /* mesma fonte DM Sans */

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
        display: flex; align-items: center; gap: 1rem;
        box-shadow: 0 4px 24px rgba(0,0,0,0.15),
                    inset 0 1px 0 rgba(255,255,255,0.1);
    }}
    .app-header h1 {{ color: {t['texto']}; font-size:1.75rem;
                      font-weight:700; margin:0; }}
    .app-header p  {{ color: {t['texto_suave']}; margin:0;
                      font-size:0.9rem; }}
    .metric-card {{
        background: var(--glass-bg);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid var(--glass-border);
        border-radius: 14px; padding: 1.2rem 1.4rem;
        box-shadow: 0 4px 16px rgba(0,0,0,0.1),
                    inset 0 1px 0 rgba(255,255,255,0.08);
    }}
    .metric-card .label {{ color: {t['texto_suave']}; font-size:0.78rem;
        font-weight:500; text-transform:uppercase; letter-spacing:0.06em; }}
    .metric-card .value {{ color: #4caf7d; font-size:2rem; font-weight:700; }}
    .metric-card .delta {{ color: {t['texto_suave']}; font-size:0.82rem; }}
    .metric-card .delta.pos {{ color: #4caf7d; }}
    .metric-card .delta.neg {{ color: var(--laranja); }}
    .progress-wrap {{
        background: {t['glass_border']};
        border-radius: 99px; height: 10px; overflow: hidden;
    }}
    .progress-bar {{ height:10px; border-radius:99px;
                     transition: width 0.4s ease; }}
    .badge {{ display:inline-block; padding:0.2rem 0.65rem;
              border-radius:99px; font-size:0.72rem; font-weight:500;
              letter-spacing:0.04em; text-transform:uppercase; }}
    .badge-green  {{ background:rgba(16,185,129,0.20); color:#6ee7b7;
                     border:1px solid rgba(16,185,129,0.30); }}
    .badge-yellow {{ background:rgba(245,200,66,0.15); color:#d97706;
                     border:1px solid rgba(245,200,66,0.30); }}
    .badge-red    {{ background:rgba(239,68,68,0.15); color:#fca5a5;
                     border:1px solid rgba(239,68,68,0.30); }}
    .badge-blue   {{ background:rgba(99,102,241,0.15); color:#c7d2fe;
                     border:1px solid rgba(99,102,241,0.30); }}
    .banana-row   {{ background: {t['banana_row']} !important; }}
    .sidebar-status {{
        background: rgba(76,175,125,0.12);
        border-left: 3px solid #4caf7d;
        padding: 0.75rem 1rem; border-radius: 0 8px 8px 0;
        font-size: 0.82rem; color: {t['texto']}; margin-bottom:0.75rem;
    }}
    .sidebar-status strong {{ color: #4caf7d; }}
    .chat-container {{ max-height:520px; overflow-y:auto; padding:0.5rem; }}
    [data-testid="stChatMessage"] p,
    [data-testid="stChatMessage"] li,
    [data-testid="stChatMessage"] span {{
        font-size:1.05rem !important; line-height:1.75 !important; }}
    [data-testid="stChatMessage"] strong {{
        font-size:1.05rem !important; color:#4caf7d; }}
    [data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) {{
        background: rgba(76,175,125,0.10);
        border: 1px solid rgba(76,175,125,0.20);
        border-radius:12px; padding:0.5rem 0.75rem; margin-bottom:0.5rem;
    }}
    .stTabs [data-baseweb="tab-list"] {{
        gap:4px; background: var(--glass-bg);
        padding:4px; border-radius:12px;
        border:1px solid var(--glass-border);
    }}
    .stTabs [data-baseweb="tab"] {{
        border-radius:10px; padding:0.45rem 1.1rem;
        font-weight:500; font-size:0.88rem;
        color: {t['texto_suave']} !important;
        background: transparent !important;
    }}
    .stTabs [aria-selected="true"] {{
        background: rgba(76,175,125,0.20) !important;
        color: #4caf7d !important;
        box-shadow: 0 1px 4px rgba(0,0,0,0.15);
    }}
    .stTabs [data-baseweb="tab-panel"] {{ background:transparent !important; }}
    .stButton > button {{
        border-radius:12px; font-weight:500; font-size:0.88rem;
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
    .stTextInput input {{
        background: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        border-radius:10px !important;
        color: {t['texto']} !important;
        caret-color: {t['texto']} !important;
    }}
    .stTextInput input::placeholder {{
        color: {t['texto_suave']} !important; }}
    .stTextInput input:focus {{
        border-color: #4caf7d !important;
        box-shadow: 0 0 0 2px rgba(76,175,125,0.20) !important;
    }}
    div[data-testid="stMetricValue"] {{
        font-family: 'DM Mono', monospace;
        font-size: 1.8rem !important;
        color: #4caf7d !important;
    }}
    div[data-testid="stMetric"] {{
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        border-radius:14px; padding:1rem 1.2rem;
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
    }}
    [data-testid="stExpander"] {{
        background: var(--glass-bg) !important;
        border: 1px solid var(--glass-border) !important;
        border-radius:12px !important;
        backdrop-filter: blur(12px);
    }}
    div[data-testid="stDialog"] > div > div {{
        max-width:900px; width:90vw;
        background: var(--glass-bg) !important;
        backdrop-filter: blur(24px) !important;
        border: 1px solid var(--glass-border) !important;
        border-radius:20px !important;
    }}
</style>
""", unsafe_allow_html=True)

Chame _render_css_tema() no início de main(),
ANTES de _render_sidebar() e DEPOIS de _init_state().
Remova o st.markdown global de CSS que existia antes.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PASSO 5 — TOGGLE NA SIDEBAR
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
No início de _render_sidebar(), antes de
qualquer outro elemento, adicione:

    t = _get_tema()
    col_tog, col_label = st.sidebar.columns([1, 4])
    with col_tog:
        if st.button(t["icone"], key="btn_tema",
                     help=t["toggle_label"]):
            st.session_state["tema_escuro"] = \
                not st.session_state.get("tema_escuro", True)
            st.rerun()
    with col_label:
        st.caption(t["toggle_label"])
    st.sidebar.divider()

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PASSO 6 — TABELAS HTML INLINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Em TODAS as funções que geram tabelas HTML
(_render_aba_metas, _render_aba_precos,
_render_aba_caixas), adicione t = _get_tema()
no início e substitua:

Linhas alternadas:
  DE:  "white" if i%2==0 else "#f9fafb"
  PARA: t["tr_par"] if i%2==0 else t["tr_impar"]

Header th cor:
  DE:  color:#374151
  PARA: color:{t['texto_th']}  (use f-string)

Header tr background:
  DE:  background:#f9fafb;border-bottom:2px solid #e5e7eb
  PARA: background:{t['th_bg']};border-bottom:1px solid {t['tabela_borda']}

Wrapper da tabela borda:
  DE:  border:1px solid #e5e7eb
  PARA: border:1px solid {t['tabela_borda']}

thead caixas:
  DE:  background:#1a4731
  PARA: background:{t['thead_caixas']}

Badges sim/não (tabela caixas):
  DE sim:  background:#d1fae5;color:#065f46
  PARA: background:{t['badge_sim_bg']};color:{t['badge_sim_fg']};border:1px solid {t['badge_sim_bd']}
  DE não:  background:#fee2e2;color:#991b1b
  PARA: background:{t['badge_nao_bg']};color:{t['badge_nao_fg']};border:1px solid {t['badge_nao_bd']}

Caption color:
  DE:  color:#6b7280
  PARA: color:{t['texto_suave']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PASSO 7 — GRÁFICOS PLOTLY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Crie um helper logo após _get_tema():

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

Em TODAS as chamadas fig.update_layout(),
passe **_plotly_base(t) e adicione t=_get_tema()
antes do bloco do gráfico.

Em TODAS as chamadas fig.update_xaxes() e
fig.update_yaxes(), passe **_plotly_axes(t).

Nas duas go.Table (exportação de metas e caixas),
substitua as cores hardcoded por:
    header=dict(
        fill_color=t["header_table"],
        font=dict(color="white", size=11, family="DM Sans"),
        line_color=t["tabela_borda"],
    ),
    cells=dict(
        fill_color=t["cell_color"],
        font=dict(color=t["cell_font"], size=11, family="DM Sans"),
        line_color=t["tabela_borda"],
    ),

E paper_bgcolor das go.Table (usadas em exportação):
    paper_bgcolor=t["table_paper"],
    title=dict(..., color=t["table_title"])

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESTRIÇÕES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- NÃO altere _render_pagina_login() nem
  o CSS dentro dela — ela tem seu próprio
  sistema visual independente
- NÃO altere auth.py
- NÃO altere lógica de dados, só visual
- O toggle deve funcionar instantaneamente
  via st.rerun() sem recarregar dados