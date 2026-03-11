"""Página dedicada para usuários com funcionalidade de busca de preços."""

import io
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import streamlit as st

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from auth import get_user
from data_processor import load_precos

st.set_page_config(
    page_title="Busca de Preços — Benverde",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

_TEMA_DARK = {
    "app_bg": "radial-gradient(ellipse at 25% 15%, #1a4731 0%, #0d2518 55%, #060e09 100%)",
    "glass_bg": "rgba(255,255,255,0.07)",
    "glass_border": "rgba(255,255,255,0.15)",
    "texto": "rgba(255,255,255,0.97)",
    "texto_suave": "rgba(230,240,255,0.82)",
    "texto_th": "rgba(255,255,255,0.92)",
    "tabela_borda": "rgba(255,255,255,0.10)",
    "th_bg": "rgba(255,255,255,0.06)",
    "tr_par": "rgba(255,255,255,0.04)",
    "tr_impar": "rgba(255,255,255,0.02)",
    "banana_row": "rgba(245,200,66,0.10)",
    "plot_font": "rgba(255,255,255,0.75)",
    "gridcolor": "rgba(255,255,255,0.08)",
    "zeroline": "rgba(255,255,255,0.12)",
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "icone": "🌙",
    "toggle_label": "☀️ Modo claro",
}

_TEMA_LIGHT = {
    "app_bg": "radial-gradient(ellipse at 25% 15%, #e8f5ee 0%, #c8e6d4 55%, #f0faf4 100%)",
    "glass_bg": "rgba(255,255,255,0.55)",
    "glass_border": "rgba(26,71,49,0.15)",
    "texto": "#1a4731",
    "texto_suave": "#4b7a62",
    "texto_th": "#1a4731",
    "tabela_borda": "rgba(26,71,49,0.15)",
    "th_bg": "rgba(26,71,49,0.07)",
    "tr_par": "rgba(255,255,255,0.70)",
    "tr_impar": "rgba(232,245,238,0.50)",
    "banana_row": "rgba(245,200,66,0.18)",
    "plot_font": "#1a4731",
    "gridcolor": "rgba(26,71,49,0.10)",
    "zeroline": "rgba(26,71,49,0.18)",
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "icone": "☀️",
    "toggle_label": "🌙 Modo escuro",
}


def _get_tema() -> dict:
    return _TEMA_DARK if st.session_state.get("tema_escuro", True) else _TEMA_LIGHT


def _cor_preco(preco, referencia) -> str:
    try:
        if float(preco) < float(referencia):
            return "color:#2d7a4f;font-weight:350"
        if float(preco) > float(referencia):
            return "color:#e8843a;font-weight:350"
    except (TypeError, ValueError):
        pass
    return ""


def _plotly_base(t: dict) -> dict:
    return dict(
        paper_bgcolor=t["paper_bgcolor"],
        plot_bgcolor=t["plot_bgcolor"],
        font=dict(color=t["plot_font"], family="DM Sans"),
    )


def _plotly_axes(t: dict) -> dict:
    return dict(gridcolor=t["gridcolor"], zerolinecolor=t["zeroline"])


@st.cache_data(ttl=300)
def _load_precos_cache() -> dict:
    return load_precos("dados/precos")


@st.cache_data(show_spinner=False)
def _exportar_precos_cache(tabela_json: str) -> tuple[bytes, bytes]:
    dados = json.loads(tabela_json)
    cols = dados["cols"]
    rows = dados["rows"]
    n_rows = len(rows)
    fig_h = max(4, n_rows * 0.38 + 1.2)
    fig, ax = plt.subplots(figsize=(18, fig_h))
    ax.axis("off")
    fig.patch.set_facecolor("#0d2518")

    tbl = ax.table(cellText=rows, colLabels=cols, cellLoc="left", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.auto_set_column_width(range(len(cols)))

    for j in range(len(cols)):
        cell = tbl[0, j]
        cell.set_facecolor("#2d7a4f")
        cell.set_text_props(color="white", fontweight="bold")
        cell.set_edgecolor("#4caf7d")

    idx_prod = cols.index("Produto") if "Produto" in cols else 0
    for i in range(1, n_rows + 1):
        row = rows[i - 1]
        is_banana = "BANANA" in str(row[idx_prod]).upper()
        bg = "#5a4b12" if is_banana else ("#1a3d2b" if i % 2 == 0 else "#122d1f")
        for j in range(len(cols)):
            cell = tbl[i, j]
            cell.set_facecolor(bg)
            cell.set_text_props(color=(1, 1, 1, 0.85))
            cell.set_edgecolor("#1e4a30")

    ax.set_title("Busca de Preços — Benverde", color="#f5c842", fontsize=13, fontweight="bold", pad=10)
    png_buf = io.BytesIO()
    fig.savefig(png_buf, format="png", bbox_inches="tight", dpi=150, facecolor=fig.get_facecolor())
    pdf_buf = io.BytesIO()
    fig.savefig(pdf_buf, format="pdf", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return png_buf.getvalue(), pdf_buf.getvalue()


if not st.session_state.get("autenticado", False):
    st.switch_page("pages/bv_9m4k2r.py")
    st.stop()

user = get_user(st.session_state.get("username_logado", "") or "")
func = (user or {}).get("funcionalidade", "administracao geral")
if func != "busca de precos" and not (user or {}).get("is_admin", False):
    st.switch_page("pages/bv_9m4k2r.py")
    st.stop()

if "tema_escuro" not in st.session_state:
    st.session_state["tema_escuro"] = True

t = _get_tema()
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
html, body, [class*="css"] {{ font-family: 'DM Sans', sans-serif; }}
[data-testid='stSidebarNav'], [data-testid='stSidebarCollapsedControl'], section[data-testid='stSidebar'] {{ display: none !important; }}
.stApp {{ background: {t['app_bg']} !important; background-attachment: fixed !important; }}
[data-testid="stHeader"] {{ background: transparent !important; }}
.block-container {{ padding-top: 1.2rem; }}
.header {{ background: {t['glass_bg']}; border:1px solid {t['glass_border']}; border-radius:14px; padding:1rem 1.3rem; }}
.badge {{ display:inline-block; padding:.2rem .5rem; border-radius:999px; font-size:.76rem; font-weight:600; margin-right:.4rem; }}
.badge-green {{ background:rgba(16,185,129,.2); color:#6ee7b7; }}
.badge-red {{ background:rgba(239,68,68,.2); color:#fca5a5; }}
.badge-blue {{ background:rgba(59,130,246,.2); color:#93c5fd; }}
.stApp, .stApp p, .stApp span, .stApp div, .stApp label, .stApp li, .stApp small, .stMarkdown p, .stMarkdown span, .stMarkdown div, td, th {{ color: {t['texto']} !important; }}
.stTextInput input, .stSelectbox [data-baseweb="select"] > div {{ background:{t['glass_bg']} !important; border:1px solid {t['glass_border']} !important; color:{t['texto']} !important; }}
.stButton > button {{ border-radius:10px !important; }}
</style>
""", unsafe_allow_html=True)

c1, c2, c3 = st.columns([6, 1.4, 1.1])
with c1:
    st.markdown("<div class='header'><h2>💰 Busca de Preços</h2></div>", unsafe_allow_html=True)
with c2:
    if st.button(t["icone"], help=t["toggle_label"], use_container_width=True):
        st.session_state["tema_escuro"] = not st.session_state.get("tema_escuro", True)
        st.rerun()
with c3:
    if st.button("Sair", use_container_width=True):
        st.session_state.clear()
        st.switch_page("pages/bv_9m4k2r.py")

col_ref, col_info = st.columns([1, 2])
if col_ref.button("🔄 Atualizar", use_container_width=True):
    _load_precos_cache.clear()
precos = _load_precos_cache()

if not precos:
    st.warning("⚠️ Nenhuma pesquisa de preços encontrada.")
    st.stop()

datas_disponiveis = list(precos.keys())
opcoes = ["GERAL"] + datas_disponiveis
data_selecionada = col_ref.selectbox("📅 Data", opcoes)
col_info.markdown(f"<br><span class='badge badge-blue'>{len(datas_disponiveis)} pesquisa(s)</span>", unsafe_allow_html=True)

if data_selecionada == "GERAL":
    col_prod_ref = None
    frames = []
    for df_tmp in precos.values():
        if df_tmp is not None and not df_tmp.empty:
            frames.append(df_tmp)
            if col_prod_ref is None:
                col_prod_ref = "Produto Buscado" if "Produto Buscado" in df_tmp.columns else df_tmp.columns[0]
    if not frames:
        st.info("Nenhum dado disponível.")
        st.stop()
    df_todos = pd.concat(frames, ignore_index=True)
    col_precos_med = [c for c in df_todos.columns if "preço" in c.lower() or "preco" in c.lower()]
    mask_limpo = (
        df_todos[col_prod_ref].astype(str).str.len().le(50)
        & ~df_todos[col_prod_ref].astype(str).str.contains(",")
        & ~df_todos[col_prod_ref].astype(str).str.contains(r'\d{1,2}[.,]\d{2}', regex=True)
    )
    df_todos = df_todos[mask_limpo].copy()
    df_todos[col_prod_ref] = df_todos[col_prod_ref].astype(str).str.strip().str.upper()
    df_preco = (
        df_todos.replace(0, float("nan")).groupby(col_prod_ref, as_index=False)[col_precos_med].mean().round(2)
    )
    df_preco.insert(0, "Produto Buscado", df_preco.pop(col_prod_ref))
else:
    df_preco = precos.get(data_selecionada)
    if df_preco is None or df_preco.empty:
        st.info("Nenhum dado para a data selecionada.")
        st.stop()

col_b1, col_b2 = st.columns([3, 1])
busca_preco = col_b1.text_input("🔍 Buscar produto", placeholder="Ex.: BANANA")
mostrar_apenas_bananas = col_b2.checkbox("🍌 Só bananas")

df_exibir = df_preco.copy()
col_prod_buscado = "Produto Buscado" if "Produto Buscado" in df_exibir.columns else df_exibir.columns[0]
if mostrar_apenas_bananas:
    df_exibir = df_exibir[df_exibir[col_prod_buscado].astype(str).str.upper().str.contains("BANANA")]
elif busca_preco:
    df_exibir = df_exibir[df_exibir[col_prod_buscado].astype(str).str.upper().str.contains(busca_preco.upper())]

col_precos = [c for c in df_exibir.columns if "preço" in c.lower() or "preco" in c.lower()]
col_status = [c for c in df_exibir.columns if "status" in c.lower()]

st.markdown(f"**{len(df_exibir)} produto(s)**")
if not df_exibir.empty:
    colunas_exibir_h = [col_prod_buscado] + [c for c in df_exibir.columns if c != col_prod_buscado]
    header_html = "".join(
        f"<th style='padding:0.7rem 0.8rem;text-align:{'right' if c in col_precos else 'left'};color:{t['texto_th']}'>{c}</th>"
        for c in colunas_exibir_h
    )
    linhas_preco_html = ""
    for i, (_, row) in enumerate(df_exibir.iterrows()):
        produto = str(row.get(col_prod_buscado, ""))
        is_banana = "BANANA" in produto.upper()
        if is_banana:
            bg = t["banana_row"]
        else:
            bg = t["tr_par"] if i % 2 == 0 else t["tr_impar"]
        valores_preco = [float(row[c]) for c in col_precos if c in row and pd.notna(row[c]) and row[c] != 0]
        ref_preco = min(valores_preco) if valores_preco else None
        celulas = ""
        for c in colunas_exibir_h:
            val = row.get(c, "")
            if c in col_precos and pd.notna(val) and val != 0:
                estilo_cor = _cor_preco(val, ref_preco) if ref_preco else ""
                celulas += f"<td style='padding:0.55rem 0.8rem;text-align:right;{estilo_cor}'>R$ {float(val):,.2f}</td>"
            elif c in col_status:
                s = str(val).strip()
                badge = "<span class='badge badge-green'>OK</span>" if s.lower()=="ok" else ("<span class='badge badge-red'>Indisponível</span>" if "indispon" in s.lower() else f"<span class='badge badge-blue'>{s}</span>")
                celulas += f"<td style='padding:0.55rem 0.8rem'>{badge}</td>"
            else:
                prefix = "🍌 " if is_banana and c == col_prod_buscado else ""
                celulas += f"<td style='padding:0.55rem 0.8rem'>{prefix}{val}</td>"
        linhas_preco_html += f"<tr style='background:{bg};'>{celulas}</tr>"
    st.markdown(f"""
    <div style="overflow-x:auto;border:1px solid {t['tabela_borda']};border-radius:10px">
    <table style="width:100%;border-collapse:collapse;font-size:0.86rem">
      <thead><tr style="background:{t['th_bg']};border-bottom:1px solid {t['tabela_borda']}">{header_html}</tr></thead>
      <tbody>{linhas_preco_html}</tbody>
    </table></div>
    """, unsafe_allow_html=True)

st.markdown("#### 📊 Comparativo de Preços — Bananas")
df_bananas = df_preco[df_preco[col_prod_buscado].astype(str).str.upper().str.contains("BANANA")].copy()
if not df_bananas.empty and col_precos:
    df_melt = df_bananas[[col_prod_buscado] + col_precos].melt(id_vars=col_prod_buscado, value_vars=col_precos, var_name="Supermercado", value_name="Preço (R$)").dropna(subset=["Preço (R$)"])
    df_melt = df_melt[df_melt["Preço (R$)"] > 0]
    df_melt["Supermercado"] = df_melt["Supermercado"].str.replace(r"Preço\s*\(?\s*", "", regex=True).str.replace(r"\)\s*$", "", regex=True).str.strip()
    if not df_melt.empty:
        fig_comp = px.bar(df_melt, x=col_prod_buscado, y="Preço (R$)", color="Supermercado", barmode="group", text="Preço (R$)")
        fig_comp.update_traces(texttemplate="R$%{text:.2f}", textposition="outside")
        fig_comp.update_layout(height=360, margin=dict(l=0, r=0, t=20, b=80), **_plotly_base(t), xaxis=dict(showgrid=False, tickangle=-20), yaxis=dict(showgrid=True, title="Preço (R$)", **_plotly_axes(t)))
        st.plotly_chart(fig_comp, width="stretch")

if not df_exibir.empty:
    df_export = df_exibir.copy()
    df_export = df_export.rename(columns={col_prod_buscado: "Produto"})
    cols = ["Produto"] + [c for c in df_export.columns if c != "Produto" and c in col_precos]
    df_export = df_export[cols]
    for c in [x for x in df_export.columns if x != "Produto"]:
        df_export[c] = df_export[c].apply(lambda v: f"R$ {float(v):.2f}" if pd.notna(v) and v != 0 else "-")
    tabela_json = json.dumps({"cols": list(df_export.columns), "rows": df_export.astype(str).values.tolist()}, ensure_ascii=False)
    png_bytes, pdf_bytes = _exportar_precos_cache(tabela_json)
    sufixo = data_selecionada.replace("/", "-").replace(" ", "_")
    d1, d2 = st.columns(2)
    d1.download_button("📥 Baixar PNG", png_bytes, file_name=f"precos_{sufixo}.png", mime="image/png", use_container_width=True)
    d2.download_button("📄 Baixar PDF", pdf_bytes, file_name=f"precos_{sufixo}.pdf", mime="application/pdf", use_container_width=True)
