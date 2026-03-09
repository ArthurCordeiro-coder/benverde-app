"""
pages/1_Registro_Estoque.py
Página isolada para registro manual de estoque de bananas.

Acesso via: http://localhost:8501/Registro_Estoque

NÃO acessa session_state de app.py nem dados de vendas/preços.
Única dependência externa: data_processor.py (leitura/escrita do JSON de estoque).
"""

import os
import re as _re
import sys
import tempfile
from datetime import datetime, date

import pdfplumber
import streamlit as st

# Garante que data_processor.py seja encontrado mesmo rodando a partir de pages/
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from data_processor import (
    salvar_movimentacao_manual,
    load_movimentacoes_manuais,
    deletar_movimentacao_manual,
    extrair_bananas_pdf_upload,
    extrair_pedido_semar,
)

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Registro de Estoque — Benverde",
    page_icon="🍌",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Esconde navegação e botão de expandir sidebar
st.markdown("""
<style>
    [data-testid='stSidebarNav'] { display: none !important; }
    [data-testid='stSidebarCollapsedControl'] { display: none !important; }
    section[data-testid='stSidebar'] { display: none !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# CSS customizado (consistente com app.py)
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

    :root {
        --verde-escuro: #1a4731;
        --verde-medio:  #2d7a4f;
        --borda:        #e5e7eb;
        --cinza-fundo:  #f9fafb;
        --amarelo-btn:  #e8843a;
        --vermelho-btn: #8B1A1A;
    }

    .page-header {
        background: linear-gradient(135deg, #7a5200 0%, #b37800 100%);
        padding: 1.2rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
    }
    .page-header h1 { color: white; font-size: 1.6rem; font-weight: 700; margin: 0; }
    .page-header p  { color: rgba(255,255,255,0.8); margin: 0; font-size: 0.9rem; }

    /* Botão NF-e — amarelo/laranja */
    .btn-nfe > button {
        background-color: var(--amarelo-btn) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
    }

    /* Botão Salvar — vermelho */
    .btn-salvar > button {
        background-color: var(--vermelho-btn) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
    }

    .linha-form {
        background: white;
        border: 1px solid var(--borda);
        border-radius: 8px;
        padding: 0.6rem 1rem;
        margin-bottom: 0.4rem;
    }
    .stDataFrame { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
_CAMINHO_JSON_DEFAULT = os.path.join(_ROOT, "dados", "cache", "estoque_manual.json")

_LOJAS = [
    "Entrada",
    "Loja 01", "Loja 02", "Loja 03", "Loja 04", "Loja 05",
    "Loja 06", "Loja 07", "Loja 08", "Loja 09", "Loja 10",
    "Loja 11", "Loja 12", "Loja 13", "Loja 14", "Loja 15",
    "Loja 16", "Loja 17", "Loja 18", "Loja 19", "Loja 20",
    "Loja 21", "Loja 22", "Loja 23", "Loja 24", "Loja 25",
    "Loja 26", "Loja 27", "Loja 28", "Loja 29", "Loja 30",
    "Loja 31", "Loja 32", "Loja 33", "Loja 34", "Loja 35",
    "Loja 36", "Loja 37", "Loja 38", "Loja 39", "Loja 40",
    "Frutas/Legumes", "Outra",
]

_VARIEDADES = [
    "BANANA NANICA",
    "BANANA DA TERRA",
    "BANANA PRATA",
    "BANANA MAÇÃ",
]

# "bonificação" é contabilizada como saída de estoque, mas rastreada como tipo próprio
_TIPOS = ["entrada", "saida", "bonificação"]

_LINHA_VAZIA = {
    "sel":       False,
    "variedade": _VARIEDADES[0],
    "quant":     0.0,
    "loja":      "",
    "tipo":      "entrada",
}

# ---------------------------------------------------------------------------
# Inicialização do estado da página
# ---------------------------------------------------------------------------
def _init():
    if "linhas" not in st.session_state:
        st.session_state["linhas"] = [dict(_LINHA_VAZIA) for _ in range(5)]
    if "mostrar_upload" not in st.session_state:
        st.session_state["mostrar_upload"] = False
    if "caminho_json" not in st.session_state:
        st.session_state["caminho_json"] = _CAMINHO_JSON_DEFAULT
    if "sel_all_hdr" not in st.session_state:
        st.session_state["sel_all_hdr"] = False
    if "tipo_all_hdr" not in st.session_state:
        st.session_state["tipo_all_hdr"] = "─"

_init()


def _set_all_sel():
    """Callback: marca/desmarca todas as linhas ao toggler o checkbox do cabeçalho."""
    val = st.session_state.get("sel_all_hdr", False)
    for i in range(len(st.session_state["linhas"])):
        st.session_state["linhas"][i]["sel"] = val
        st.session_state[f"sel_{i}"] = val


def _set_all_tipo():
    """Callback: aplica o tipo selecionado no cabeçalho a todas as linhas."""
    val = st.session_state.get("tipo_all_hdr", "─")
    if val == "─":
        return
    for i in range(len(st.session_state["linhas"])):
        st.session_state["linhas"][i]["tipo"] = val
        st.session_state[f"tipo_{i}"] = val
        if val == "entrada":
            st.session_state["linhas"][i]["loja"] = "Entrada"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _adicionar_linhas(novas: list[dict]):
    """Adiciona linhas ao formulário (usada após upload de NF-e ou Semar)."""
    for item in novas:
        st.session_state["linhas"].append({
            "sel":       False,
            "variedade": item.get("produto", ""),
            "quant":     float(item.get("quant", 0.0)),
            "loja":      item.get("loja", ""),
            "tipo":      item.get("tipo", "entrada"),
        })


def _loja_semar_para_form(loja_semar: str) -> str:
    """Converte 'LOJA 13 - CARAGUA' → 'Loja 13' (formato do seletor do formulário)."""
    m = _re.search(r'LOJA\s+(\d+)', loja_semar, _re.IGNORECASE)
    if m:
        return f"Loja {int(m.group(1)):02d}"
    return "Outra"


def _detectar_tipo_pdf(caminho: str) -> str:
    """Retorna 'semar' se for Pedido de Compra Semar, senão 'danfe'."""
    try:
        with pdfplumber.open(caminho) as pdf:
            texto = pdf.pages[0].extract_text() or ""
            if "pedido de compra" in texto.lower():
                return "semar"
    except Exception:
        pass
    return "danfe"


def _linhas_validas() -> list[dict]:
    return [l for l in st.session_state["linhas"] if l["quant"] > 0]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="page-header">
  <h1>🍌 Registro de Estoque de Bananas</h1>
  <p>Registro manual de entradas e saídas · Benverde Hortifrúti</p>
</div>
""", unsafe_allow_html=True)

caminho_json = st.session_state["caminho_json"]

# ---------------------------------------------------------------------------
# Botão NF-e / upload
# ---------------------------------------------------------------------------
col_nfe, col_espaco = st.columns([2, 6])
with col_nfe:
    st.markdown('<div class="btn-nfe">', unsafe_allow_html=True)
    if st.button("📄 Enviar documento (NF-e)", use_container_width=True):
        st.session_state["mostrar_upload"] = not st.session_state["mostrar_upload"]
    st.markdown("</div>", unsafe_allow_html=True)

if st.session_state["mostrar_upload"]:
    arquivo_pdf = st.file_uploader(
        "Selecione o PDF da NF-e",
        type=["pdf"],
        key="upload_nfe",
        label_visibility="collapsed",
    )
    if arquivo_pdf is not None:
        with st.spinner("🔍 Extraindo bananas do PDF..."):
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                    tmp.write(arquivo_pdf.read())
                    tmp_path = tmp.name

                tipo_pdf = _detectar_tipo_pdf(tmp_path)

                if tipo_pdf == "semar":
                    df_semar = extrair_pedido_semar(tmp_path)
                    if not df_semar.empty:
                        itens = [
                            {
                                "produto": str(row["Produto"]),
                                "quant":   float(row["QUANT"]),
                                "loja":    _loja_semar_para_form(str(row["Loja"])),
                                "tipo":    "entrada",
                            }
                            for _, row in df_semar.iterrows()
                        ]
                        _adicionar_linhas(itens)
                        st.success(
                            f"✅ Pedido Semar: {len(itens)} linha(s) adicionada(s) "
                            f"com loja e quantidade preenchidos."
                        )
                        st.session_state["mostrar_upload"] = False
                    else:
                        st.warning("⚠️ Nenhuma banana encontrada no Pedido Semar.")
                else:
                    bananas = extrair_bananas_pdf_upload(tmp_path, arquivo_pdf.name)
                    if bananas:
                        _adicionar_linhas(bananas)
                        st.success(f"✅ {len(bananas)} item(ns) de banana encontrado(s) e adicionado(s) ao formulário.")
                        st.session_state["mostrar_upload"] = False
                    else:
                        st.warning("⚠️ Nenhuma banana encontrada no PDF.")

            except Exception as exc:
                st.error(f"❌ Erro ao processar PDF: {exc}")
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass

st.markdown("---")

# ---------------------------------------------------------------------------
# Formulário de linhas
# ---------------------------------------------------------------------------
st.markdown("#### Linhas de movimentação")

# Cabeçalho visual
hcols = st.columns([0.4, 3.5, 2, 3, 2])
hcols[0].checkbox("Todos", key="sel_all_hdr", on_change=_set_all_sel)
hcols[1].markdown("**Variedade**")
hcols[2].markdown("**Qtd (kg)**")
hcols[3].markdown("**Loja**")
hcols[4].selectbox(
    "tipo_all", options=["─"] + _TIPOS,
    key="tipo_all_hdr", label_visibility="collapsed",
    on_change=_set_all_tipo,
    help="Aplicar tipo a todas as linhas",
)

# Renderiza cada linha
for i, linha in enumerate(st.session_state["linhas"]):
    cols = st.columns([0.4, 3.5, 2, 3, 2])

    linha["sel"] = cols[0].checkbox(
        "sel", value=linha["sel"], key=f"sel_{i}", label_visibility="collapsed"
    )
    var_idx = _VARIEDADES.index(linha["variedade"]) if linha["variedade"] in _VARIEDADES else 0
    linha["variedade"] = cols[1].selectbox(
        "var", options=_VARIEDADES, index=var_idx,
        key=f"var_{i}", label_visibility="collapsed",
    )
    linha["quant"] = cols[2].number_input(
        "qtd", value=float(linha["quant"]), min_value=0.0, step=0.5,
        key=f"qtd_{i}", label_visibility="collapsed",
    )
    is_entrada = linha["tipo"] == "entrada"
    if is_entrada:
        linha["loja"] = "Entrada"
    loja_idx = _LOJAS.index(linha["loja"]) if linha["loja"] in _LOJAS else 0
    linha["loja"] = cols[3].selectbox(
        "loja", options=_LOJAS, index=loja_idx,
        key=f"loja_{i}", label_visibility="collapsed",
        disabled=is_entrada,
    )
    tipo_idx = _TIPOS.index(linha["tipo"]) if linha["tipo"] in _TIPOS else 0
    linha["tipo"] = cols[4].radio(
        "tipo", options=_TIPOS, index=tipo_idx,
        key=f"tipo_{i}", label_visibility="collapsed", horizontal=True,
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Botões de ação
# ---------------------------------------------------------------------------
col_add, col_rem, col_salvar = st.columns([2, 2, 2])

with col_add:
    if st.button("➕ Adicionar linha", use_container_width=True):
        st.session_state["linhas"].append(dict(_LINHA_VAZIA))
        st.rerun()

with col_rem:
    if st.button("🗑 Remover selecionadas", use_container_width=True):
        antes = len(st.session_state["linhas"])
        # Lê seleção direto dos widgets (session_state), não do dict auxiliar
        selecionadas = {
            i for i in range(antes)
            if st.session_state.get(f"sel_{i}", False)
        }
        st.session_state["linhas"] = [
            l for i, l in enumerate(st.session_state["linhas"])
            if i not in selecionadas
        ]
        # Limpa chaves de checkbox obsoletas
        for i in range(antes):
            st.session_state.pop(f"sel_{i}", None)
        # Garante mínimo de 1 linha vazia
        if not st.session_state["linhas"]:
            st.session_state["linhas"] = [dict(_LINHA_VAZIA)]
        removidas = antes - len(st.session_state["linhas"])
        if removidas:
            st.success(f"🗑 {removidas} linha(s) removida(s).")
        st.rerun()

with col_salvar:
    st.markdown('<div class="btn-salvar">', unsafe_allow_html=True)
    salvar_clicado = st.button("💾 Salvar", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

if salvar_clicado:
    validas = _linhas_validas()
    if not validas:
        st.warning("⚠️ Nenhuma linha com quantidade maior que zero para salvar.")
    else:
        agora = datetime.now().isoformat()
        registros_para_salvar = [
            {
                "data":     agora,
                "tipo":     l["tipo"],
                "produto":  l["variedade"].strip().upper(),
                "quant":    float(l["quant"]),
                "unidade":  "KG",
                "loja":     l["loja"],
                "arquivo":  "manual",
            }
            for l in validas
        ]
        try:
            salvar_movimentacao_manual(registros_para_salvar, caminho_json)
            st.success(f"✅ {len(registros_para_salvar)} registro(s) salvos com sucesso!")
            # Limpa o formulário
            st.session_state["linhas"] = [dict(_LINHA_VAZIA) for _ in range(5)]
            st.rerun()
        except Exception as exc:
            st.error(f"❌ Erro ao salvar: {exc}")

# ---------------------------------------------------------------------------
# Tabela de registros salvos hoje
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("#### 📋 Registros salvos hoje")

try:
    todos = load_movimentacoes_manuais(caminho_json)
    hoje = date.today().isoformat()
    # Guarda (índice_global, registro) para saber qual deletar
    hoje_registros = [
        (idx, r) for idx, r in enumerate(todos)
        if str(r.get("data", "")).startswith(hoje)
    ]

    if hoje_registros:
        from datetime import datetime as _dt
        # Cabeçalho
        hd = st.columns([1.5, 1.2, 2.5, 1.2, 1, 1.5, 0.5])
        for col, label in zip(hd, ["Hora", "Tipo", "Produto", "Qtd (kg)", "Unid", "Loja", ""]):
            col.markdown(f"**{label}**")
        st.divider()

        for idx_global, r in hoje_registros:
            hora = ""
            try:
                hora = _dt.fromisoformat(str(r.get("data", ""))).strftime("%H:%M:%S")
            except Exception:
                hora = str(r.get("data", ""))

            cols = st.columns([1.5, 1.2, 2.5, 1.2, 1, 1.5, 0.5])
            cols[0].write(hora)
            cols[1].write(r.get("tipo", ""))
            cols[2].write(r.get("produto", ""))
            cols[3].write(f"{float(r.get('quant', 0)):,.1f}")
            cols[4].write(r.get("unidade", ""))
            cols[5].write(r.get("loja", ""))
            if cols[6].button("🗑", key=f"del_reg_{idx_global}", help="Deletar registro"):
                deletar_movimentacao_manual(idx_global, caminho_json)
                st.rerun()

        st.caption(f"Total hoje: {len(hoje_registros)} registro(s)")
    else:
        st.info("Nenhum registro salvo hoje.")
except Exception as exc:
    st.warning(f"Não foi possível carregar registros: {exc}")
