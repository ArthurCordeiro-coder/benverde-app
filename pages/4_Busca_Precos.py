"""
Busca de Preços — interface Streamlit
======================================
Integra o pipeline buscar_precos.py com UI interativa:
  1. Captura tokens das lojas VipCommerce em paralelo
  2. Para produtos sem escolha salva, exibe popup de escolha
  3. Busca os preços finais e exibe/salva o resultado
"""

import csv
import io
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import pandas as pd
import streamlit as st

# ─────────────────────────────────────────────────────────────
# Importa funções do módulo buscar_precos.py
# ─────────────────────────────────────────────────────────────
_BUSCA_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "verificação dos preços dos produtos",
    )
)
if _BUSCA_DIR not in sys.path:
    sys.path.insert(0, _BUSCA_DIR)

from buscar_precos import (  # noqa: E402
    LOJAS,
    MAX_WORKERS_LOJAS,
    MAX_WORKERS_TOKENS,
    _INALTERADO,
    _criar_driver_alabarce,
    _processar_alabarce_salvo,
    _processar_loja_vc,
    buscar_produto,
    buscar_produto_alabarce,
    capturar_dados_loja,
    carregar_escolhas,
    encontrar_candidatos,
    extrair_preco,
    normalizar,
    salvar_escolhas,
)

# ─────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────
_CSV_PRODUTOS      = os.path.join(_BUSCA_DIR, "produtos.csv")
_PASTA_RESULTADOS  = r"C:\Users\pesso\OneDrive\Documentos\benverde\MeuAppGerencia\dados\precos"


# ─────────────────────────────────────────────────────────────
# Estado da sessão
# ─────────────────────────────────────────────────────────────
def _init():
    defaults = {
        # Máquina de estados: idle → capturando → escolhendo → buscando → pronto
        "busca_fase":           "idle",
        "busca_produtos":       [],
        "busca_dados_lojas":    {},
        "busca_driver_ala":     None,
        "busca_escolhas":       {},
        # Lista de (nome_produto, loja_nome, [candidatos]) aguardando escolha
        "busca_pendentes":      [],
        "busca_idx":            0,       # índice da escolha atual
        "busca_cand_sel":       None,    # candidato expandido na tela de detalhe
        "busca_resultados":     [],
        "busca_csv_path":       "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ─────────────────────────────────────────────────────────────
# Dialog de escolha (popup modal)
# ─────────────────────────────────────────────────────────────
@st.dialog("🔍 Nova escolha de produto", width="large")
def _dialogo_escolha():
    pendentes: list = st.session_state["busca_pendentes"]
    idx: int        = st.session_state["busca_idx"]

    if idx >= len(pendentes):
        st.session_state["busca_fase"] = "buscando"
        st.rerun()
        return

    nome, loja_nome, candidatos = pendentes[idx]
    n_total = len(pendentes)

    # Cabeçalho
    st.caption(f"Escolha **{idx + 1}** de **{n_total}**")
    st.markdown(f"**Produto:** `{nome}`  &nbsp;|&nbsp;  **Loja:** {loja_nome}")
    st.divider()

    cand_sel = st.session_state["busca_cand_sel"]

    # ── Tela 1: lista de candidatos ────────────────────────────
    if cand_sel is None:
        st.markdown("Selecione o produto correspondente:")

        for i, c in enumerate(candidatos):
            preco = extrair_preco(c) or c.get("preco") or "—"
            label = f"**{c.get('descricao', '?')}**  —  R$ {preco}"
            if st.button(label, key=f"cand_{idx}_{i}", use_container_width=True):
                st.session_state["busca_cand_sel"] = c
                st.rerun()

        st.divider()
        if st.button("❌ Nenhum / Pular", use_container_width=True, key=f"pular_{idx}"):
            _confirmar(None)

    # ── Tela 2: detalhes + confirmação ─────────────────────────
    else:
        c = cand_sel
        st.subheader(c.get("descricao", "?"))

        # Métricas principais
        preco      = extrair_preco(c) or c.get("preco") or "—"
        disponivel = c.get("disponivel", True)
        em_oferta  = c.get("em_oferta", False)
        oferta     = c.get("oferta") or {}
        marca_obj  = c.get("marca")
        marca      = (
            marca_obj.get("nome") if isinstance(marca_obj, dict) else marca_obj
        ) or "—"

        col1, col2, col3 = st.columns(3)
        col1.metric("Preço",      f"R$ {preco}" if preco != "—" else "—")
        col2.metric("Disponível", "✅ Sim" if disponivel else "❌ Não")
        col3.metric("Marca",      marca)

        if em_oferta and oferta.get("preco_oferta"):
            st.info(f"🏷️ Em oferta: R$ {oferta['preco_oferta']}")

        # Detalhes completos expansíveis
        with st.expander("🔎 Todos os detalhes do produto"):
            exibir = {
                k: v for k, v in c.items()
                if v is not None and v != "" and v != {} and v != []
            }
            st.json(exibir)

        st.divider()

        col_v, col_c = st.columns(2)
        with col_v:
            if st.button("◀ Voltar", use_container_width=True, key=f"voltar_{idx}"):
                st.session_state["busca_cand_sel"] = None
                st.rerun()
        with col_c:
            if st.button(
                "✅ Confirmar este produto",
                type="primary",
                use_container_width=True,
                key=f"confirmar_{idx}",
            ):
                _confirmar(c)


def _confirmar(produto):
    """Salva a escolha e avança para a próxima pendência."""
    pendentes  = st.session_state["busca_pendentes"]
    idx        = st.session_state["busca_idx"]
    nome, loja_nome, _ = pendentes[idx]

    chave      = normalizar(nome)
    chave_loja = loja_nome.lower()
    escolhas   = st.session_state["busca_escolhas"]
    escolhas.setdefault(chave_loja, {})[chave] = produto
    salvar_escolhas(escolhas)

    st.session_state["busca_idx"]      = idx + 1
    st.session_state["busca_cand_sel"] = None

    if st.session_state["busca_idx"] >= len(pendentes):
        st.session_state["busca_fase"] = "buscando"

    st.rerun()


# ─────────────────────────────────────────────────────────────
# Fase 1 — captura tokens e coleta candidatos
# ─────────────────────────────────────────────────────────────
def _fase_capturando():
    escolhas = carregar_escolhas()
    st.session_state["busca_escolhas"] = escolhas

    # Tokens VipCommerce em paralelo
    lojas_vc = [l for l in LOJAS if l.get("tipo", "vipcommerce") == "vipcommerce"]
    dados_lojas: dict = {}
    _TIMEOUT_TOKEN = 90  # segundos por loja; se passar disso, pula e continua
    ex = ThreadPoolExecutor(max_workers=min(MAX_WORKERS_TOKENS, len(lojas_vc) or 1))
    futuros = {ex.submit(capturar_dados_loja, loja): loja for loja in lojas_vc}
    try:
        for fut in as_completed(futuros, timeout=_TIMEOUT_TOKEN):
            loja = futuros[fut]
            try:
                resultado = fut.result()
                if resultado.get("token"):
                    dados_lojas[loja["nome"]] = resultado
                else:
                    st.warning(f"⚠️ Token não capturado para {loja['nome']} — loja ignorada.")
            except Exception as exc:
                st.warning(f"⚠️ Falha ao capturar token de {loja['nome']}: {exc}")
    except TimeoutError:
        lojas_ok  = list(dados_lojas.keys())
        lojas_nok = [l["nome"] for l in lojas_vc if l["nome"] not in lojas_ok]
        st.warning(f"⚠️ Timeout na captura de tokens. Lojas ignoradas: {', '.join(lojas_nok)}")
    finally:
        ex.shutdown(wait=False)  # não bloqueia esperando threads travadas
    if not dados_lojas:
        raise RuntimeError("Nenhuma loja respondeu a tempo. Verifique a conexão e tente novamente.")
    st.session_state["busca_dados_lojas"] = dados_lojas

    # Driver Alabarce — só cria se há produtos sem escolha salva (ausente ou None)
    lojas_ala = [l for l in LOJAS if l.get("tipo") == "alabarce"]
    precisa_driver_ala = False
    for loja in lojas_ala:
        chave_loja = loja["nome"].lower()
        esc_loja   = escolhas.get(chave_loja, {})
        if any(
            normalizar(p) not in esc_loja or esc_loja.get(normalizar(p)) is None
            for p in st.session_state["busca_produtos"]
        ):
            precisa_driver_ala = True
            break
    if precisa_driver_ala:
        st.session_state["busca_driver_ala"] = _criar_driver_alabarce()

    # Coleta candidatos para produtos sem escolha salva
    pendentes: list = []
    salvou = False
    produtos = st.session_state["busca_produtos"]
    driver_ala = st.session_state.get("busca_driver_ala")

    for nome in produtos:
        chave = normalizar(nome)

        # VipCommerce
        for loja in lojas_vc:
            loja_nome  = loja["nome"]
            chave_loja = loja_nome.lower()
            if chave not in escolhas.get(chave_loja, {}):
                dados = dados_lojas.get(loja_nome)
                if not dados:
                    continue
                lista      = buscar_produto(nome, loja, dados) or []
                candidatos = encontrar_candidatos(nome, lista)
                if not candidatos:
                    escolhas.setdefault(chave_loja, {})[chave] = None
                    salvou = True
                elif len(candidatos) == 1:
                    escolhas.setdefault(chave_loja, {})[chave] = candidatos[0]
                    salvou = True
                else:
                    pendentes.append((nome, loja_nome, candidatos))

        # Alabarce
        for loja in lojas_ala:
            loja_nome  = loja["nome"]
            chave_loja = loja_nome.lower()
            esc_loja   = escolhas.get(chave_loja, {})
            precisa_buscar = chave not in esc_loja or esc_loja.get(chave) is None
            if precisa_buscar and driver_ala:
                lista      = buscar_produto_alabarce(nome, driver_ala) or []
                candidatos = encontrar_candidatos(nome, lista)
                if not candidatos:
                    escolhas.setdefault(chave_loja, {})[chave] = None
                    salvou = True
                elif len(candidatos) == 1:
                    escolhas.setdefault(chave_loja, {})[chave] = candidatos[0]
                    salvou = True
                else:
                    pendentes.append((nome, loja_nome, candidatos))

    if salvou:
        salvar_escolhas(escolhas)

    st.session_state["busca_pendentes"] = pendentes
    st.session_state["busca_idx"]       = 0
    st.session_state["busca_fase"]      = "escolhendo" if pendentes else "buscando"


# ─────────────────────────────────────────────────────────────
# Fase 3 — busca preços finais
# ─────────────────────────────────────────────────────────────
def _fase_buscando(progress_bar, status_text):
    produtos    = st.session_state["busca_produtos"]
    escolhas    = st.session_state["busca_escolhas"]
    dados_lojas = st.session_state["busca_dados_lojas"]
    driver_ala  = st.session_state.get("busca_driver_ala")
    resultados  = []

    lojas_vc_ativas = [
        (l, dados_lojas[l["nome"]])
        for l in LOJAS
        if l.get("tipo", "vipcommerce") == "vipcommerce"
        and l["nome"] in dados_lojas
    ]

    # Timeout por produto: 3 lojas × 15s cada + folga = 60s
    _TIMEOUT_PRODUTO = 60

    try:
        for i, nome in enumerate(produtos):
            chave = normalizar(nome)
            status_text.text(f"[{i + 1}/{len(produtos)}] {nome}")
            linha = {"Produto Buscado": nome}

            # VipCommerce: 3 lojas em paralelo, 1 produto por vez
            # (evita rate-limit: cada servidor recebe só 1 conexão simultânea)
            novas: dict = {}
            with ThreadPoolExecutor(
                max_workers=min(MAX_WORKERS_LOJAS, len(lojas_vc_ativas) or 1)
            ) as ex:
                futuros = {
                    ex.submit(
                        _processar_loja_vc,
                        nome, loja, dados,
                        escolhas.setdefault(loja["nome"].lower(), {}),
                        chave,
                    ): loja
                    for loja, dados in lojas_vc_ativas
                }
                try:
                    for fut in as_completed(futuros, timeout=_TIMEOUT_PRODUTO):
                        loja_f = futuros[fut]
                        try:
                            campos, escolha_nova = fut.result()
                            if campos and escolha_nova != "TOKEN_EXPIRED":
                                linha.update(campos)
                            if (
                                escolha_nova is not _INALTERADO
                                and escolha_nova != "TOKEN_EXPIRED"
                            ):
                                novas[loja_f["nome"].lower()] = escolha_nova
                        except Exception:
                            pass
                except TimeoutError:
                    lojas_ok = {k for k in linha if k != "Produto Buscado"}
                    for loja_f, _ in lojas_vc_ativas:
                        col = f"Status ({loja_f['nome']})"
                        if col not in lojas_ok:
                            linha[f"Produto Encontrado ({loja_f['nome']})"] = ""
                            linha[f"Preço ({loja_f['nome']})"]               = ""
                            linha[col]                                        = "Timeout"

            if novas:
                for cl, esc in novas.items():
                    escolhas[cl][chave] = esc
                salvar_escolhas(escolhas)

            # Alabarce — sequencial (driver único), só se houver escolha salva
            for loja in LOJAS:
                if loja.get("tipo") != "alabarce":
                    continue
                loja_nome  = loja["nome"]
                chave_loja = loja_nome.lower()
                entrada    = escolhas.get(chave_loja, {}).get(chave)
                if entrada and driver_ala:
                    res = _processar_alabarce_salvo(entrada, nome, driver_ala)
                    linha[f"Produto Encontrado ({loja_nome})"] = res["descricao"]
                    linha[f"Preço ({loja_nome})"]              = res["preco"]
                    linha[f"Status ({loja_nome})"]             = res["status"]
                else:
                    linha[f"Produto Encontrado ({loja_nome})"] = ""
                    linha[f"Preço ({loja_nome})"]              = ""
                    linha[f"Status ({loja_nome})"]             = "Não encontrado"

            resultados.append(linha)
            progress_bar.progress((i + 1) / len(produtos))

    finally:
        if driver_ala:
            driver_ala.quit()
            st.session_state["busca_driver_ala"] = None

    st.session_state["busca_resultados"] = resultados
    st.session_state["busca_csv_path"]   = _salvar_csv(resultados)
    st.session_state["busca_fase"]       = "pronto"


def _salvar_csv(resultados: list) -> str:
    campos = ["Produto Buscado"]
    for loja in LOJAS:
        campos.append(f"Produto Encontrado ({loja['nome']})")
    for loja in LOJAS:
        campos.append(f"Preço ({loja['nome']})")
        campos.append(f"Status ({loja['nome']})")

    hoje     = date.today()
    nome_arq = f"preços_{hoje.day:02d}_{hoje.month:02d}.csv"
    os.makedirs(_PASTA_RESULTADOS, exist_ok=True)
    caminho  = os.path.join(_PASTA_RESULTADOS, nome_arq)

    try:
        with open(caminho, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(resultados)
        return caminho
    except PermissionError:
        alt = nome_arq.replace(".csv", "_novo.csv")
        caminho_alt = os.path.join(_PASTA_RESULTADOS, alt)
        with open(caminho_alt, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(resultados)
        return caminho_alt


# ─────────────────────────────────────────────────────────────
# Página principal
# ─────────────────────────────────────────────────────────────
def main():
    st.title("💰 Busca de Preços")
    _init()

    fase = st.session_state["busca_fase"]

    # ── Idle ──────────────────────────────────────────────────
    if fase == "idle":
        n_lojas = len(LOJAS)
        st.info(
            f"Clique em **Buscar Preços** para capturar os tokens das {n_lojas} lojas "
            "e iniciar a busca. Produtos já escolhidos anteriormente são atualizados "
            "automaticamente; novos produtos precisam de confirmação."
        )
        if st.button("🔍 Buscar Preços", type="primary"):
            try:
                with open(_CSV_PRODUTOS, encoding="utf-8-sig") as f:
                    produtos = [
                        row["Produtos"].strip()
                        for row in csv.DictReader(f)
                        if row.get("Produtos")
                    ]
            except FileNotFoundError:
                st.error(f"Arquivo não encontrado: `{_CSV_PRODUTOS}`")
                return
            except KeyError:
                st.error("Coluna **Produtos** não encontrada no CSV.")
                return

            st.session_state["busca_produtos"] = produtos
            st.session_state["busca_fase"]     = "capturando"
            st.rerun()

    # ── Capturando tokens ──────────────────────────────────────
    elif fase == "capturando":
        with st.spinner(
            "Abrindo o navegador e capturando tokens das lojas… "
            "(isso leva alguns segundos)"
        ):
            try:
                _fase_capturando()
            except Exception as e:
                st.session_state["busca_fase"] = "idle"
                st.error(f"Erro ao capturar tokens: {e}")
                st.stop()
        st.rerun()

    # ── Escolhendo produtos novos ──────────────────────────────
    elif fase == "escolhendo":
        pendentes = st.session_state["busca_pendentes"]
        idx       = st.session_state["busca_idx"]

        if idx < len(pendentes):
            st.progress(
                idx / len(pendentes),
                text=f"Escolha **{idx + 1}** de **{len(pendentes)}** — confirme cada produto antes de continuar.",
            )
            _dialogo_escolha()
        else:
            st.session_state["busca_fase"] = "buscando"
            st.rerun()

    # ── Buscando preços ────────────────────────────────────────
    elif fase == "buscando":
        pb   = st.progress(0.0, text="Buscando preços…")
        stat = st.empty()
        try:
            _fase_buscando(pb, stat)
        except Exception as e:
            st.session_state["busca_fase"] = "idle"
            st.error(f"Erro ao buscar preços: {e}")
            st.stop()
        st.rerun()

    # ── Pronto ─────────────────────────────────────────────────
    elif fase == "pronto":
        resultados = st.session_state["busca_resultados"]
        csv_path   = st.session_state.get("busca_csv_path", "")

        st.success(f"✅ Busca concluída! **{len(resultados)}** produto(s) processado(s).")
        if csv_path:
            st.caption(f"💾 Salvo em: `{csv_path}`")

        if resultados:
            df = pd.DataFrame(resultados).set_index("Produto Buscado")
            st.dataframe(df, use_container_width=True)

            # Botão de download
            buf = io.StringIO()
            df.to_csv(buf, encoding="utf-8-sig")
            st.download_button(
                label="⬇️ Baixar CSV",
                data=buf.getvalue().encode("utf-8-sig"),
                file_name=os.path.basename(csv_path) if csv_path else "precos.csv",
                mime="text/csv",
            )

        if st.button("🔄 Nova busca"):
            for k in [
                "busca_fase", "busca_produtos", "busca_dados_lojas",
                "busca_driver_ala", "busca_escolhas", "busca_pendentes",
                "busca_idx", "busca_cand_sel", "busca_resultados", "busca_csv_path",
            ]:
                st.session_state.pop(k, None)
            st.rerun()


main()
