"""
=============================================================
  Busca de Preços — VipCommerce (multi-loja) + Alabarce
  Versão 2.0 — Otimizado para Streamlit Cloud
=============================================================

MUDANÇAS EM RELAÇÃO À VERSÃO ANTERIOR:
  - Token JWT capturado via cookie (sem browser quando possível)
  - Processamento sequencial (sem ThreadPoolExecutor para browsers)
  - Interface via Streamlit (não depende de terminal)
  - Caminhos relativos (funciona em Linux e Windows)
  - Alabarce via requests + BeautifulSoup (sem Playwright quando possível)
  - Uso mínimo de RAM (~200MB máx vs ~1.2GB antes)
"""

import csv
import json
import os
import io
import re
import time
import uuid
import logging
import unicodedata
from datetime import date, datetime

import requests as req
import streamlit as st

# ─────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO DE LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("buscar_precos")

# ─────────────────────────────────────────────────────────────
#  CAMINHOS (relativos ao diretório do script)
# ─────────────────────────────────────────────────────────────

_DIR = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_ENTRADA = os.path.join(_DIR, "produtos.csv")
ARQUIVO_ESCOLHAS = os.path.join(_DIR, "escolhas.json")
PASTA_RESULTADOS = os.path.join(_DIR, "dados", "precos")

# ─────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO DE LOJAS
# ─────────────────────────────────────────────────────────────

LOJAS = [
    {
        "nome": "Semar",
        "url": "https://www.semarentrega.com.br",
        "dominio": "semarentrega.com.br",
        "tipo": "vipcommerce",
    },
    {
        "nome": "Rossi",
        "url": "https://www.rossidelivery.com.br",
        "dominio": "rossidelivery.com.br",
        "tipo": "vipcommerce",
    },
    {
        "nome": "Shibata",
        "url": "https://www.loja.shibata.com.br",
        "dominio": "loja.shibata.com.br",
        "tipo": "vipcommerce",
    },
    {
        "nome": "Alabarce",
        "url": "https://alabarce.net.br",
        "tipo": "alabarce",
    },
]

LOJAS_CONHECIDAS = {"semar", "rossi", "shibata", "alabarce"}
DELAY_ENTRE_BUSCAS = 0.3

# Headers padrão para simular navegador real
_HEADERS_NAVEGADOR = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
}


# =============================================================
#  1. CAPTURA DE TOKEN — VipCommerce
#     Estratégia: requests (cookie) → Playwright (fallback)
# =============================================================

def _capturar_token_via_requests(loja: dict) -> dict | None:
    """
    Tenta obter o token JWT acessando o site via requests.
    O VipCommerce armazena o token no cookie 'vip-token'.
    Retorna dict com token ou None se falhar.
    """
    try:
        session = req.Session()
        session.headers.update(_HEADERS_NAVEGADOR)

        resp = session.get(loja["url"], timeout=20, allow_redirects=True)
        resp.raise_for_status()

        # Procura o cookie vip-token
        token = session.cookies.get("vip-token")

        if not token:
            # Tenta em todos os cookies (pode estar em domínio diferente)
            for cookie in session.cookies:
                if cookie.name == "vip-token" and cookie.value:
                    token = cookie.value
                    break

        if not token or len(token) < 20:
            log.info(f"{loja['nome']}: cookie vip-token não encontrado via requests")
            return None

        log.info(f"{loja['nome']}: token capturado via cookie (requests)")
        return {"token": token}

    except Exception as e:
        log.warning(f"{loja['nome']}: falha ao capturar token via requests: {e}")
        return None


def _capturar_token_via_playwright(loja: dict) -> dict | None:
    """
    Fallback: abre Playwright, aguarda a primeira requisição ao
    VipCommerce e captura token + sessao_id + session + IDs.
    Usa wait_for_event com predicado — muito mais confiável que timeout fixo.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("Playwright não instalado. Execute: pip install playwright && playwright install chromium")
        return None

    resultado = {
        "token": None, "sessao_id": None, "session": None,
        "org_id": None, "filial_id": None, "cd_id": None,
    }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                      "--single-process", "--disable-extensions"],
            )
            context = browser.new_context(user_agent=_HEADERS_NAVEGADOR["User-Agent"])
            page = context.new_page()

            dados_capturados = {"encontrou": False}

            def interceptar(request):
                if dados_capturados["encontrou"]:
                    return
                if "vipcommerce" not in request.url:
                    return

                headers = request.headers
                auth = headers.get("authorization", "")
                token_valor = auth.replace("Bearer", "").replace("bearer", "").strip()

                if token_valor and len(token_valor) > 20:
                    resultado["token"] = token_valor
                    resultado["sessao_id"] = headers.get("sessao-id", "").strip()

                    url = request.url
                    if "session=" in url:
                        resultado["session"] = url.split("session=")[-1].split("&")[0].strip()

                    if "/org/" in url and "/centro_distribuicao/" in url:
                        partes = url.split("/")
                        try:
                            resultado["org_id"] = partes[partes.index("org") + 1]
                            resultado["filial_id"] = partes[partes.index("filial") + 1]
                            resultado["cd_id"] = partes[partes.index("centro_distribuicao") + 1]
                        except (ValueError, IndexError):
                            pass

                    dados_capturados["encontrou"] = True

            page.on("request", interceptar)
            page.goto(loja["url"], wait_until="domcontentloaded", timeout=30000)

            # Aguarda até 20s pelo token (muito melhor que os 5s fixos do código antigo)
            for _ in range(40):
                if dados_capturados["encontrou"]:
                    break
                page.wait_for_timeout(500)

            # Se pegou o token mas não os IDs, tenta digitar algo para forçar
            if resultado["token"] and not resultado["cd_id"]:
                try:
                    campo = page.locator(
                        "input[type='search'], input[type='text'], "
                        "input[placeholder*='usca'], input[placeholder*='esquisa']"
                    ).first
                    campo.click()
                    campo.type("arroz")
                    for _ in range(16):  # mais 8s
                        if resultado["cd_id"]:
                            break
                        page.wait_for_timeout(500)
                except Exception:
                    pass

            browser.close()

        if resultado["token"]:
            log.info(f"{loja['nome']}: token capturado via Playwright")
            return resultado
        else:
            log.warning(f"{loja['nome']}: Playwright não encontrou token")
            return None

    except Exception as e:
        log.error(f"{loja['nome']}: erro no Playwright: {e}")
        return None


def _obter_dados_filial(token: str, dominio: str) -> dict | None:
    """
    Chama o endpoint de filial para obter org_id e filial_id.
    GET /organizacoes/filiais/dominio/{dominio}
    """
    url = f"https://services.vipcommerce.com.br/organizacoes/filiais/dominio/{dominio}"
    headers = {
        **_HEADERS_NAVEGADOR,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": f"https://www.{dominio}",
    }
    try:
        resp = req.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", {})
        org = data.get("organizacao", {})
        return {
            "filial_id": str(data.get("id", "")),
            "org_id": str(org.get("id", "")),
        }
    except Exception as e:
        log.warning(f"Erro ao obter dados da filial ({dominio}): {e}")
        return None


def _obter_cd_id(token: str, org_id: str, filial_id: str, dominio: str) -> str | None:
    """
    Tenta descobrir o cd_id (centro de distribuição) via API.
    Testa endpoints comuns do VipCommerce.
    """
    base = "https://services.vipcommerce.com.br"
    headers = {
        **_HEADERS_NAVEGADOR,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "domainkey": dominio,
        "organizationid": org_id,
        "filialid": filial_id,
    }

    # Tentativa 1: endpoint de centros de distribuição
    endpoints = [
        f"{base}/api-admin/v1/org/{org_id}/filial/{filial_id}/centros_distribuicao",
        f"{base}/api-admin/v1/org/{org_id}/filial/{filial_id}/centro_distribuicao",
        f"{base}/organizacoes/{org_id}/filiais/{filial_id}/centros-distribuicao",
    ]
    for url in endpoints:
        try:
            resp = req.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # Tenta extrair o primeiro cd_id
                if isinstance(data, dict) and "data" in data:
                    items = data["data"]
                    if isinstance(items, list) and items:
                        return str(items[0].get("id", ""))
                    elif isinstance(items, dict):
                        return str(items.get("id", ""))
                elif isinstance(data, list) and data:
                    return str(data[0].get("id", ""))
        except Exception:
            continue

    return None


def capturar_dados_loja(loja: dict) -> dict | None:
    """
    Captura todos os dados necessários para acessar a API de uma loja VipCommerce.
    Estratégia em cascata:
      1. Token via cookie (requests) → dados via API
      2. Tudo via Playwright (fallback)

    Retorna dict com: token, sessao_id, session, org_id, filial_id, cd_id
    Ou None se falhar completamente.
    """
    resultado = {
        "token": None, "sessao_id": None, "session": None,
        "org_id": None, "filial_id": None, "cd_id": None,
    }

    # ── Estratégia 1: requests puro ─────────────────────────
    dados_cookie = _capturar_token_via_requests(loja)
    if dados_cookie and dados_cookie.get("token"):
        resultado["token"] = dados_cookie["token"]

        # Gera um sessao_id (UUID) — funciona para sessões anônimas
        resultado["sessao_id"] = str(uuid.uuid4())
        resultado["session"] = str(uuid.uuid4())

        # Busca org_id e filial_id via API
        dados_filial = _obter_dados_filial(resultado["token"], loja["dominio"])
        if dados_filial:
            resultado["org_id"] = dados_filial["org_id"]
            resultado["filial_id"] = dados_filial["filial_id"]

            # Busca cd_id
            cd_id = _obter_cd_id(
                resultado["token"], resultado["org_id"],
                resultado["filial_id"], loja["dominio"]
            )
            if cd_id:
                resultado["cd_id"] = cd_id
                log.info(f"{loja['nome']}: dados completos via requests (sem browser!)")
                return resultado

    # ── Estratégia 2: Playwright (fallback) ──────────────────
    log.info(f"{loja['nome']}: tentando via Playwright (fallback)...")
    dados_pw = _capturar_token_via_playwright(loja)
    if dados_pw and dados_pw.get("token"):
        # Mescla: dados do Playwright preenchem o que falta
        for chave in resultado:
            if not resultado[chave] and dados_pw.get(chave):
                resultado[chave] = dados_pw[chave]

        # Se ainda não tem org/filial, tenta via API com o token do Playwright
        if not resultado["org_id"] and resultado["token"]:
            dados_filial = _obter_dados_filial(resultado["token"], loja["dominio"])
            if dados_filial:
                resultado["org_id"] = dados_filial["org_id"]
                resultado["filial_id"] = dados_filial["filial_id"]

        if resultado["token"] and resultado["org_id"]:
            log.info(f"{loja['nome']}: dados capturados via Playwright")
            return resultado

    log.error(f"{loja['nome']}: falha total na captura de dados")
    return None


# =============================================================
#  2. FUNÇÕES DE API — VipCommerce
#     (preservadas do código original — já funcionavam)
# =============================================================

def normalizar(texto: str) -> str:
    """Remove acentos e converte para minúsculas."""
    texto = texto.lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def montar_headers(loja: dict, dados: dict) -> dict:
    """Monta headers para chamadas à API VipCommerce."""
    return {
        "Authorization": f"Bearer {dados['token']}",
        "domainkey": loja["dominio"],
        "organizationid": dados["org_id"],
        "filialid": dados["filial_id"],
        "sessao-id": dados.get("sessao_id", ""),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": _HEADERS_NAVEGADOR["User-Agent"],
    }


def buscar_produto(termo: str, loja: dict, dados: dict) -> list | None:
    """
    Busca produtos na API VipCommerce.
    Retorna lista de produtos, lista vazia, ou None (token expirado).
    """
    base = "https://services.vipcommerce.com.br/api-admin/v1"
    url = (
        f"{base}/org/{dados['org_id']}/filial/{dados['filial_id']}"
        f"/centro_distribuicao/{dados['cd_id']}/loja/buscas/produtos"
        f"/termo/{req.utils.quote(termo)}"
        f"?page=1&session={dados.get('session', '')}"
    )
    try:
        r = req.get(url, headers=montar_headers(loja, dados), timeout=15)
        if r.status_code == 401:
            return None  # token expirado
        if r.status_code != 200:
            log.warning(f"Status {r.status_code} para '{termo}' em {loja['nome']}")
            return []
        d = r.json()
        if isinstance(d, dict):
            data = d.get("data", {})
            if isinstance(data, dict) and "produtos" in data:
                return data["produtos"]
            if isinstance(data, list):
                return data
            for chave in ("produtos", "items", "results"):
                if chave in d and isinstance(d[chave], list):
                    return d[chave]
        return d if isinstance(d, list) else []
    except Exception as e:
        log.error(f"Erro na busca '{termo}' em {loja['nome']}: {e}")
        return []


def buscar_detalhes(produto_id, loja: dict, dados: dict) -> dict | None:
    """Busca detalhes de um produto específico."""
    base = "https://services.vipcommerce.com.br/api-admin/v1"
    url = (
        f"{base}/org/{dados['org_id']}/filial/{dados['filial_id']}"
        f"/centro_distribuicao/{dados['cd_id']}/loja/produtos/{produto_id}/detalhes"
    )
    try:
        r = req.get(url, headers=montar_headers(loja, dados), timeout=15)
        if r.status_code != 200:
            return None
        d = r.json()
        return d.get("data", d) if isinstance(d, dict) else d
    except Exception as e:
        log.error(f"Erro nos detalhes {produto_id} em {loja['nome']}: {e}")
        return None


def extrair_preco(p: dict) -> str | None:
    """Extrai preço do produto, considerando oferta e preço por kg."""
    em_oferta = p.get("em_oferta", False)
    oferta = p.get("oferta") or {}

    if em_oferta and oferta.get("preco_oferta"):
        preco_base = float(oferta["preco_oferta"])
    else:
        for chave in ("preco", "price", "valor"):
            if p.get(chave):
                preco_base = float(p[chave])
                break
        else:
            return None

    qtd = p.get("quantidade_unidade_diferente")
    if qtd and float(qtd) > 0 and p.get("possui_unidade_diferente"):
        preco_kg = preco_base / float(qtd)
        return f"{preco_kg:.2f}"

    return f"{preco_base:.2f}"


# =============================================================
#  3. LÓGICA DE MATCH (preservada — já funcionava)
# =============================================================

def encontrar_candidatos(termo: str, lista_produtos: list) -> list:
    """Filtra e ordena candidatos relevantes para o termo buscado."""
    IGNORAR = {"de", "da", "do", "das", "dos", "com", "em"}
    EXCLUIR = {
        "polpa", "semente", "swift", "suco", "refresco", "refrigerante",
        "bolo", "biscoito", "creme", "granola", "azeitona", "congelado",
        "congelada", "feltrin", "desidratado", "desidratada",
    }

    palavras = [p for p in normalizar(termo).split() if len(p) > 2 and p not in IGNORAR]
    if not palavras:
        return []

    def dw(p):
        return normalizar(p.get("descricao", "") or p.get("nome", "")).split()

    candidatos = []
    for p in lista_produtos:
        words = dw(p)
        if set(words) & EXCLUIR:
            continue
        desc_str = " ".join(words)
        if words[:len(palavras)] == palavras:
            candidatos.append((0, len(words), p))
        elif words and words[0] == palavras[0] and all(w in desc_str for w in palavras[1:]):
            candidatos.append((1, len(words), p))

    candidatos.sort(key=lambda x: (x[0], x[1]))
    return [p for _, _, p in candidatos]


def processar_match(nome: str, produto: dict, loja: dict, dados: dict,
                    lista_produtos: list | None = None) -> dict:
    """Processa o match de um produto e retorna descricao, preco e status."""
    produto_id = produto.get("produto_id") or produto.get("id")
    descricao = produto.get("descricao") or produto.get("nome", "")
    disponivel = produto.get("disponivel", True)

    preco = extrair_preco(produto)

    if preco is None and lista_produtos is not None:
        item_lista = next(
            (p for p in lista_produtos if p.get("produto_id") == produto.get("produto_id")),
            None,
        )
        if item_lista:
            preco = extrair_preco(item_lista)

    if preco is None and produto_id:
        detalhes = buscar_detalhes(produto_id, loja, dados)
        if detalhes:
            preco = extrair_preco(detalhes) or preco
            disponivel = detalhes.get("disponivel", disponivel)

    status = "OK" if disponivel else "Indisponível"
    preco_final = preco if disponivel else ""

    return {"descricao": descricao, "preco": preco_final, "status": status}


# =============================================================
#  4. ALABARCE — requests + BeautifulSoup (sem Playwright)
# =============================================================

def _buscar_alabarce_requests(termo: str) -> list:
    """
    Busca produtos no Alabarce usando requests + BeautifulSoup.
    Sem precisar de browser. Muito mais leve em RAM.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.warning("BeautifulSoup não instalado. Execute: pip install beautifulsoup4")
        return []

    url = f"https://alabarce.net.br/products?utf8=%E2%9C%93&keywords={req.utils.quote(termo)}"
    try:
        resp = req.get(url, headers=_HEADERS_NAVEGADOR, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        cards = soup.select(".product-cards .product")
        if not cards:
            # Tenta seletores alternativos
            cards = soup.select("[class*='product']")

        produtos = []
        for card in cards:
            # Nome
            el_nome = card.select_one("h5.product-title") or card.select_one("[class*='title']")
            if not el_nome:
                continue
            nome = el_nome.get_text(strip=True).upper()
            if not nome:
                continue

            # Preço
            preco_num = None
            el_preco = card.select_one(".price-amount span") or card.select_one("[class*='price'] span")
            if el_preco:
                preco_str = el_preco.get_text(strip=True)
                preco_clean = (
                    preco_str
                    .replace("R$", "").replace("R$ ", "")
                    .replace("\xa0", "").strip()
                    .replace(".", "").replace(",", ".")
                )
                try:
                    preco_num = float(preco_clean)
                except ValueError:
                    pass

            produtos.append({
                "descricao": nome,
                "preco": f"{preco_num:.2f}" if preco_num else "",
                "disponivel": True,
            })

        return produtos

    except Exception as e:
        log.error(f"Erro na busca Alabarce (requests) '{termo}': {e}")
        return []


def _buscar_alabarce_playwright(termo: str) -> list:
    """
    Fallback: busca no Alabarce via Playwright (se o site precisar de JS).
    Usa um browser único, sem manter instância aberta.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                      "--single-process"],
            )
            page = browser.new_page()

            url = f"https://alabarce.net.br/products?utf8=%E2%9C%93&keywords={req.utils.quote(termo)}"
            page.goto(url, wait_until="domcontentloaded")

            # Fecha popup de loja se aparecer
            try:
                modal = page.locator("#stock-picker")
                if modal.count() > 0 and modal.is_visible():
                    btn = modal.locator("a[href*='current_stock'][href*='withdrawal']")
                    if btn.count() > 0:
                        btn.first.click()
                        page.locator("#stock-picker").wait_for(state="hidden", timeout=5000)
            except Exception:
                pass

            # Aguarda cards
            try:
                page.locator(".product-cards .product").first.wait_for(timeout=8000)
                page.wait_for_timeout(500)
            except Exception:
                browser.close()
                return []

            cards = page.locator(".product-cards .product").all()
            produtos = []

            for card in cards:
                try:
                    el_nome = card.locator("h5.product-title")
                    if el_nome.count() == 0:
                        continue
                    nome = el_nome.first.inner_text().strip().upper()
                except Exception:
                    continue

                if not nome:
                    continue

                preco_num = None
                try:
                    el_preco = card.locator(".price-amount span")
                    if el_preco.count() > 0:
                        preco_str = el_preco.first.inner_text()
                        preco_clean = (
                            preco_str
                            .replace("R$", "").replace("R$ ", "")
                            .replace("\xa0", "").strip()
                            .replace(".", "").replace(",", ".")
                        )
                        preco_num = float(preco_clean)
                except Exception:
                    pass

                produtos.append({
                    "descricao": nome,
                    "preco": f"{preco_num:.2f}" if preco_num else "",
                    "disponivel": True,
                })

            browser.close()
            return produtos

    except Exception as e:
        log.error(f"Erro na busca Alabarce (Playwright) '{termo}': {e}")
        return []


def buscar_produto_alabarce(termo: str) -> list:
    """
    Busca no Alabarce com fallback automático.
    Tenta requests primeiro, Playwright se necessário.
    """
    # Tenta via requests (sem browser)
    produtos = _buscar_alabarce_requests(termo)
    if produtos:
        return produtos

    # Fallback: Playwright
    log.info(f"Alabarce: '{termo}' sem resultados via requests, tentando Playwright...")
    return _buscar_alabarce_playwright(termo)


# =============================================================
#  5. CACHE DE ESCOLHAS (preservado — já funcionava)
# =============================================================

def _slim(entrada) -> dict | None:
    """Extrai apenas produto_id e descricao de um objeto de produto."""
    if entrada is None:
        return None
    if isinstance(entrada, dict):
        return {
            "produto_id": entrada.get("produto_id"),
            "descricao": entrada.get("descricao", ""),
        }
    return entrada


def carregar_escolhas() -> dict:
    """Carrega escolhas salvas do JSON."""
    if os.path.exists(ARQUIVO_ESCOLHAS):
        try:
            with open(ARQUIVO_ESCOLHAS, encoding="utf-8") as f:
                dados = json.load(f)
            # Garante formato correto (slim)
            novo = {}
            for loja in LOJAS_CONHECIDAS:
                if loja in dados and isinstance(dados[loja], dict):
                    novo[loja] = {k: _slim(v) for k, v in dados[loja].items()}
            return novo
        except Exception as e:
            log.warning(f"Erro ao carregar escolhas: {e}")
    return {}


def salvar_escolhas(escolhas: dict):
    """Salva escolhas no JSON (formato slim)."""
    dados_slim = {
        loja: {k: _slim(v) for k, v in produtos.items()}
        for loja, produtos in escolhas.items()
        if isinstance(produtos, dict)
    }
    try:
        with open(ARQUIVO_ESCOLHAS, "w", encoding="utf-8") as f:
            json.dump(dados_slim, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"Erro ao salvar escolhas: {e}")


# =============================================================
#  6. GERAR CSV DE RESULTADOS
# =============================================================

def gerar_csv(resultados: list) -> str:
    """
    Gera conteúdo CSV dos resultados.
    Retorna string CSV para uso com st.download_button.
    """
    campos = ["Produto Buscado"]
    for loja in LOJAS:
        campos.append(f"Produto Encontrado ({loja['nome']})")
    for loja in LOJAS:
        campos.append(f"Preco ({loja['nome']})")
        campos.append(f"Status ({loja['nome']})")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=campos, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(resultados)
    writer.writerow({})
    writer.writerow({"Produto Buscado": f"Busca gerada em: {date.today().strftime('%d/%m/%Y')}"})

    return output.getvalue()


def salvar_csv_local(conteudo_csv: str):
    """Salva CSV localmente (backup)."""
    try:
        os.makedirs(PASTA_RESULTADOS, exist_ok=True)
        hoje = date.today()
        nome_arquivo = f"precos_{hoje.day:02d}_{hoje.month:02d}.csv"
        caminho = os.path.join(PASTA_RESULTADOS, nome_arquivo)
        with open(caminho, "w", newline="", encoding="utf-8-sig") as f:
            f.write(conteudo_csv)
        log.info(f"CSV salvo em: {caminho}")
    except Exception as e:
        log.warning(f"Erro ao salvar CSV local: {e}")


# =============================================================
#  7. INTERFACE STREAMLIT
# =============================================================

def _inicializar_estado():
    """Inicializa session_state do Streamlit."""
    defaults = {
        "etapa": "inicio",           # inicio | capturando | buscando | selecao | concluido
        "dados_lojas": {},            # {nome_loja: dados}
        "escolhas": {},               # cache de escolhas
        "resultados": [],             # resultados finais
        "produtos": [],               # lista de produtos do CSV
        "produto_atual_idx": 0,       # índice do produto sendo processado
        "pendente_selecao": None,     # produto aguardando seleção do usuário
        "csv_conteudo": None,         # CSV gerado
        "log_mensagens": [],          # log visual
    }
    for chave, valor in defaults.items():
        if chave not in st.session_state:
            st.session_state[chave] = valor


def _log_ui(msg: str):
    """Adiciona mensagem ao log visual."""
    st.session_state.log_mensagens.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def _carregar_produtos():
    """Carrega a lista de produtos do CSV."""
    if not os.path.exists(ARQUIVO_ENTRADA):
        st.error(f"Arquivo '{ARQUIVO_ENTRADA}' não encontrado.")
        return []
    try:
        with open(ARQUIVO_ENTRADA, encoding="utf-8-sig") as f:
            return [row["Produtos"].strip() for row in csv.DictReader(f) if row.get("Produtos")]
    except KeyError:
        st.error("Coluna 'Produtos' não encontrada no CSV.")
        return []


def _etapa_capturar_tokens():
    """Etapa 1: captura tokens de todas as lojas VipCommerce."""
    lojas_vc = [l for l in LOJAS if l.get("tipo") == "vipcommerce"]
    barra = st.progress(0, text="Capturando tokens...")

    for i, loja in enumerate(lojas_vc):
        barra.progress((i) / len(lojas_vc), text=f"Capturando token: {loja['nome']}...")
        _log_ui(f"Capturando token: {loja['nome']}...")

        dados = capturar_dados_loja(loja)
        if dados and dados.get("token"):
            st.session_state.dados_lojas[loja["nome"]] = dados
            _log_ui(f"  {loja['nome']}: OK (org={dados.get('org_id')})")
        else:
            _log_ui(f"  {loja['nome']}: FALHOU — será ignorada")

    barra.progress(1.0, text="Tokens capturados!")

    if not st.session_state.dados_lojas:
        st.error("Nenhuma loja VipCommerce respondeu. Verifique a conexão.")
        st.session_state.etapa = "inicio"
        return False

    return True


def _processar_loja_vc(nome: str, loja: dict, dados: dict,
                       escolhas_loja: dict, chave: str) -> tuple:
    """
    Processa uma loja VipCommerce para um produto.
    Retorna (campos_csv, escolha_nova, candidatos_para_selecao).
    """
    loja_nome = loja["nome"]

    # Cache hit
    if chave in escolhas_loja:
        entrada = escolhas_loja[chave]
        if entrada is None:
            return (
                {
                    f"Produto Encontrado ({loja_nome})": "",
                    f"Preco ({loja_nome})": "",
                    f"Status ({loja_nome})": "Não encontrado",
                },
                None, None
            )

        lista = buscar_produto(nome, loja, dados) or []
        res = processar_match(nome, entrada, loja, dados, lista_produtos=lista)
        return (
            {
                f"Produto Encontrado ({loja_nome})": res["descricao"],
                f"Preco ({loja_nome})": res["preco"],
                f"Status ({loja_nome})": res["status"],
            },
            None, None
        )

    # Produto novo: buscar na API
    lista = buscar_produto(nome, loja, dados)

    if lista is None:
        # Token expirado — tenta recapturar
        _log_ui(f"  {loja_nome}: token expirado, recapturando...")
        novos_dados = capturar_dados_loja(loja)
        if novos_dados and novos_dados.get("token"):
            st.session_state.dados_lojas[loja_nome] = novos_dados
            lista = buscar_produto(nome, loja, novos_dados) or []
        else:
            lista = []

    if not lista:
        return (
            {
                f"Produto Encontrado ({loja_nome})": "",
                f"Preco ({loja_nome})": "",
                f"Status ({loja_nome})": "Não encontrado",
            },
            None, None
        )

    candidatos = encontrar_candidatos(nome, lista)
    if not candidatos:
        return (
            {
                f"Produto Encontrado ({loja_nome})": "",
                f"Preco ({loja_nome})": "",
                f"Status ({loja_nome})": "Sem match",
            },
            None, None
        )

    # Se só tem 1 candidato, seleciona automaticamente
    if len(candidatos) == 1:
        escolhido = candidatos[0]
        res = processar_match(nome, escolhido, loja, dados)
        return (
            {
                f"Produto Encontrado ({loja_nome})": res["descricao"],
                f"Preco ({loja_nome})": res["preco"],
                f"Status ({loja_nome})": res["status"],
            },
            _slim(escolhido), None
        )

    # Múltiplos candidatos → precisa da seleção do usuário
    return (None, None, candidatos)


def _renderizar_selecao(nome_produto: str, loja_nome: str, candidatos: list) -> dict | None:
    """Renderiza widget de seleção no Streamlit e retorna o escolhido."""
    st.subheader(f"Escolha para '{nome_produto}' em {loja_nome}")

    opcoes = ["Nenhum / Pular"]
    for c in candidatos:
        preco = extrair_preco(c) or c.get("preco", "?")
        desc = c.get("descricao", "?")
        opcoes.append(f"{desc} — R$ {preco}")

    escolha_idx = st.selectbox(
        "Selecione o produto correto:",
        range(len(opcoes)),
        format_func=lambda i: opcoes[i],
        key=f"sel_{normalizar(nome_produto)}_{normalizar(loja_nome)}",
    )

    if escolha_idx == 0:
        return None
    return candidatos[escolha_idx - 1]


# =============================================================
#  8. MAIN — APLICAÇÃO STREAMLIT
# =============================================================

def main():
    st.set_page_config(page_title="Busca de Preços — Benverde", page_icon="🛒", layout="wide")
    st.title("🛒 Busca de Preços")
    st.caption("VipCommerce + Alabarce — Sistema Benverde")

    _inicializar_estado()

    # ── Sidebar: info e controles ─────────────────────────────
    with st.sidebar:
        st.header("Configuração")
        st.write(f"**Lojas:** {', '.join(l['nome'] for l in LOJAS)}")
        st.write(f"**Arquivo de produtos:** `produtos.csv`")
        st.write(f"**Escolhas salvas:** `escolhas.json`")

        if st.button("Limpar cache de escolhas"):
            if os.path.exists(ARQUIVO_ESCOLHAS):
                os.remove(ARQUIVO_ESCOLHAS)
                st.session_state.escolhas = {}
                st.success("Cache limpo!")

        st.divider()
        st.subheader("Log")
        for msg in st.session_state.log_mensagens[-20:]:
            st.text(msg)

    # ── Tela inicial ──────────────────────────────────────────
    if st.session_state.etapa == "inicio":
        produtos = _carregar_produtos()
        if not produtos:
            return

        st.session_state.produtos = produtos
        st.info(f"**{len(produtos)} produto(s)** encontrados no CSV.")

        if st.button("🚀 Iniciar Busca de Preços", type="primary"):
            st.session_state.escolhas = carregar_escolhas()
            st.session_state.etapa = "capturando"
            st.rerun()

    # ── Etapa 1: Capturar tokens ──────────────────────────────
    elif st.session_state.etapa == "capturando":
        st.subheader("Etapa 1/2 — Capturando tokens")
        sucesso = _etapa_capturar_tokens()
        if sucesso:
            st.session_state.etapa = "buscando"
            st.session_state.produto_atual_idx = 0
            st.session_state.resultados = []
            st.rerun()

    # ── Etapa 2: Buscar preços ────────────────────────────────
    elif st.session_state.etapa == "buscando":
        st.subheader("Etapa 2/2 — Buscando preços")
        produtos = st.session_state.produtos
        idx = st.session_state.produto_atual_idx
        escolhas = st.session_state.escolhas

        barra = st.progress(idx / len(produtos), text=f"Produto {idx}/{len(produtos)}")

        while idx < len(produtos):
            nome = produtos[idx]
            chave = normalizar(nome)
            _log_ui(f"[{idx+1}/{len(produtos)}] {nome}")

            linha = {"Produto Buscado": nome}
            pendencias_selecao = []  # (loja, candidatos)

            # ── Lojas VipCommerce ──────────────────────────
            for loja in LOJAS:
                if loja.get("tipo") != "vipcommerce":
                    continue
                if loja["nome"] not in st.session_state.dados_lojas:
                    continue

                dados = st.session_state.dados_lojas[loja["nome"]]
                chave_loja = loja["nome"].lower()
                escolhas.setdefault(chave_loja, {})

                campos, escolha_nova, candidatos = _processar_loja_vc(
                    nome, loja, dados, escolhas[chave_loja], chave
                )

                if candidatos:
                    # Precisa de seleção do usuário
                    pendencias_selecao.append((loja, candidatos))
                elif campos:
                    linha.update(campos)
                    if escolha_nova is not None:
                        escolhas[chave_loja][chave] = _slim(escolha_nova)
                        salvar_escolhas(escolhas)
                        _log_ui(f"  [{loja['nome']}] Salvo automaticamente")

            # Se tem pendências de seleção, pausa para o usuário escolher
            if pendencias_selecao:
                st.session_state.produto_atual_idx = idx
                st.session_state.pendente_selecao = {
                    "nome": nome,
                    "chave": chave,
                    "linha": linha,
                    "pendencias": pendencias_selecao,
                }
                st.session_state.etapa = "selecao"
                st.rerun()
                return

            # ── Alabarce ──────────────────────────────────
            for loja in LOJAS:
                if loja.get("tipo") != "alabarce":
                    continue

                loja_nome = loja["nome"]
                chave_loja = loja_nome.lower()
                escolhas.setdefault(chave_loja, {})

                if chave in escolhas[chave_loja]:
                    entrada = escolhas[chave_loja][chave]
                    if entrada is None:
                        linha[f"Produto Encontrado ({loja_nome})"] = ""
                        linha[f"Preco ({loja_nome})"] = ""
                        linha[f"Status ({loja_nome})"] = "Não encontrado"
                    else:
                        # Re-busca para obter preço atual
                        lista_atual = buscar_produto_alabarce(nome)
                        desc_salva = normalizar(entrada.get("descricao", ""))
                        match = next(
                            (p for p in lista_atual if normalizar(p.get("descricao", "")) == desc_salva),
                            None,
                        )
                        if match:
                            preco = match["preco"]
                            status = "OK" if preco else "Indisponível"
                            linha[f"Produto Encontrado ({loja_nome})"] = match["descricao"]
                        else:
                            preco = entrada.get("preco", "")
                            status = "OK" if preco else "Indisponível"
                            linha[f"Produto Encontrado ({loja_nome})"] = entrada.get("descricao", "")
                        linha[f"Preco ({loja_nome})"] = preco
                        linha[f"Status ({loja_nome})"] = status
                    continue

                # Produto novo no Alabarce
                lista = buscar_produto_alabarce(nome)
                if not lista:
                    escolhas[chave_loja][chave] = None
                    salvar_escolhas(escolhas)
                    linha[f"Produto Encontrado ({loja_nome})"] = ""
                    linha[f"Preco ({loja_nome})"] = ""
                    linha[f"Status ({loja_nome})"] = "Não encontrado"
                    continue

                candidatos = encontrar_candidatos(nome, lista)
                if not candidatos:
                    escolhas[chave_loja][chave] = None
                    salvar_escolhas(escolhas)
                    linha[f"Produto Encontrado ({loja_nome})"] = ""
                    linha[f"Preco ({loja_nome})"] = ""
                    linha[f"Status ({loja_nome})"] = "Sem match"
                    continue

                if len(candidatos) == 1:
                    escolhido = candidatos[0]
                    escolhas[chave_loja][chave] = _slim(escolhido)
                    salvar_escolhas(escolhas)
                    linha[f"Produto Encontrado ({loja_nome})"] = escolhido["descricao"]
                    linha[f"Preco ({loja_nome})"] = escolhido.get("preco", "")
                    linha[f"Status ({loja_nome})"] = "OK" if escolhido.get("preco") else "Indisponível"
                else:
                    # Precisa seleção
                    pendencias_selecao.append((loja, candidatos))

            if pendencias_selecao:
                st.session_state.produto_atual_idx = idx
                st.session_state.pendente_selecao = {
                    "nome": nome,
                    "chave": chave,
                    "linha": linha,
                    "pendencias": pendencias_selecao,
                }
                st.session_state.etapa = "selecao"
                st.rerun()
                return

            st.session_state.resultados.append(linha)
            idx += 1
            time.sleep(DELAY_ENTRE_BUSCAS)
            barra.progress(idx / len(produtos), text=f"Produto {idx}/{len(produtos)}")

        # Busca concluída
        st.session_state.etapa = "concluido"
        st.rerun()

    # ── Etapa de seleção do usuário ───────────────────────────
    elif st.session_state.etapa == "selecao":
        info = st.session_state.pendente_selecao
        nome = info["nome"]
        chave = info["chave"]
        linha = info["linha"]
        escolhas = st.session_state.escolhas

        st.subheader(f"Escolha necessária para: **{nome}**")

        with st.form(key=f"form_selecao_{chave}"):
            selecoes = {}
            for loja, candidatos in info["pendencias"]:
                loja_nome = loja["nome"]
                opcoes = ["Nenhum / Pular"]
                for c in candidatos:
                    preco = extrair_preco(c) or c.get("preco", "?")
                    desc = c.get("descricao", "?")
                    opcoes.append(f"{desc} — R$ {preco}")

                escolha_idx = st.selectbox(
                    f"**{loja_nome}** — selecione o produto:",
                    range(len(opcoes)),
                    format_func=lambda i, o=opcoes: o[i],
                    key=f"sel_{chave}_{normalizar(loja_nome)}",
                )
                selecoes[loja_nome] = (escolha_idx, candidatos, loja)

            submitted = st.form_submit_button("Confirmar escolha", type="primary")

        if submitted:
            for loja_nome, (escolha_idx, candidatos, loja) in selecoes.items():
                chave_loja = loja_nome.lower()
                escolhas.setdefault(chave_loja, {})

                if escolha_idx == 0:
                    escolhas[chave_loja][chave] = None
                    linha[f"Produto Encontrado ({loja_nome})"] = ""
                    linha[f"Preco ({loja_nome})"] = ""
                    linha[f"Status ({loja_nome})"] = "Não encontrado"
                else:
                    escolhido = candidatos[escolha_idx - 1]
                    escolhas[chave_loja][chave] = _slim(escolhido)

                    if loja.get("tipo") == "vipcommerce" and loja_nome in st.session_state.dados_lojas:
                        dados = st.session_state.dados_lojas[loja_nome]
                        res = processar_match(nome, escolhido, loja, dados)
                        linha[f"Produto Encontrado ({loja_nome})"] = res["descricao"]
                        linha[f"Preco ({loja_nome})"] = res["preco"]
                        linha[f"Status ({loja_nome})"] = res["status"]
                    else:
                        linha[f"Produto Encontrado ({loja_nome})"] = escolhido.get("descricao", "")
                        linha[f"Preco ({loja_nome})"] = escolhido.get("preco", "")
                        linha[f"Status ({loja_nome})"] = "OK" if escolhido.get("preco") else "Indisponível"

            salvar_escolhas(escolhas)
            st.session_state.resultados.append(linha)
            st.session_state.produto_atual_idx += 1
            st.session_state.pendente_selecao = None
            st.session_state.etapa = "buscando"
            st.rerun()

    # ── Concluído ─────────────────────────────────────────────
    elif st.session_state.etapa == "concluido":
        resultados = st.session_state.resultados

        ok = sum(
            1 for r in resultados
            if any(r.get(f"Status ({l['nome']})") == "OK" for l in LOJAS)
        )

        st.success(f"Busca concluída! **{ok}/{len(resultados)}** produto(s) com ao menos um preço.")

        # Tabela de resultados
        if resultados:
            st.dataframe(resultados, use_container_width=True)

            # Gerar CSV
            csv_conteudo = gerar_csv(resultados)
            salvar_csv_local(csv_conteudo)

            hoje = date.today()
            nome_arquivo = f"precos_{hoje.day:02d}_{hoje.month:02d}.csv"
            st.download_button(
                label="📥 Baixar CSV",
                data=csv_conteudo,
                file_name=nome_arquivo,
                mime="text/csv",
                type="primary",
            )

        if st.button("🔄 Nova busca"):
            for chave in list(st.session_state.keys()):
                del st.session_state[chave]
            st.rerun()


if __name__ == "__main__":
    main()
