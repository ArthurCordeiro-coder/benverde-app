"""
=============================================================
  Busca de Preços — VipCommerce (multi-loja) + Alabarce
  Versão 2.0 — Módulo de lógica (sem Streamlit)
=============================================================

Otimizado para Streamlit Cloud:
  - Token JWT capturado via cookie 'vip-token' (requests puro)
  - Fallback Playwright com espera inteligente (sem timeout fixo)
  - Alabarce via requests + BeautifulSoup (sem Playwright quando possível)
  - Processamento sequencial — uso mínimo de RAM
  - Caminhos relativos — funciona em Linux e Windows
"""

import csv
import json
import os
import sys
import uuid
import logging
import unicodedata
from datetime import date

import requests as req

# Garante saída UTF-8 (suporte a emojis no terminal/Windows)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# ─────────────────────────────────────────────────────────────
#  LOGGING
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
ARQUIVO_ENTRADA  = os.path.join(_DIR, "produtos.csv")
ARQUIVO_ESCOLHAS = os.path.join(_DIR, "escolhas.json")
PASTA_RESULTADOS = os.path.join(_DIR, "dados", "precos")

# ─────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO DE LOJAS
# ─────────────────────────────────────────────────────────────

LOJAS = [
    {
        "nome":    "Semar",
        "url":     "https://www.semarentrega.com.br",
        "dominio": "semarentrega.com.br",
        "tipo":    "vipcommerce",
    },
    {
        "nome":    "Rossi",
        "url":     "https://www.rossidelivery.com.br",
        "dominio": "rossidelivery.com.br",
        "tipo":    "vipcommerce",
    },
    {
        "nome":    "Shibata",
        "url":     "https://www.loja.shibata.com.br",
        "dominio": "loja.shibata.com.br",
        "tipo":    "vipcommerce",
    },
    {
        "nome":    "Alabarce",
        "url":     "https://alabarce.net.br",
        "tipo":    "alabarce",
    },
]

LOJAS_CONHECIDAS = {"semar", "rossi", "shibata", "alabarce"}
DELAY_ENTRE_BUSCAS = 0.3

# ─────────────────────────────────────────────────────────────
#  COMPATIBILIDADE — nomes usados pela página Streamlit
# ─────────────────────────────────────────────────────────────

MAX_WORKERS_TOKENS = 1   # sequencial (antes era 4)
MAX_WORKERS_LOJAS  = 1   # sequencial (antes era 4)
_INALTERADO = object()   # sentinel: escolha veio do cache

# ─────────────────────────────────────────────────────────────
#  HEADERS COMUNS
# ─────────────────────────────────────────────────────────────

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
# =============================================================

def _capturar_token_via_requests(loja: dict) -> str | None:
    """
    Acessa o site da loja via requests e lê o cookie 'vip-token'.
    Retorna o JWT ou None.
    """
    try:
        session = req.Session()
        session.headers.update(_HEADERS_NAVEGADOR)
        resp = session.get(loja["url"], timeout=20, allow_redirects=True)
        resp.raise_for_status()

        for cookie in session.cookies:
            if cookie.name == "vip-token" and cookie.value and len(cookie.value) > 20:
                log.info(f"{loja['nome']}: token via cookie (sem browser)")
                return cookie.value

        log.info(f"{loja['nome']}: cookie vip-token não encontrado via requests")
        return None

    except Exception as e:
        log.warning(f"{loja['nome']}: falha requests: {e}")
        return None


def _capturar_token_via_playwright(loja: dict) -> dict | None:
    """
    Fallback: Playwright intercepta a primeira requisição VipCommerce
    que contenha um token JWT válido.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("Playwright não disponível")
        return None

    resultado = {
        "token": None, "sessao_id": None, "session": None,
        "org_id": None, "filial_id": None, "cd_id": None,
    }

    # Flag para interromper esperas quando a página crasha
    _page_crashed = [False]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox", "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage", "--disable-gpu",
                    "--no-zygote", "--disable-extensions",
                ],
            )
            context = browser.new_context(
                user_agent=_HEADERS_NAVEGADOR["User-Agent"]
            )
            page = context.new_page()
            # Crash do renderer é esperado em containers com memória limitada.
            # O token já foi capturado nos requests iniciais (antes do crash).
            page.on("crash", lambda _pg: (
                _page_crashed.__setitem__(0, True),
                log.warning("Playwright [%s]: renderer crashou (token já capturado: %s)",
                            loja["nome"], bool(resultado["token"]))
            ))
            page.set_default_timeout(30_000)

            def interceptar(request):
                if "vipcommerce" not in request.url:
                    return
                headers = request.headers
                auth = headers.get("authorization", "")
                token_valor = auth.replace("Bearer", "").replace("bearer", "").strip()

                # Captura token na primeira requisição autenticada
                if token_valor and len(token_valor) > 20 and not resultado["token"]:
                    resultado["token"]     = token_valor
                    resultado["sessao_id"] = headers.get("sessao-id", "").strip()

                url = request.url

                # Captura session de qualquer requisição
                if "session=" in url and not resultado["session"]:
                    resultado["session"] = url.split("session=")[-1].split("&")[0].strip()

                # Captura org/filial/cd de requisições com esses segmentos
                if "/org/" in url and not resultado["org_id"]:
                    partes = url.split("/")
                    try:
                        idx_org = partes.index("org")
                        resultado["org_id"] = partes[idx_org + 1].split("?")[0]
                    except (ValueError, IndexError):
                        pass
                    try:
                        idx_fil = partes.index("filial")
                        resultado["filial_id"] = partes[idx_fil + 1].split("?")[0]
                    except (ValueError, IndexError):
                        pass

                if "/centro_distribuicao/" in url and not resultado["cd_id"]:
                    partes = url.split("/")
                    try:
                        idx_cd = partes.index("centro_distribuicao")
                        resultado["cd_id"] = partes[idx_cd + 1].split("?")[0]
                    except (ValueError, IndexError):
                        pass

            page.on("request", interceptar)
            try:
                page.goto(loja["url"], wait_until="domcontentloaded", timeout=30_000)
            except Exception:
                pass  # goto pode lançar se a página crashar durante o carregamento

            # Espera até 15s pelo token (para imediatamente se página crashou)
            for _ in range(30):
                if resultado["token"] or _page_crashed[0]:
                    break
                page.wait_for_timeout(500)

            # Se pegou token mas não cd_id, digita algo para forçar
            # requisições à API de busca (que contém org/filial/cd na URL)
            if resultado["token"] and not resultado["cd_id"] and not _page_crashed[0]:
                try:
                    campo = page.locator(
                        "input[type='search'], input[type='text'], "
                        "input[placeholder*='usca'], input[placeholder*='esquisa']"
                    ).first
                    campo.click()
                    campo.type("arroz")
                    # Espera até 10s pelo cd_id
                    for _ in range(20):
                        if resultado["cd_id"] or _page_crashed[0]:
                            break
                        page.wait_for_timeout(500)
                except Exception:
                    pass

            # Última tentativa: espera mais um pouco caso requests estejam em voo
            if resultado["token"] and not resultado["cd_id"] and not _page_crashed[0]:
                page.wait_for_timeout(3000)

            # Tenta ler vip-token dos cookies do browser
            if not resultado["token"] and not _page_crashed[0]:
                try:
                    cookies = context.cookies()
                    for c in cookies:
                        if c["name"] == "vip-token" and c["value"] and len(c["value"]) > 20:
                            resultado["token"] = c["value"]
                            break
                except Exception:
                    pass

            try:
                browser.close()
            except Exception:
                pass  # browser pode já estar fechado após crash do renderer

        if resultado["token"]:
            log.info(f"{loja['nome']}: token via Playwright")
            return resultado
        log.warning(f"{loja['nome']}: Playwright não encontrou token")
        return None

    except Exception as e:
        log.error(f"{loja['nome']}: erro Playwright: {e}")
        return None


def _obter_dados_filial(token: str, dominio: str, sessao_id: str = "") -> dict | None:
    """GET /organizacoes/filiais/dominio/{dominio} → org_id, filial_id."""
    url = f"https://services.vipcommerce.com.br/organizacoes/filiais/dominio/{dominio}"

    # Determina o Origin correto (pode ter www. ou não)
    # Para "loja.shibata.com.br" → Origin é "https://www.loja.shibata.com.br"
    origin = f"https://www.{dominio}"

    headers = {
        **_HEADERS_NAVEGADOR,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": f"{origin}/",
        "sessao-id": sessao_id,
    }
    try:
        resp = req.get(url, headers=headers, timeout=15)
        # Verifica se tem corpo antes de parsear
        if resp.status_code == 304 or not resp.text or not resp.text.strip():
            log.warning(f"Filial ({dominio}): resposta vazia (status {resp.status_code})")
            return None
        if resp.status_code != 200:
            log.warning(f"Filial ({dominio}): status {resp.status_code}")
            return None
        data = resp.json().get("data", {})
        org  = data.get("organizacao", {})
        result = {
            "filial_id": str(data.get("id", "")),
            "org_id":    str(org.get("id", "")),
        }
        if result["org_id"] and result["filial_id"]:
            return result
        log.warning(f"Filial ({dominio}): org_id ou filial_id vazio no JSON")
        return None
    except Exception as e:
        log.warning(f"Erro filial ({dominio}): {e}")
        return None


def _obter_cd_id(token: str, org_id: str, filial_id: str, dominio: str) -> str | None:
    """Tenta descobrir o cd_id via endpoints conhecidos."""
    base = "https://services.vipcommerce.com.br"
    headers = {
        **_HEADERS_NAVEGADOR,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "domainkey": dominio,
        "organizationid": org_id,
        "filialid": filial_id,
    }
    endpoints = [
        f"{base}/api-admin/v1/org/{org_id}/filial/{filial_id}/centros_distribuicao",
        f"{base}/api-admin/v1/org/{org_id}/filial/{filial_id}/centro_distribuicao",
        f"{base}/organizacoes/{org_id}/filiais/{filial_id}/centros-distribuicao",
    ]
    for url in endpoints:
        try:
            resp = req.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if isinstance(data, dict) and "data" in data:
                items = data["data"]
                if isinstance(items, list) and items:
                    return str(items[0].get("id", ""))
                if isinstance(items, dict):
                    return str(items.get("id", ""))
            elif isinstance(data, list) and data:
                return str(data[0].get("id", ""))
        except Exception:
            continue
    return None


def capturar_dados_loja(loja: dict) -> dict:
    """
    Captura token + IDs para uma loja VipCommerce.
    Estratégia: cookie requests → Playwright fallback.

    Retorna dict com token, sessao_id, session, org_id, filial_id, cd_id.
    Campos podem ser None se não capturados.
    """
    resultado = {
        "token": None, "sessao_id": None, "session": None,
        "org_id": None, "filial_id": None, "cd_id": None,
    }

    # ── Estratégia 1: cookie via requests ────────────────────
    token = _capturar_token_via_requests(loja)
    if token:
        resultado["token"]     = token
        resultado["sessao_id"] = str(uuid.uuid4())
        resultado["session"]   = str(uuid.uuid4())

        dados_filial = _obter_dados_filial(token, loja["dominio"], resultado["sessao_id"])
        if dados_filial:
            resultado["org_id"]    = dados_filial["org_id"]
            resultado["filial_id"] = dados_filial["filial_id"]

            cd_id = _obter_cd_id(token, dados_filial["org_id"],
                                 dados_filial["filial_id"], loja["dominio"])
            if cd_id:
                resultado["cd_id"] = cd_id
                log.info(f"{loja['nome']}: completo via requests (sem browser)")
                return resultado

    # ── Estratégia 2: Playwright fallback ────────────────────
    log.info(f"{loja['nome']}: tentando Playwright...")
    dados_pw = _capturar_token_via_playwright(loja)
    if dados_pw:
        for chave in resultado:
            if not resultado[chave] and dados_pw.get(chave):
                resultado[chave] = dados_pw[chave]

        if not resultado["org_id"] and resultado["token"]:
            sessao = resultado.get("sessao_id") or str(uuid.uuid4())
            dados_filial = _obter_dados_filial(resultado["token"], loja["dominio"], sessao)
            if dados_filial:
                resultado["org_id"]    = dados_filial["org_id"]
                resultado["filial_id"] = dados_filial["filial_id"]
                if not resultado["cd_id"]:
                    cd_id = _obter_cd_id(resultado["token"], dados_filial["org_id"],
                                         dados_filial["filial_id"], loja["dominio"])
                    if cd_id:
                        resultado["cd_id"] = cd_id

    if resultado["token"]:
        log.info(
            f"{loja['nome']}: org={resultado.get('org_id')} cd={resultado.get('cd_id')} "
            f"sessao_id={resultado.get('sessao_id', '')[:8]}... "
            f"session={resultado.get('session', '')[:8]}... "
            f"token={resultado.get('token', '')[:20]}..."
        )
    else:
        log.error(f"{loja['nome']}: falha total na captura")

    return resultado


# =============================================================
#  2. FUNÇÕES DE API — VipCommerce (preservadas)
# =============================================================

def normalizar(texto: str) -> str:
    """Remove acentos e converte para minúsculas."""
    texto = texto.lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def montar_headers(loja: dict, dados: dict) -> dict:
    return {
        "Authorization":  f"Bearer {dados['token']}",
        "domainkey":      loja["dominio"],
        "organizationid": dados["org_id"],
        "filialid":       dados["filial_id"],
        "sessao-id":      dados.get("sessao_id", ""),
        "Content-Type":   "application/json",
        "Accept":         "application/json",
        "User-Agent":     _HEADERS_NAVEGADOR["User-Agent"],
    }


def buscar_produto(termo: str, loja: dict, dados: dict) -> list | None:
    """Busca na API VipCommerce. Retorna lista, [] ou None (token expirado)."""
    base = "https://services.vipcommerce.com.br/api-admin/v1"

    # Monta URL com session
    url_base = (
        f"{base}/org/{dados['org_id']}/filial/{dados['filial_id']}"
        f"/centro_distribuicao/{dados['cd_id']}/loja/buscas/produtos"
        f"/termo/{req.utils.quote(termo)}"
    )
    session_val = dados.get("session", "")
    urls_tentar = [
        f"{url_base}?page=1&session={session_val}" if session_val else f"{url_base}?page=1",
        f"{url_base}?page=1",  # retry sem session
    ]

    headers = montar_headers(loja, dados)

    for url in urls_tentar:
        try:
            r = req.get(url, headers=headers, timeout=15)
            if r.status_code == 401:
                return None  # token expirado
            if r.status_code == 403:
                log.warning(
                    f"403 para '{termo}' em {loja['nome']} "
                    f"(sessao_id={dados.get('sessao_id', '')[:8]}...) — tentando próxima URL"
                )
                continue  # tenta sem session
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
            log.error(f"Erro busca '{termo}' em {loja['nome']}: {e}")
            return []

    log.warning(f"Todas as tentativas falharam (403) para '{termo}' em {loja['nome']}")
    return []


def buscar_detalhes(produto_id, loja: dict, dados: dict) -> dict | None:
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
        log.error(f"Erro detalhes {produto_id} em {loja['nome']}: {e}")
        return None


def extrair_preco(p: dict) -> str | None:
    """Extrai preço (oferta / normal / por kg)."""
    em_oferta = p.get("em_oferta", False)
    oferta    = p.get("oferta") or {}

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
#  3. LÓGICA DE MATCH (preservada)
# =============================================================

def encontrar_candidatos(termo: str, lista_produtos: list) -> list:
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
    produto_id = produto.get("produto_id") or produto.get("id")
    descricao  = produto.get("descricao") or produto.get("nome", "")
    disponivel = produto.get("disponivel", True)
    preco = extrair_preco(produto)

    if preco is None and lista_produtos is not None:
        item = next(
            (p for p in lista_produtos if p.get("produto_id") == produto.get("produto_id")),
            None,
        )
        if item:
            preco = extrair_preco(item)

    if preco is None and produto_id:
        detalhes = buscar_detalhes(produto_id, loja, dados)
        if detalhes:
            preco      = extrair_preco(detalhes) or preco
            disponivel = detalhes.get("disponivel", disponivel)

    status      = "OK" if disponivel else "Indisponível"
    preco_final = preco if disponivel else ""
    return {"descricao": descricao, "preco": preco_final, "status": status}


# =============================================================
#  4. ALABARCE — requests + BS4 (Playwright fallback)
# =============================================================

def _buscar_alabarce_requests(termo: str) -> list:
    """Busca no Alabarce via requests + BeautifulSoup."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.warning("BeautifulSoup não instalado")
        return []

    url = f"https://alabarce.net.br/products?utf8=%E2%9C%93&keywords={req.utils.quote(termo)}"
    try:
        resp = req.get(url, headers=_HEADERS_NAVEGADOR, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        cards = soup.select(".product-cards .product")
        if not cards:
            cards = soup.select("[class*='product']")

        produtos = []
        for card in cards:
            el_nome = card.select_one("h5.product-title") or card.select_one("[class*='title']")
            if not el_nome:
                continue
            nome = el_nome.get_text(strip=True).upper()
            if not nome:
                continue

            preco_num = None
            el_preco = card.select_one(".price-amount span") or card.select_one("[class*='price'] span")
            if el_preco:
                preco_clean = (
                    el_preco.get_text(strip=True)
                    .replace("R$", "").replace("\xa0", "").strip()
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
        log.error(f"Erro Alabarce requests '{termo}': {e}")
        return []


def _dispensar_popup_loja_alabarce(page):
    """Fecha o popup de seleção de loja do Alabarce."""
    try:
        modal = page.locator("#stock-picker")
        if modal.count() == 0 or not modal.is_visible():
            return
        btn = modal.locator("a[href*='current_stock'][href*='withdrawal']")
        if btn.count() > 0:
            btn.first.click()
            try:
                page.locator("#stock-picker").wait_for(state="hidden", timeout=5000)
            except Exception:
                pass
    except Exception:
        pass


def _buscar_alabarce_com_page(termo: str, page) -> list:
    """Busca usando uma page Playwright já aberta."""
    try:
        url = f"https://alabarce.net.br/products?utf8=%E2%9C%93&keywords={req.utils.quote(termo)}"
        page.goto(url, wait_until="domcontentloaded")
        _dispensar_popup_loja_alabarce(page)

        try:
            page.locator(".product-cards .product").first.wait_for(timeout=8000)
            page.wait_for_timeout(500)
        except Exception:
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
                    preco_clean = (
                        el_preco.first.inner_text()
                        .replace("R$", "").replace("\xa0", "").strip()
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
        return produtos
    except Exception as e:
        log.error(f"Erro Alabarce page '{termo}': {e}")
        return []


def buscar_produto_alabarce(termo: str, driver=None) -> list:
    """
    Busca no Alabarce.
    - Se driver fornecido (Playwright): usa ele (compat legado)
    - Senão: tenta requests, fallback Playwright efêmero
    """
    if driver is not None:
        page = getattr(driver, "page", driver)
        return _buscar_alabarce_com_page(termo, page)

    # Tenta requests primeiro
    produtos = _buscar_alabarce_requests(termo)
    if produtos:
        return produtos

    # Fallback: Playwright efêmero
    log.info(f"Alabarce: '{termo}' fallback Playwright...")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage", "--disable-gpu", "--no-zygote"],
            )
            page = browser.new_page()
            resultado = _buscar_alabarce_com_page(termo, page)
            browser.close()
            return resultado
    except Exception as e:
        log.error(f"Erro Alabarce Playwright '{termo}': {e}")
        return []


# ── Compat legado: driver reutilizável ───────────────────────

class _PlaywrightContext:
    """Contexto Playwright reutilizável."""

    def __init__(self):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-gpu", "--no-zygote"],
        )
        self.page = self.browser.new_page()
        self.page.set_default_timeout(30_000)

    def quit(self):
        try:
            self.browser.close()
        except Exception:
            pass
        try:
            self._pw.stop()
        except Exception:
            pass


def _criar_driver_alabarce():
    """Cria driver Playwright reutilizável para o Alabarce."""
    try:
        ctx = _PlaywrightContext()
        try:
            ctx.page.goto("https://alabarce.net.br", wait_until="domcontentloaded")
            ctx.page.wait_for_timeout(2000)
            _dispensar_popup_loja_alabarce(ctx.page)
        except Exception:
            pass
        return ctx
    except Exception as e:
        log.error(f"Erro ao criar driver Alabarce: {e}")
        return None


def _processar_alabarce_salvo(entrada: dict, nome: str, driver) -> dict:
    """Re-busca produto Alabarce já salvo para obter preço atualizado."""
    desc_salva = normalizar(entrada.get("descricao", ""))

    if driver is not None:
        lista_atual = buscar_produto_alabarce(nome, driver)
    else:
        lista_atual = buscar_produto_alabarce(nome)

    match = next(
        (p for p in lista_atual if normalizar(p.get("descricao", "")) == desc_salva),
        None,
    )

    if match:
        return {
            "descricao": match["descricao"],
            "preco":     match["preco"],
            "status":    "OK" if match["preco"] else "Indisponível",
        }

    return {
        "descricao": entrada.get("descricao", ""),
        "preco":     entrada.get("preco", ""),
        "status":    "OK" if entrada.get("preco") else "Indisponível",
    }


# =============================================================
#  5. CACHE DE ESCOLHAS (preservado)
# =============================================================

def _slim(entrada) -> dict | None:
    if entrada is None:
        return None
    if isinstance(entrada, dict):
        return {
            "produto_id": entrada.get("produto_id"),
            "descricao":  entrada.get("descricao", ""),
        }
    return entrada


def carregar_escolhas() -> dict:
    if os.path.exists(ARQUIVO_ESCOLHAS):
        try:
            with open(ARQUIVO_ESCOLHAS, encoding="utf-8") as f:
                dados = json.load(f)
            novo = {}
            for loja in LOJAS_CONHECIDAS:
                if loja in dados and isinstance(dados[loja], dict):
                    novo[loja] = {k: _slim(v) for k, v in dados[loja].items()}
            return novo
        except Exception as e:
            log.warning(f"Erro ao carregar escolhas: {e}")
    return {}


def salvar_escolhas(escolhas: dict):
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
#  6. PROCESSAR LOJA VC (compat com página Streamlit)
# =============================================================

def _processar_loja_vc(nome: str, loja: dict, dados: dict,
                       escolhas_loja: dict, chave: str) -> tuple:
    """
    Retorna (campos_csv, escolha_para_salvar):
      - _INALTERADO = cache hit
      - None = não encontrado
      - dict = produto escolhido
      - "TOKEN_EXPIRED" = token expirado
    """
    loja_nome = loja["nome"]

    # Cache hit
    if chave in escolhas_loja:
        entrada = escolhas_loja[chave]
        if entrada is None:
            campos = {
                f"Produto Encontrado ({loja_nome})": "",
                f"Preço ({loja_nome})":              "",
                f"Status ({loja_nome})":             "Não encontrado",
            }
            return campos, _INALTERADO

        lista = buscar_produto(nome, loja, dados) or []
        res = processar_match(nome, entrada, loja, dados, lista_produtos=lista)
        log.info(f"  [{loja_nome}] {res['descricao']} | R$ {res['preco']} | {res['status']}")
        campos = {
            f"Produto Encontrado ({loja_nome})": res["descricao"],
            f"Preço ({loja_nome})":              res["preco"],
            f"Status ({loja_nome})":             res["status"],
        }
        return campos, _INALTERADO

    # Produto novo
    lista = buscar_produto(nome, loja, dados)
    if lista is None:
        return None, "TOKEN_EXPIRED"

    if not lista:
        campos = {
            f"Produto Encontrado ({loja_nome})": "",
            f"Preço ({loja_nome})":              "",
            f"Status ({loja_nome})":             "Não encontrado",
        }
        return campos, None

    candidatos = encontrar_candidatos(nome, lista)
    if not candidatos:
        campos = {
            f"Produto Encontrado ({loja_nome})": "",
            f"Preço ({loja_nome})":              "",
            f"Status ({loja_nome})":             "Sem match",
        }
        return campos, None

    # Auto-seleciona se só 1 candidato
    if len(candidatos) == 1:
        escolhido = candidatos[0]
        res = processar_match(nome, escolhido, loja, dados)
        campos = {
            f"Produto Encontrado ({loja_nome})": res["descricao"],
            f"Preço ({loja_nome})":              res["preco"],
            f"Status ({loja_nome})":             res["status"],
        }
        return campos, _slim(escolhido)

    # Sem terminal → auto-seleciona primeiro
    if not sys.stdin.isatty():
        escolhido = candidatos[0]
        res = processar_match(nome, escolhido, loja, dados)
        log.info(f"  [AUTO] {loja_nome}: {res['descricao']}")
        campos = {
            f"Produto Encontrado ({loja_nome})": res["descricao"],
            f"Preço ({loja_nome})":              res["preco"],
            f"Status ({loja_nome})":             res["status"],
        }
        return campos, _slim(escolhido)

    # Terminal interativo (fallback)
    print(f"\n  🔍 '{nome}' em {loja_nome} — escolha:")
    print("   [0] Nenhum / Pular")
    for i, c in enumerate(candidatos, 1):
        preco = extrair_preco(c) or c.get("preco", "?")
        print(f"   [{i}] {c.get('descricao','?')}  R$ {preco}")
    while True:
        try:
            n = int(input("  👉 Número: ").strip())
            if n == 0:
                return {
                    f"Produto Encontrado ({loja_nome})": "",
                    f"Preço ({loja_nome})":              "",
                    f"Status ({loja_nome})":             "Não encontrado",
                }, None
            if 1 <= n <= len(candidatos):
                escolhido = candidatos[n - 1]
                res = processar_match(nome, escolhido, loja, dados)
                campos = {
                    f"Produto Encontrado ({loja_nome})": res["descricao"],
                    f"Preço ({loja_nome})":              res["preco"],
                    f"Status ({loja_nome})":             res["status"],
                }
                return campos, _slim(escolhido)
        except (ValueError, EOFError):
            escolhido = candidatos[0]
            res = processar_match(nome, escolhido, loja, dados)
            campos = {
                f"Produto Encontrado ({loja_nome})": res["descricao"],
                f"Preço ({loja_nome})":              res["preco"],
                f"Status ({loja_nome})":             res["status"],
            }
            return campos, _slim(escolhido)
