"""
=============================================================
  Busca de Preços — VipCommerce (multi-loja) + Alabarce
=============================================================

COMO USAR:
1. Configure os domínios das lojas na seção LOJAS abaixo
2. Rode: python buscar_precos.py
3. O Chrome abre automaticamente para cada loja, captura o
   token e descobre org_id/cd_id sozinho
4. Na 1ª execução você escolhe os produtos manualmente
5. Nas execuções seguintes tudo é automático

PARA RODAR AUTOMATICAMENTE AO LIGAR O PC:
  Veja as instruções no final deste arquivo.

ARQUIVOS GERADOS:
  - precos_resultado.csv  → preços de todas as lojas
  - escolhas.json         → suas escolhas salvas (não apague)
"""

import csv
import json
import os
import sys
import threading
import time
import unicodedata
import shutil
import subprocess
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Garante saída UTF-8 no Windows (suporte a emojis no terminal)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Força UTF-8 nos subprocessos filhos — evita cp1252 no Windows
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
os.environ["PYTHONLEGACYWINDOWSSTDIO"] = "0"

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

_DIR = os.path.dirname(os.path.abspath(__file__))

# =============================================================
# 🎭  PLAYWRIGHT — instalação única por processo
# =============================================================

_playwright_ready = False
_playwright_install_lock = threading.Lock()


def _garantir_playwright():
    """Instala o browser Chromium do Playwright uma vez por processo."""
    global _playwright_ready
    if _playwright_ready:
        return
    with _playwright_install_lock:
        if _playwright_ready:
            return
        try:
            subprocess.run(
                ["playwright", "install", "chromium"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=300,
            )
        except Exception as e:
            print(f"⚠️  playwright install chromium: {e}")
        _playwright_ready = True


_ARGS_CHROME = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--window-size=1280,800",
]


class _PlaywrightContext:
    """Agrupa playwright, browser e page para facilitar o ciclo de vida."""

    def __init__(self):
        _garantir_playwright()
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch(headless=True, args=_ARGS_CHROME)
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


# =============================================================
# ⚙️  CONFIGURAÇÃO DE LOJAS
#
# tipo "vipcommerce" → usa API VipCommerce (token automático)
# tipo "alabarce"    → usa Playwright + scraping HTML
# =============================================================

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
        "nome": "Alabarce",
        "url":  "https://alabarce.net.br",
        "tipo": "alabarce",
    },
]

ARQUIVO_ENTRADA  = os.path.join(_DIR, "produtos.csv")
ARQUIVO_SAIDA    = os.path.join(_DIR, "precos_resultado.csv")
ARQUIVO_ESCOLHAS = os.path.join(_DIR, "escolhas.json")
DELAY_ENTRE_BUSCAS = 0.3   # delay por produto (não por loja)
MAX_WORKERS_TOKENS = 4     # lojas VC em paralelo na captura de tokens
MAX_WORKERS_LOJAS  = 4     # lojas VC em paralelo por produto

# Lock para serializar interação com o usuário quando há novos produtos
_INTERACAO_LOCK = threading.Lock()
# Sentinel: escolha não modificada (cache hit, não precisa salvar)
_INALTERADO = object()

LOJAS_CONHECIDAS = {"semar", "rossi", "shibata", "alabarce"}


def _slim(entrada):
    """Extrai apenas produto_id e descricao de um objeto de produto."""
    if entrada is None:
        return None
    if isinstance(entrada, dict):
        return {
            "produto_id": entrada.get("produto_id"),
            "descricao":  entrada.get("descricao", ""),
        }
    return entrada


def _migrar_escolhas_se_necessario(dados: dict) -> tuple:
    """
    Detecta formato antigo (chaves de produto na raiz) e migra para o novo
    (apenas chaves de loja na raiz, valores com só produto_id + descricao).
    Retorna (dados_migrados, foi_migrado).
    """
    chaves_raiz = set(dados.keys())
    chaves_nao_loja = chaves_raiz - LOJAS_CONHECIDAS

    if not chaves_nao_loja:
        # Já está no formato novo — só garantir que os valores internos são slim
        novo = {}
        for loja in LOJAS_CONHECIDAS:
            if loja in dados and isinstance(dados[loja], dict):
                novo[loja] = {
                    k: _slim(v) for k, v in dados[loja].items()
                }
        return novo, False

    # Formato antigo detectado: migrar
    print("⚠️  escolhas.json em formato antigo — migrando automaticamente...")
    novo = {}
    for loja in LOJAS_CONHECIDAS:
        if loja in dados and isinstance(dados[loja], dict):
            novo[loja] = {
                k: _slim(v) for k, v in dados[loja].items()
            }
    return novo, True

# =============================================================
# 🌐  CAPTURA AUTOMÁTICA DE TOKEN E PARÂMETROS VIA PLAYWRIGHT
#     (apenas lojas VipCommerce)
# =============================================================

def capturar_dados_loja(loja):
    """
    Abre o Chromium, acessa a loja, faz uma busca de 'arroz' para
    forçar requisições à API e captura automaticamente:
      - token JWT (Authorization)
      - sessao_id (header)
      - session  (query param)
      - org_id, filial_id, cd_id (extraídos da URL da API)

    Tenta até 3 vezes antes de desistir. Em caso de falha total,
    retorna dict com todos os valores None (nunca levanta exceção).
    """
    _RESULTADO_VAZIO = {
        "token": None, "sessao_id": None, "session": None,
        "org_id": None, "filial_id": None, "cd_id": None,
    }
    _MAX_TENTATIVAS = 3

    for tentativa in range(1, _MAX_TENTATIVAS + 1):
        print(f"\n🌐 Capturando token: {loja['nome']} (tentativa {tentativa}/{_MAX_TENTATIVAS})")
        resultado = {
            "token": None, "sessao_id": None, "session": None,
            "org_id": None, "filial_id": None, "cd_id": None,
        }
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                )
                context = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    )
                )
                page = context.new_page()

                # CDP session — captura requisições exatamente como o Selenium
                # performance log fazia com Network.requestWillBeSent
                client = context.new_cdp_session(page)
                client.send("Network.enable")

                urls_capturadas = []  # debug

                def on_cdp_request(params):
                    req = params.get("request", {})
                    url = req.get("url", "")
                    headers = req.get("headers", {})

                    urls_capturadas.append(url[:100])  # debug

                    if "vipcommerce.com.br" not in url:
                        return

                    auth = headers.get("Authorization") or headers.get("authorization", "")
                    sid  = headers.get("sessao-id") or headers.get("Sessao-Id", "")

                    if auth.startswith("Bearer ") and sid and not resultado["token"]:
                        resultado["token"]     = auth.replace("Bearer ", "").strip()
                        resultado["sessao_id"] = sid.strip()

                    if "session=" in url and not resultado["session"]:
                        resultado["session"] = url.split("session=")[-1].split("&")[0].strip()

                    if "/org/" in url and "/centro_distribuicao/" in url and not resultado["org_id"]:
                        partes = url.split("/")
                        try:
                            resultado["org_id"]    = partes[partes.index("org") + 1]
                            resultado["filial_id"] = partes[partes.index("filial") + 1]
                            resultado["cd_id"]     = partes[partes.index("centro_distribuicao") + 1]
                        except (ValueError, IndexError):
                            pass

                client.on("Network.requestWillBeSent", on_cdp_request)

                page.goto(loja["url"], wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(5000)

                # Tenta digitar no campo de busca
                try:
                    campo = page.locator(
                        "input[type='search'], input[type='text'], "
                        "input[placeholder*='usca'], input[placeholder*='esquisa']"
                    ).first
                    campo.click()
                    campo.type("arroz")
                    page.wait_for_timeout(4000)
                except Exception:
                    pass

                # Debug: exibe quantas URLs foram capturadas e amostra
                vc_urls = [u for u in urls_capturadas if "vipcommerce" in u]
                print(f"  🔍 {len(urls_capturadas)} URLs capturadas, {len(vc_urls)} vipcommerce")
                if not vc_urls and urls_capturadas:
                    print(f"  🔍 Amostra: {urls_capturadas[:3]}")

                # Tenta pegar session do localStorage como fallback
                if resultado["token"] and not resultado["session"]:
                    try:
                        resultado["session"] = page.evaluate("localStorage.getItem('session')")
                    except Exception:
                        pass

                browser.close()

            if not resultado["token"]:
                raise RuntimeError("token não encontrado nas requisições")
            if not resultado["org_id"]:
                raise RuntimeError("org_id não encontrado na URL")

            print(f"✅ {loja['nome']}: org={resultado['org_id']} | cd={resultado['cd_id']}")
            return resultado

        except Exception as exc:
            import traceback
            print(f"  ⚠️  {loja['nome']} tentativa {tentativa} falhou: {exc}")
            print(f"  🔍 Traceback:\n{traceback.format_exc()}")
            if tentativa < _MAX_TENTATIVAS:
                print(f"  🔄 Aguardando 5s antes de tentar novamente...")
                time.sleep(5)

    print(f"❌ {loja['nome']}: todas as tentativas falharam.")
    return _RESULTADO_VAZIO


# =============================================================
# 🌿  ALABARCE — Playwright + scraping HTML
# =============================================================

def _dispensar_popup_loja_alabarce(page):
    """
    Fecha o popup de seleção de loja/entrega do Alabarce (#stock-picker).
    Clica automaticamente em 'Retirar na loja' para definir a sessão.
    Não levanta exceção se o popup não estiver presente.
    """
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
        pass  # popup não presente ou já fechado


def _criar_driver_alabarce() -> _PlaywrightContext:
    """Cria e retorna um _PlaywrightContext reutilizável para o Alabarce.
    Já resolve o popup de seleção de loja na inicialização."""
    ctx = _PlaywrightContext()
    try:
        ctx.page.goto("https://alabarce.net.br", wait_until="domcontentloaded")
        ctx.page.wait_for_timeout(2000)
        _dispensar_popup_loja_alabarce(ctx.page)
    except Exception:
        pass
    return ctx


def buscar_produto_alabarce(termo, page):
    """
    Busca um produto no Alabarce via Playwright.

    Navega diretamente para a URL de busca:
        https://alabarce.net.br/products?utf8=✓&keywords={termo}

    Retorna lista de dicts compatível com encontrar_candidatos:
        [{"descricao": str, "preco": str, "disponivel": bool}, ...]
    """
    try:
        url_busca = (
            "https://alabarce.net.br/products"
            f"?utf8=%E2%9C%93&keywords={requests.utils.quote(termo)}"
        )
        page.goto(url_busca, wait_until="domcontentloaded")

        # Dispensar popup de loja se reaparecer
        _dispensar_popup_loja_alabarce(page)

        # Aguarda os cards de produto aparecerem (máx 8s)
        try:
            page.locator(".product-cards .product").first.wait_for(timeout=8000)
            page.wait_for_timeout(500)
        except Exception:
            return []  # sem resultados para este termo

        cards = page.locator(".product-cards .product").all()
        produtos = []

        for card in cards:
            # Nome
            try:
                el_nome = card.locator("h5.product-title")
                if el_nome.count() == 0:
                    continue
                nome = el_nome.first.inner_text().strip().upper()
            except Exception:
                continue

            if not nome:
                continue

            # Preço — pega o preço atual (promoção ou normal)
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
                "descricao":  nome,
                "preco":      f"{preco_num:.2f}" if preco_num else "",
                "disponivel": True,
            })

        return produtos

    except Exception as e:
        print(f"  ❌ Erro na busca Alabarce '{termo}': {e}")
        return []


def _processar_alabarce_salvo(entrada, nome, page):
    """
    Quando a escolha já está salva para o Alabarce,
    re-busca o produto para obter o preço atual.
    Fallback para os dados salvos se o produto sumir do site.
    """
    desc_salva = normalizar(entrada.get("descricao", ""))

    lista_atual = buscar_produto_alabarce(nome, page)

    # Tenta achar o mesmo produto pelo nome normalizado
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

    # Fallback: usa dados salvos (produto pode estar fora de estoque temporariamente)
    return {
        "descricao": entrada.get("descricao", ""),
        "preco":     entrada.get("preco", ""),
        "status":    "OK" if entrada.get("preco") else "Indisponível",
    }


# =============================================================
# 🔧  FUNÇÕES DE API  (VipCommerce)
# =============================================================

def normalizar(texto):
    texto = texto.lower().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto


def montar_headers(loja, dados):
    return {
        "Authorization":  f"Bearer {dados['token']}",
        "domainkey":      loja["dominio"],
        "organizationid": dados["org_id"],
        "filialid":       dados["filial_id"],
        "sessao-id":      dados["sessao_id"],
        "Content-Type":   "application/json",
        "Accept":         "application/json",
    }


def buscar_produto(termo, loja, dados):
    base = "https://services.vipcommerce.com.br/api-admin/v1"
    url  = (
        f"{base}/org/{dados['org_id']}/filial/{dados['filial_id']}"
        f"/centro_distribuicao/{dados['cd_id']}/loja/buscas/produtos/termo/{requests.utils.quote(termo)}"
        f"?page=1&&session={dados['session']}"
    )
    try:
        r = requests.get(url, headers=montar_headers(loja, dados), timeout=15)
        if r.status_code == 401:
            return None  # sinaliza token expirado
        if r.status_code != 200:
            print(f"  ⚠️  Status {r.status_code} para '{termo}' em {loja['nome']}")
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
        print(f"  ❌ Erro na busca '{termo}' em {loja['nome']}: {e}")
        return []


def buscar_detalhes(produto_id, loja, dados):
    base = "https://services.vipcommerce.com.br/api-admin/v1"
    url  = (
        f"{base}/org/{dados['org_id']}/filial/{dados['filial_id']}"
        f"/centro_distribuicao/{dados['cd_id']}/loja/produtos/{produto_id}/detalhes"
    )
    try:
        r = requests.get(url, headers=montar_headers(loja, dados), timeout=15)
        if r.status_code != 200:
            return None
        d = r.json()
        return d.get("data", d) if isinstance(d, dict) else d
    except Exception as e:
        print(f"  ❌ Erro nos detalhes {produto_id} em {loja['nome']}: {e}")
        return None


def extrair_preco(p):
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
# 🔧  LÓGICA DE MATCH
# =============================================================

def encontrar_candidatos(termo, lista_produtos):
    IGNORAR = {"de", "da", "do", "das", "dos", "com", "em"}
    EXCLUIR = {"polpa", "semente", "swift", "suco", "refresco", "refrigerante",
               "bolo", "biscoito", "creme", "granola", "azeitona", "congelado",
               "congelada", "feltrin", "desidratado", "desidratada"}

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


# =============================================================
# 🔧  ESCOLHAS SALVAS (separadas por loja)
# =============================================================

def carregar_escolhas():
    if os.path.exists(ARQUIVO_ESCOLHAS):
        with open(ARQUIVO_ESCOLHAS, encoding="utf-8") as f:
            dados = json.load(f)
        dados_migrados, foi_migrado = _migrar_escolhas_se_necessario(dados)
        if foi_migrado:
            shutil.copy(ARQUIVO_ESCOLHAS, ARQUIVO_ESCOLHAS + ".bak")
            print("💾 Backup criado: escolhas.json.bak")
            salvar_escolhas(dados_migrados)
            print("✅ escolhas.json migrado para novo formato.")
        return dados_migrados
    return {}


def salvar_escolhas(escolhas):
    dados_slim = {
        loja: {k: _slim(v) for k, v in produtos.items()}
        for loja, produtos in escolhas.items()
        if isinstance(produtos, dict)
    }
    with open(ARQUIVO_ESCOLHAS, "w", encoding="utf-8") as f:
        json.dump(dados_slim, f, ensure_ascii=False, indent=2)


def perguntar_usuario(nome, loja_nome, candidatos):
    """Pede escolha ao usuário. Se não houver terminal (rodando via Streamlit),
    seleciona automaticamente o primeiro candidato."""
    if not sys.stdin.isatty():
        print(f"  [AUTO] {loja_nome}: escolhendo automaticamente → {candidatos[0].get('descricao','?')}")
        return candidatos[0]

    print(f"\n  🔍 '{nome}' em {loja_nome} — escolha o produto:")
    print("   [0] Nenhum / Pular")
    for i, c in enumerate(candidatos, 1):
        preco = extrair_preco(c) or c.get("preco", "?")
        print(f"   [{i}] {c.get('descricao','?')}  R$ {preco}")

    while True:
        try:
            n = int(input("  👉 Escolha o número: ").strip())
            if n == 0:
                return None
            if 1 <= n <= len(candidatos):
                return candidatos[n - 1]
            print(f"  ⚠️  Digite entre 0 e {len(candidatos)}.")
        except ValueError:
            print("  ⚠️  Digite apenas um número.")
        except EOFError:
            print(f"  [AUTO] stdin encerrado → escolhendo automaticamente: {candidatos[0].get('descricao','?')}")
            return candidatos[0]


def processar_match(nome, produto, loja, dados, termo=None, lista_produtos=None):
    produto_id  = produto.get("produto_id") or produto.get("id")
    descricao   = produto.get("descricao") or produto.get("nome", "")
    disponivel  = produto.get("disponivel", True)
    preco_promo = produto.get("precoPromocional", "")

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
            preco       = extrair_preco(detalhes) or preco
            preco_promo = detalhes.get("precoPromocional", "") or preco_promo
            disponivel  = detalhes.get("disponivel", disponivel)

    status      = "OK" if disponivel else "Indisponível"
    preco_final = preco if disponivel else ""

    return {
        "descricao":  descricao,
        "preco":      preco_final,
        "status":     status,
    }


# =============================================================
# ⚡  HELPER PARALELO — processa UMA loja VipCommerce
# =============================================================

def _processar_loja_vc(nome, loja, dados, escolhas_loja, chave):
    """
    Processa uma loja VipCommerce para um produto. Thread-safe.

    Retorna (campos_csv, escolha_para_salvar):
      - campos_csv: dict com as 3 colunas CSV da loja
      - escolha_para_salvar: _INALTERADO (cache hit) | None (não encontrado) | dict (produto)
      Quando escolha_para_salvar é "TOKEN_EXPIRED", campos_csv é None.
    """
    loja_nome = loja["nome"]

    # ── Cache hit ──────────────────────────────────────────────
    if chave in escolhas_loja:
        entrada = escolhas_loja[chave]
        if entrada is None:
            campos = {
                f"Produto Encontrado ({loja_nome})": "",
                f"Preço ({loja_nome})":              "",
                f"Status ({loja_nome})":             "Não encontrado",
            }
            return campos, _INALTERADO

        res = processar_match(nome, entrada, loja, dados,
                              lista_produtos=buscar_produto(nome, loja, dados))
        print(f"  [{loja_nome}] → {res['descricao']} | R$ {res['preco']} | {res['status']}")
        campos = {
            f"Produto Encontrado ({loja_nome})": res["descricao"],
            f"Preço ({loja_nome})":              res["preco"],
            f"Status ({loja_nome})":             res["status"],
        }
        return campos, _INALTERADO

    # ── Produto novo: buscar na API ────────────────────────────
    lista = buscar_produto(nome, loja, dados)

    if lista is None:
        return None, "TOKEN_EXPIRED"

    if not lista:
        print(f"  [{loja_nome}] → Nenhum resultado na API.")
        campos = {
            f"Produto Encontrado ({loja_nome})": "",
            f"Preço ({loja_nome})":              "",
            f"Status ({loja_nome})":             "Não encontrado",
        }
        return campos, None  # salvar None

    candidatos = encontrar_candidatos(nome, lista)
    if not candidatos:
        print(f"  [{loja_nome}] → Nenhum candidato válido.")
        campos = {
            f"Produto Encontrado ({loja_nome})": "",
            f"Preço ({loja_nome})":              "",
            f"Status ({loja_nome})":             "Sem match",
        }
        return campos, None  # salvar None

    # Interação com o usuário — serializada para não misturar prints
    with _INTERACAO_LOCK:
        escolhido = perguntar_usuario(nome, loja_nome, candidatos)

    if escolhido is None:
        campos = {
            f"Produto Encontrado ({loja_nome})": "",
            f"Preço ({loja_nome})":              "",
            f"Status ({loja_nome})":             "Não encontrado",
        }
        return campos, None

    res = processar_match(nome, escolhido, loja, dados)
    print(f"  [{loja_nome}] → {res['descricao']} | R$ {res['preco']} | {res['status']}")
    campos = {
        f"Produto Encontrado ({loja_nome})": res["descricao"],
        f"Preço ({loja_nome})":              res["preco"],
        f"Status ({loja_nome})":             res["status"],
    }
    return campos, _slim(escolhido)  # salvar apenas identidade


# =============================================================
# 🚀  EXECUÇÃO PRINCIPAL
# =============================================================

def main():
    import subprocess
    subprocess.run(["playwright", "install", "chromium"], check=False)

    print("=" * 55)
    print("  Busca de Preços — VipCommerce + Alabarce")
    print("=" * 55)

    # Lê CSV
    try:
        with open(ARQUIVO_ENTRADA, encoding="utf-8-sig") as f:
            produtos = [row["Produtos"].strip() for row in csv.DictReader(f) if row.get("Produtos")]
    except FileNotFoundError:
        print(f"\n❌ Arquivo '{ARQUIVO_ENTRADA}' não encontrado.")
        return
    except KeyError:
        print("\n❌ Coluna 'Produtos' não encontrada no CSV.")
        return

    print(f"\n📋 {len(produtos)} produto(s) no CSV.")
    print(f"🏪 Lojas configuradas: {', '.join(l['nome'] for l in LOJAS)}\n")

    escolhas = carregar_escolhas()

    # ------------------------------------------------------------------
    # Etapa 1 — Captura tokens das lojas VipCommerce
    # ------------------------------------------------------------------
    print("=" * 55)
    print("  Etapa 1/2 — Capturando tokens das lojas VipCommerce")
    print("=" * 55)

    dados_lojas = {}
    lojas_vc = [l for l in LOJAS if l.get("tipo", "vipcommerce") == "vipcommerce"]
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS_TOKENS, len(lojas_vc) or 1)) as ex:
        futuros = {ex.submit(capturar_dados_loja, loja): loja for loja in lojas_vc}
        for fut in as_completed(futuros):
            loja = futuros[fut]
            try:
                resultado = fut.result()
                if resultado.get("token"):
                    dados_lojas[loja["nome"]] = resultado
                else:
                    print(f"⚠️  {loja['nome']}: token não capturado — loja será ignorada nesta execução.")
            except Exception as exc:
                print(f"⚠️  {loja['nome']}: erro inesperado ({exc}) — loja será ignorada.")

    if not dados_lojas:
        print("❌ Nenhuma loja respondeu. Verifique a conexão e tente novamente.")
        return

    # ------------------------------------------------------------------
    # Inicia driver do Alabarce (reutilizado para todos os produtos)
    # ------------------------------------------------------------------
    lojas_alabarce = [l for l in LOJAS if l.get("tipo") == "alabarce"]
    driver_alabarce = None
    if lojas_alabarce:
        print("\n🌿 Iniciando navegador para Alabarce...")
        driver_alabarce = _criar_driver_alabarce()
        print("✅ Navegador Alabarce pronto.")

    print("\n" + "=" * 55)
    print("  Etapa 2/2 — Buscando preços")
    print("=" * 55 + "\n")

    resultados = []

    try:
        for i, nome in enumerate(produtos, 1):
            chave = normalizar(nome)
            print(f"\n[{i}/{len(produtos)}] {nome}")

            linha = {"Produto Buscado": nome}

            # ══════════════════════════════════════════════════
            # Lojas VipCommerce — processadas em paralelo
            # ══════════════════════════════════════════════════
            lojas_vc_ativas = [
                (l, dados_lojas[l["nome"]])
                for l in LOJAS
                if l.get("tipo", "vipcommerce") == "vipcommerce"
                and l["nome"] in dados_lojas
            ]
            novas_escolhas_vc: dict = {}

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

                for fut in as_completed(futuros):
                    loja_f     = futuros[fut]
                    loja_nome  = loja_f["nome"]
                    chave_loja = loja_nome.lower()
                    try:
                        campos, escolha_nova = fut.result()

                        if escolha_nova == "TOKEN_EXPIRED":
                            print(f"\n  ⚠️  Token expirado em {loja_nome}! Recapturando...")
                            dados_lojas[loja_nome] = capturar_dados_loja(loja_f)
                            campos, escolha_nova = _processar_loja_vc(
                                nome, loja_f, dados_lojas[loja_nome],
                                escolhas.setdefault(chave_loja, {}), chave,
                            )

                        if campos:
                            linha.update(campos)
                        if escolha_nova is not _INALTERADO:
                            novas_escolhas_vc[chave_loja] = escolha_nova

                    except Exception as exc:
                        print(f"  ❌ Erro em {loja_nome}: {exc}")

            # Salva novas escolhas VC de uma vez (evita gravações concorrentes)
            if novas_escolhas_vc:
                for cl, esc in novas_escolhas_vc.items():
                    escolhas[cl][chave] = esc
                salvar_escolhas(escolhas)

            # ══════════════════════════════════════════════════
            # Alabarce — mantido sequencial (driver único)
            # ══════════════════════════════════════════════════
            for loja in LOJAS:
                if loja.get("tipo") != "alabarce" or not driver_alabarce:
                    continue

                loja_nome  = loja["nome"]
                chave_loja = loja_nome.lower()
                page       = driver_alabarce.page
                escolhas.setdefault(chave_loja, {})

                if chave in escolhas[chave_loja]:
                    entrada = escolhas[chave_loja][chave]
                    if entrada is None:
                        print(f"  [{loja_nome}] → sem escolha salva")
                        linha[f"Produto Encontrado ({loja_nome})"] = ""
                        linha[f"Preço ({loja_nome})"]              = ""
                        linha[f"Status ({loja_nome})"]             = "Não encontrado"
                    else:
                        res = _processar_alabarce_salvo(entrada, nome, page)
                        print(f"  [{loja_nome}] → {res['descricao']} | R$ {res['preco']} | {res['status']}")
                        linha[f"Produto Encontrado ({loja_nome})"] = res["descricao"]
                        linha[f"Preço ({loja_nome})"]              = res["preco"]
                        linha[f"Status ({loja_nome})"]             = res["status"]
                    continue

                lista = buscar_produto_alabarce(nome, page)

                if not lista:
                    print(f"  [{loja_nome}] → Nenhum resultado.")
                    escolhas[chave_loja][chave] = None
                    salvar_escolhas(escolhas)
                    linha[f"Produto Encontrado ({loja_nome})"] = ""
                    linha[f"Preço ({loja_nome})"]              = ""
                    linha[f"Status ({loja_nome})"]             = "Não encontrado"
                    continue

                candidatos = encontrar_candidatos(nome, lista)

                if not candidatos:
                    print(f"  [{loja_nome}] → Nenhum candidato válido.")
                    escolhas[chave_loja][chave] = None
                    salvar_escolhas(escolhas)
                    linha[f"Produto Encontrado ({loja_nome})"] = ""
                    linha[f"Preço ({loja_nome})"]              = ""
                    linha[f"Status ({loja_nome})"]             = "Sem match"
                    continue

                escolhido = perguntar_usuario(nome, loja_nome, candidatos)
                escolhas[chave_loja][chave] = _slim(escolhido)
                salvar_escolhas(escolhas)

                if escolhido is None:
                    linha[f"Produto Encontrado ({loja_nome})"] = ""
                    linha[f"Preço ({loja_nome})"]              = ""
                    linha[f"Status ({loja_nome})"]             = "Não encontrado"
                else:
                    preco  = escolhido.get("preco", "")
                    status = "OK" if preco else "Indisponível"
                    print(f"  [{loja_nome}] → {escolhido['descricao']} | R$ {preco} | {status}")
                    linha[f"Produto Encontrado ({loja_nome})"] = escolhido["descricao"]
                    linha[f"Preço ({loja_nome})"]              = preco
                    linha[f"Status ({loja_nome})"]             = status

            time.sleep(DELAY_ENTRE_BUSCAS)  # delay único por produto, não por loja
            resultados.append(linha)

    finally:
        if driver_alabarce:
            driver_alabarce.quit()
            print("\n🌿 Navegador Alabarce encerrado.")

    # ------------------------------------------------------------------
    # Salva CSV com cabeçalho dinâmico
    # ------------------------------------------------------------------
    campos = ["Produto Buscado"]
    for loja in LOJAS:
        campos.append(f"Produto Encontrado ({loja['nome']})")
    for loja in LOJAS:
        campos.append(f"Preço ({loja['nome']})")
        campos.append(f"Status ({loja['nome']})")

    from datetime import date
    import shutil

    hoje = date.today()
    nome_arquivo = f"preços_{hoje.day:02d}_{hoje.month:02d}.csv"
    pasta_resultados = r"C:\Users\pesso\OneDrive\Documentos\benverde\MeuAppGerencia\dados\precos"
    caminho_final = os.path.join(pasta_resultados, nome_arquivo)

    try:
        with open(nome_arquivo, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(resultados)
            writer.writerow({})
            writer.writerow({"Produto Buscado": f"Busca gerada em: {hoje.strftime('%d/%m/%Y')}"})

        os.makedirs(pasta_resultados, exist_ok=True)
        shutil.move(nome_arquivo, caminho_final)
        print(f"\n📄 Resultado salvo em: {caminho_final}")

    except PermissionError:
        alt = nome_arquivo.replace(".csv", "_novo.csv")
        print(f"\n⚠️  Arquivo em uso. Salvando como '{alt}'...")
        with open(alt, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(resultados)
            writer.writerow({})
            writer.writerow({"Produto Buscado": f"Busca gerada em: {hoje.strftime('%d/%m/%Y')}"})
        shutil.move(alt, os.path.join(pasta_resultados, alt))
        print(f"   ✅ Salvo em: {os.path.join(pasta_resultados, alt)}")

    ok = sum(
        1 for r in resultados
        if any(r.get(f"Status ({l['nome']})") == "OK" for l in LOJAS)
    )
    print(f"\n✅ Concluído! {ok}/{len(produtos)} produto(s) com ao menos um preço.")
    print(f"💾 Escolhas salvas em: {ARQUIVO_ESCOLHAS}")


if __name__ == "__main__":
    main()
