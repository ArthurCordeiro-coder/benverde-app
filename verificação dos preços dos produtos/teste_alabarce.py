"""
Diagnóstico do Alabarce — roda com browser visível para inspecionar o HTML.
Execute: python teste_alabarce.py
"""
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

TERMO = "Alface"

options = webdriver.ChromeOptions()
# ─── NÃO headless: abre janela visível ───
options.add_argument("--window-size=1280,900")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options,
)
driver.set_page_load_timeout(30)

url = f"https://alabarce.net.br/products?utf8=%E2%9C%93&keywords={requests.utils.quote(TERMO)}"
print(f"\n🌐 Abrindo: {url}")
driver.get(url)
time.sleep(4)

print(f"📄 Título da página: {driver.title}")
print(f"🔗 URL atual:        {driver.current_url}")

# ── Salva screenshot ──────────────────────────────────────────
driver.save_screenshot("alabarce_debug.png")
print("\n📸 Screenshot salvo em: alabarce_debug.png")

# ── Testa seletor atual ───────────────────────────────────────
els = driver.find_elements(By.CSS_SELECTOR, "div.media.product")
print(f"\n[Seletor atual] div.media.product → {len(els)} elemento(s)")

# ── Testa seletores alternativos ──────────────────────────────
seletores = [
    "li.product",
    "div.product",
    "article.product",
    "[class*='product-item']",
    "[class*='product_item']",
    "[class*='product-card']",
    ".products-grid li",
    ".product-list li",
    "div[class*='product']",
    "li[class*='product']",
]
print("\n🔍 Testando seletores alternativos:")
for sel in seletores:
    n = len(driver.find_elements(By.CSS_SELECTOR, sel))
    if n:
        print(f"  ✅ '{sel}' → {n} elemento(s)")
        # Mostra o texto do primeiro
        try:
            texto = driver.find_elements(By.CSS_SELECTOR, sel)[0].text[:120]
            print(f"     Primeiro: {texto!r}")
        except Exception:
            pass
    else:
        print(f"  ❌ '{sel}' → 0")

# ── Salva o HTML da página ────────────────────────────────────
with open("alabarce_debug.html", "w", encoding="utf-8") as f:
    f.write(driver.page_source)
print("\n💾 HTML salvo em: alabarce_debug.html")

print("\n⏸  Janela aberta por 30s para inspeção manual...")
time.sleep(30)
driver.quit()
print("✅ Diagnóstico concluído.")
