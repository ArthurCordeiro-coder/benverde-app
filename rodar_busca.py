"""
Executor de Busca de Precos
============================
Roda o buscar_precos.py local (Selenium) e publica o CSV no GitHub.

Uso:
  python rodar_busca.py          (busca interativa)
  python rodar_busca.py --auto   (auto-seleciona, sem perguntas)

Fluxo:
  1. Executa buscar_precos.py do OneDrive (Selenium, funciona no Windows)
  2. Copia o CSV gerado para a pasta do repositorio (benverde-app)
  3. git add + commit + push automatico
"""

import os
import sys
import shutil
import subprocess
from datetime import date
from pathlib import Path

# ── Caminhos ────────────────────────────────────────────────
ONEDRIVE_BUSCA = Path(
    r"C:\Users\pesso\OneDrive\Documentos\benverde\MeuAppGerencia"
    r"\verificação dos preços dos produtos"
)
ONEDRIVE_DADOS = Path(
    r"C:\Users\pesso\OneDrive\Documentos\benverde\MeuAppGerencia"
    r"\dados\precos"
)
REPO_DIR = Path(r"C:\Users\pesso\benverde-app")
REPO_DADOS = REPO_DIR / "verificação dos preços dos produtos" / "dados" / "precos"


def rodar_busca():
    """Executa o buscar_precos.py do OneDrive."""
    script = ONEDRIVE_BUSCA / "buscar_precos.py"
    if not script.exists():
        print(f"ERRO: Script nao encontrado: {script}")
        return False

    print("=" * 60)
    print("  BUSCA DE PRECOS — executando localmente")
    print("=" * 60)

    resultado = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(ONEDRIVE_BUSCA),
    )
    return resultado.returncode == 0


def encontrar_csv_mais_recente():
    """Encontra o CSV de precos mais recente na pasta do OneDrive."""
    if not ONEDRIVE_DADOS.exists():
        return None

    csvs = sorted(ONEDRIVE_DADOS.glob("preços_*.csv"), key=os.path.getmtime)
    if not csvs:
        return None

    return csvs[-1]


def copiar_e_publicar(csv_origem: Path):
    """Copia o CSV para o repo e faz git push."""
    nome = csv_origem.name

    # Copia para o repo
    REPO_DADOS.mkdir(parents=True, exist_ok=True)
    destino = REPO_DADOS / nome
    shutil.copy2(str(csv_origem), str(destino))
    print(f"\nCSV copiado para o repositorio: {destino}")

    # Git add + commit + push
    caminho_relativo = str(destino.relative_to(REPO_DIR))

    print("Publicando no GitHub...")
    subprocess.run(["git", "add", caminho_relativo], cwd=str(REPO_DIR), check=True)

    hoje = date.today().strftime("%d/%m/%Y")
    msg = f"precos: atualização {hoje} ({nome})"
    resultado_commit = subprocess.run(
        ["git", "commit", "-m", msg],
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
    )

    if resultado_commit.returncode != 0:
        if "nothing to commit" in resultado_commit.stdout:
            print("Nenhuma alteracao (CSV identico ao anterior).")
            return True
        print(f"Erro no commit: {resultado_commit.stderr}")
        return False

    resultado_push = subprocess.run(
        ["git", "push"],
        cwd=str(REPO_DIR),
    )

    if resultado_push.returncode == 0:
        print(f"Publicado no GitHub com sucesso!")
        return True
    else:
        print("Erro ao fazer push. Verifique sua conexao.")
        return False


def main():
    # 1. Roda a busca
    sucesso = rodar_busca()
    if not sucesso:
        print("\nA busca falhou ou foi interrompida.")
        input("Pressione Enter para sair...")
        return

    # 2. Encontra o CSV gerado
    csv_hoje = encontrar_csv_mais_recente()
    if not csv_hoje:
        print("\nNenhum CSV encontrado na pasta de resultados.")
        input("Pressione Enter para sair...")
        return

    print(f"\nCSV encontrado: {csv_hoje.name}")

    # 3. Copia e publica
    copiar_e_publicar(csv_hoje)

    print("\nConcluido!")
    input("Pressione Enter para sair...")


if __name__ == "__main__":
    main()
