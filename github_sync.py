"""github_sync.py
Camada de sincronizacao: envia arquivos JSON ao repositorio GitHub
via Contents API para que os dados sobrevivam ao reinicio do Streamlit Cloud.

Configuracao (st.secrets ou variaveis de ambiente):
    [github_sync]
    token  = "ghp_..."
    repo   = "owner/repo"
    branch = "main"
"""

import base64
import logging
import os
import threading
import time

import requests

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_SHA_CACHE: dict[str, str] = {}
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_MIN_INTERVAL = 0.5  # segundos entre chamadas para evitar rajadas
_last_call: float = 0.0

_API = "https://api.github.com"


# ---------------------------------------------------------------------------
# Configuracao
# ---------------------------------------------------------------------------

def _get_config() -> tuple[str, str, str]:
    """Retorna (token, repo, branch). Levanta RuntimeError se ausente."""
    try:
        import streamlit as st
        sec = st.secrets["github_sync"]
        return sec["token"], sec["repo"], sec.get("branch", "main")
    except Exception:
        pass
    token = os.environ.get("GITHUB_SYNC_TOKEN", "")
    repo = os.environ.get("GITHUB_SYNC_REPO", "")
    branch = os.environ.get("GITHUB_SYNC_BRANCH", "main")
    if token and repo:
        return token, repo, branch
    raise RuntimeError("github_sync nao configurado")


def is_enabled() -> bool:
    """True se o token esta disponivel."""
    try:
        _get_config()
        return True
    except RuntimeError:
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _repo_path(local_path: str) -> str:
    """Converte caminho absoluto local em caminho relativo do repo (barras /)."""
    rel = os.path.relpath(os.path.abspath(local_path), _REPO_ROOT)
    return rel.replace("\\", "/")


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _fetch_sha(token: str, repo: str, branch: str, rpath: str) -> str | None:
    """Busca o SHA atual de um arquivo no repo. Retorna None se nao existir."""
    url = f"{_API}/repos/{repo}/contents/{rpath}"
    resp = requests.get(url, headers=_headers(token), params={"ref": branch}, timeout=15)
    if resp.status_code == 200:
        return resp.json().get("sha")
    return None


# ---------------------------------------------------------------------------
# API publica
# ---------------------------------------------------------------------------

def push_file(local_path: str, commit_message: str = "") -> bool:
    """Le local_path e envia ao GitHub. Retorna True em caso de sucesso."""
    if not is_enabled():
        return True  # no-op silencioso

    try:
        with open(local_path, "rb") as f:
            content = f.read()
    except OSError as exc:
        logger.warning("github_sync: nao conseguiu ler '%s': %s", local_path, exc)
        return False

    rpath = _repo_path(local_path)
    if not commit_message:
        commit_message = f"sync: {rpath}"

    b64 = base64.b64encode(content).decode()

    try:
        token, repo, branch = _get_config()
    except RuntimeError:
        return False

    url = f"{_API}/repos/{repo}/contents/{rpath}"

    with _LOCK:
        # Throttle: respeitar intervalo minimo entre chamadas
        global _last_call
        elapsed = time.monotonic() - _last_call
        if elapsed < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - elapsed)

        sha = _SHA_CACHE.get(rpath) or _fetch_sha(token, repo, branch, rpath)

        payload: dict = {
            "message": commit_message,
            "content": b64,
            "branch": branch,
        }
        if sha:
            payload["sha"] = sha

        resp = requests.put(url, json=payload, headers=_headers(token), timeout=30)

        # Conflito de SHA: refetch e tenta de novo
        if resp.status_code == 409:
            sha = _fetch_sha(token, repo, branch, rpath)
            if sha:
                payload["sha"] = sha
            else:
                payload.pop("sha", None)
            resp = requests.put(url, json=payload, headers=_headers(token), timeout=30)

        _last_call = time.monotonic()

        # Rate limit atingido
        if resp.status_code in (429, 403):
            logger.warning(
                "github_sync: rate limit atingido ao enviar '%s' — HTTP %s",
                rpath, resp.status_code,
            )
            return False

        if resp.status_code in (200, 201):
            _SHA_CACHE[rpath] = resp.json()["content"]["sha"]
            logger.info("github_sync: '%s' enviado com sucesso.", rpath)
            return True

        logger.warning(
            "github_sync: falha ao enviar '%s' — HTTP %s: %s",
            rpath, resp.status_code, resp.text[:200],
        )
        return False


def delete_file(local_path: str, commit_message: str = "") -> bool:
    """Remove um arquivo do repositorio GitHub."""
    if not is_enabled():
        return True

    rpath = _repo_path(local_path)
    if not commit_message:
        commit_message = f"sync: remove {rpath}"

    try:
        token, repo, branch = _get_config()
    except RuntimeError:
        return False

    url = f"{_API}/repos/{repo}/contents/{rpath}"

    with _LOCK:
        sha = _SHA_CACHE.pop(rpath, None) or _fetch_sha(token, repo, branch, rpath)
        if not sha:
            return True  # arquivo ja nao existe no repo

        payload = {
            "message": commit_message,
            "sha": sha,
            "branch": branch,
        }
        resp = requests.delete(url, json=payload, headers=_headers(token), timeout=30)

        if resp.status_code == 200:
            logger.info("github_sync: '%s' removido do repo.", rpath)
            return True

        logger.warning(
            "github_sync: falha ao remover '%s' — HTTP %s: %s",
            rpath, resp.status_code, resp.text[:200],
        )
        return False

    return False
