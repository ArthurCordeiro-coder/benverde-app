"""auth.py
Sistema de autenticação para o app Benverde.
Usa apenas stdlib: hashlib, secrets, json, threading, datetime, re, os.
"""

import hashlib
import json
import os
import re
import secrets
import threading
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_LOCK = threading.Lock()
_BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
_USERS_PATH    = os.path.join(_BASE_DIR, "users.json")
_PENDING_PATH  = os.path.join(_BASE_DIR, "pending.json")
_LOCKOUTS_PATH = os.path.join(_BASE_DIR, "lockouts.json")

# ---------------------------------------------------------------------------
# Internos
# ---------------------------------------------------------------------------

def _hash_senha(salt: str, senha: str) -> str:
    return hashlib.sha256((salt + senha).encode()).hexdigest()


def _load_json(path: str, default):
    """Lê um JSON; retorna `default` se o arquivo não existir ou estiver corrompido."""
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default


def _save_json(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Leitura / escrita pública
# ---------------------------------------------------------------------------

def carregar_users() -> list[dict]:
    with _LOCK:
        return _load_json(_USERS_PATH, [])


def salvar_users(users: list[dict]) -> None:
    with _LOCK:
        _save_json(_USERS_PATH, users)


def carregar_pending() -> list[dict]:
    with _LOCK:
        return _load_json(_PENDING_PATH, [])


def salvar_pending(pending: list[dict]) -> None:
    with _LOCK:
        _save_json(_PENDING_PATH, pending)


def carregar_lockouts() -> dict:
    with _LOCK:
        return _load_json(_LOCKOUTS_PATH, {})


def salvar_lockouts(lockouts: dict) -> None:
    with _LOCK:
        _save_json(_LOCKOUTS_PATH, lockouts)


def get_user(username: str) -> dict | None:
    """Busca um usuário aprovado pelo username."""
    return next((u for u in carregar_users() if u["username"] == username), None)


# ---------------------------------------------------------------------------
# Lógica de autenticação
# ---------------------------------------------------------------------------

def verificar_login(username: str, senha: str) -> tuple[bool, str]:
    """
    Valida credenciais com proteção contra brute-force.
    Retorna (True, "ok") ou (False, "motivo").
    """
    with _LOCK:
        lockouts = _load_json(_LOCKOUTS_PATH, {})
        agora    = datetime.now(timezone.utc)

        # 1. Verifica bloqueio ativo
        entry = lockouts.get(username, {})
        bloqueado_ate_str = entry.get("bloqueado_ate")
        if bloqueado_ate_str:
            bloqueado_ate = datetime.fromisoformat(bloqueado_ate_str)
            if agora < bloqueado_ate:
                hora = bloqueado_ate.astimezone().strftime("%H:%M")
                return False, f"Usuário bloqueado até {hora}"

        # 2. Busca usuário
        users = _load_json(_USERS_PATH, [])
        user  = next((u for u in users if u["username"] == username), None)

        def _registrar_tentativa() -> tuple[bool, str]:
            e = lockouts.setdefault(username, {"tentativas": 0, "bloqueado_ate": None})
            e["tentativas"] = e.get("tentativas", 0) + 1
            n = e["tentativas"]
            if n >= 5:
                e["bloqueado_ate"] = (agora + timedelta(minutes=15)).isoformat()
                e["tentativas"]    = 0
                _save_json(_LOCKOUTS_PATH, lockouts)
                return False, "Muitas tentativas. Usuário bloqueado por 15 minutos."
            _save_json(_LOCKOUTS_PATH, lockouts)
            return False, f"Usuário ou senha inválidos ({n} de 5)"

        # 3. Usuário não encontrado
        if user is None:
            return _registrar_tentativa()

        # 4. Senha incorreta
        if _hash_senha(user["salt"], senha) != user["senha_hash"]:
            return _registrar_tentativa()

        # 5. Sucesso — zera lockout
        lockouts[username] = {"tentativas": 0, "bloqueado_ate": None}
        _save_json(_LOCKOUTS_PATH, lockouts)
        return True, "ok"


def registrar_usuario(username: str, nome: str, senha: str) -> tuple[bool, str]:
    """
    Registra um novo usuário.
    Retorna (True, "admin_criado") | (True, "pendente") | (False, "motivo").
    """
    if not re.fullmatch(r"[a-zA-Z0-9_]{3,20}", username):
        return False, "Username deve ter 3–20 caracteres (letras, números e _)"
    if len(senha) < 6:
        return False, "Senha deve ter pelo menos 6 caracteres"

    with _LOCK:
        users   = _load_json(_USERS_PATH, [])
        pending = _load_json(_PENDING_PATH, [])

        if any(u["username"] == username for u in users):
            return False, "Username já cadastrado"
        if any(p["username"] == username for p in pending):
            return False, "Username já aguarda aprovação"

        salt      = secrets.token_hex(32)
        hash_     = _hash_senha(salt, senha)
        agora_iso = datetime.now(timezone.utc).isoformat()

        # Primeiro usuário → admin direto (sem aprovação)
        if not users:
            users.append({
                "username":   username,
                "nome":       nome,
                "salt":       salt,
                "senha_hash": hash_,
                "is_admin":   True,
                "criado_em":  agora_iso,
            })
            _save_json(_USERS_PATH, users)
            return True, "admin_criado"

        # Demais → fila de aprovação
        pending.append({
            "username":      username,
            "nome":          nome,
            "salt":          salt,
            "senha_hash":    hash_,
            "solicitado_em": agora_iso,
        })
        _save_json(_PENDING_PATH, pending)
        return True, "pendente"


def aprovar_usuario(username: str) -> bool:
    """Move o usuário de pending.json para users.json como is_admin=False."""
    with _LOCK:
        pending = _load_json(_PENDING_PATH, [])
        entry   = next((p for p in pending if p["username"] == username), None)
        if entry is None:
            return False
        users = _load_json(_USERS_PATH, [])
        users.append({
            "username":   entry["username"],
            "nome":       entry["nome"],
            "salt":       entry["salt"],
            "senha_hash": entry["senha_hash"],
            "is_admin":   False,
            "criado_em":  datetime.now(timezone.utc).isoformat(),
        })
        _save_json(_USERS_PATH, users)
        _save_json(_PENDING_PATH, [p for p in pending if p["username"] != username])
        return True


def rejeitar_usuario(username: str) -> bool:
    """Remove o usuário de pending.json."""
    with _LOCK:
        pending = _load_json(_PENDING_PATH, [])
        nova    = [p for p in pending if p["username"] != username]
        if len(nova) == len(pending):
            return False
        _save_json(_PENDING_PATH, nova)
        return True
