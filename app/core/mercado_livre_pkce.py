from __future__ import annotations

import base64
import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


PKCE_STORE_PATH = Path(__file__).resolve().parents[2] / "data" / "mercado_livre_pkce.json"
PKCE_TTL = timedelta(minutes=20)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _read_store() -> dict[str, dict[str, str]]:
    if not PKCE_STORE_PATH.exists():
        return {}
    try:
        data = json.loads(PKCE_STORE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_store(data: dict[str, dict[str, str]]) -> None:
    PKCE_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PKCE_STORE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _prune_expired(data: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    minimum_created_at = _now() - PKCE_TTL
    active: dict[str, dict[str, str]] = {}
    for state, record in data.items():
        try:
            created_at = datetime.fromisoformat(record["created_at"])
        except (KeyError, TypeError, ValueError):
            continue
        if created_at >= minimum_created_at:
            active[state] = record
    return active


def create_pkce_authorization() -> dict[str, str]:
    state = secrets.token_urlsafe(24)
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    store = _prune_expired(_read_store())
    store[state] = {
        "code_verifier": verifier,
        "created_at": _now().isoformat(),
    }
    _write_store(store)
    return {
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }


def consume_code_verifier(state: str | None) -> str:
    if not state:
        raise RuntimeError("Callback OAuth sem state para validar PKCE.")
    store = _prune_expired(_read_store())
    record: dict[str, Any] | None = store.pop(state, None)
    _write_store(store)
    if not record or not record.get("code_verifier"):
        raise RuntimeError("PKCE state ausente ou expirado. Gere uma nova URL de autorizacao.")
    return str(record["code_verifier"])
