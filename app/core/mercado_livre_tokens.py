from __future__ import annotations

import json
import os
import re
from pathlib import Path

import httpx

from app.core.settings import get_settings


def _require(value: str | None, name: str) -> str:
    if not value:
        raise RuntimeError(f"{name} nao configurado.")
    return value


def _env_file_path() -> Path:
    return Path(__file__).resolve().parents[2] / ".env"


def _set_env_value(env_text: str, key: str, value: str) -> str:
    pattern = rf"(?m)^{re.escape(key)}=.*$"
    replacement = f"{key}={value}"
    if re.search(pattern, env_text):
        return re.sub(pattern, replacement, env_text, count=1)
    suffix = "" if not env_text or env_text.endswith("\n") else "\n"
    return f"{env_text}{suffix}{replacement}\n"


def _persist_tokens(*, access_token: str, refresh_token: str | None) -> None:
    env_path = _env_file_path()
    env_text = env_path.read_text(encoding="utf-8") if env_path.exists() else ""
    env_text = _set_env_value(env_text, "ML_ACCESS_TOKEN", access_token)
    if refresh_token:
        env_text = _set_env_value(env_text, "ML_REFRESH_TOKEN", refresh_token)
        os.environ["ML_REFRESH_TOKEN"] = refresh_token
    env_path.write_text(env_text, encoding="utf-8")
    os.environ["ML_ACCESS_TOKEN"] = access_token
    get_settings.cache_clear()


def refresh_mercado_livre_tokens(refresh_token: str | None = None) -> dict[str, object]:
    settings = get_settings()
    payload = {
        "grant_type": "refresh_token",
        "client_id": _require(settings.ml_client_id, "ML_CLIENT_ID"),
        "client_secret": _require(settings.ml_client_secret, "ML_CLIENT_SECRET"),
        "refresh_token": _require(refresh_token or settings.ml_refresh_token, "ML_REFRESH_TOKEN"),
    }
    response = httpx.post(
        f"{settings.ml_api_base.rstrip('/')}/oauth/token",
        headers={
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        },
        data=payload,
        timeout=30,
    )
    try:
        data = response.json()
    except ValueError:
        data = {"raw": response.text}

    if response.status_code >= 400:
        raise RuntimeError(
            f"Mercado Livre token refresh failed with status {response.status_code}: "
            f"{json.dumps(data, ensure_ascii=False)}"
        )
    if not isinstance(data, dict) or not data.get("access_token"):
        raise RuntimeError("Mercado Livre token refresh returned an invalid payload.")

    # 2026-05-02: persist refreshed tokens so the next Bronze cycle does not fail
    # again with the same expired credential after a local API restart.
    _persist_tokens(
        access_token=str(data["access_token"]),
        refresh_token=str(data["refresh_token"]) if data.get("refresh_token") else None,
    )
    return data
