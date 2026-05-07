from __future__ import annotations

from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query

from app.core.mercado_livre_tokens import refresh_mercado_livre_tokens
from app.core.settings import get_settings

router = APIRouter(prefix="/integracoes/mercado-livre", tags=["mercado livre"])


def build_authorization_url(*, state: str | None = None) -> str:
    settings = get_settings()
    if not settings.ml_client_id:
        raise HTTPException(status_code=500, detail="ML_CLIENT_ID nao configurado.")
    if not settings.ml_redirect_uri:
        raise HTTPException(status_code=500, detail="ML_REDIRECT_URI nao configurado.")

    params = {
        "response_type": "code",
        "client_id": settings.ml_client_id,
        "redirect_uri": settings.ml_redirect_uri,
    }
    if state:
        params["state"] = state

    return f"{settings.ml_auth_base.rstrip('/')}/authorization?{urlencode(params)}"


@router.get("/auth-url")
def auth_url(state: str | None = None) -> dict[str, str | None]:
    settings = get_settings()
    return {
        "authorization_url": build_authorization_url(state=state),
        "redirect_uri": settings.ml_redirect_uri,
        "state": state,
    }


@router.get("/callback")
def callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
) -> dict[str, str | None]:
    if error:
        return {
            "status": "error",
            "error": error,
            "error_description": error_description,
            "code": None,
            "state": state,
            "next_step": None,
        }
    if not code:
        raise HTTPException(status_code=400, detail="Callback sem parametro code.")

    return {
        "status": "received_code",
        "code": code,
        "state": state,
        "next_step": (
            "Rode: docker compose run --rm api python scripts/mercado_livre_oauth.py "
            f'exchange --code "{code}"'
        ),
    }


@router.post("/refresh-token")
def refresh_token() -> dict[str, str | int | None]:
    # 2026-05-02: expose a local refresh hook so Bronze ops can validate and
    # repair Mercado Livre credentials without waiting for the full cycle order.
    data = refresh_mercado_livre_tokens()
    return {
        "status": "refreshed",
        "expires_in": int(data["expires_in"]) if data.get("expires_in") is not None else None,
        "user_id": str(data["user_id"]) if data.get("user_id") is not None else None,
    }
