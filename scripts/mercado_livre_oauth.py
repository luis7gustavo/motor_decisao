from __future__ import annotations

import argparse
import base64
import hashlib
import json
import secrets
import sys
from pathlib import Path
from urllib.parse import urlencode

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.settings import get_settings  # noqa: E402


def _require(value: str | None, name: str) -> str:
    if not value:
        raise SystemExit(f"{name} nao configurado. Preencha no .env antes de continuar.")
    return value


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


def _authorization_url(*, state: str | None, use_pkce: bool) -> dict[str, str]:
    settings = get_settings()
    client_id = _require(settings.ml_client_id, "ML_CLIENT_ID")
    redirect_uri = _require(settings.ml_redirect_uri, "ML_REDIRECT_URI")

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
    }
    result = {"redirect_uri": redirect_uri}

    if state:
        params["state"] = state
        result["state"] = state

    if use_pkce:
        verifier, challenge = _pkce_pair()
        params["code_challenge"] = challenge
        params["code_challenge_method"] = "S256"
        result["code_verifier"] = verifier

    result["authorization_url"] = (
        f"{settings.ml_auth_base.rstrip('/')}/authorization?{urlencode(params)}"
    )
    return result


def _token_request(payload: dict[str, str]) -> dict[str, object]:
    settings = get_settings()
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
        raise SystemExit(
            json.dumps(
                {
                    "status_code": response.status_code,
                    "error": data,
                    "hint": (
                        "Confira se o redirect_uri e exatamente o mesmo cadastrado "
                        "no app do Mercado Livre e usado na URL de autorizacao."
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    if not isinstance(data, dict):
        raise SystemExit("Resposta inesperada do Mercado Livre.")
    return data


def exchange_code(code: str, code_verifier: str | None = None) -> dict[str, object]:
    settings = get_settings()
    payload = {
        "grant_type": "authorization_code",
        "client_id": _require(settings.ml_client_id, "ML_CLIENT_ID"),
        "client_secret": _require(settings.ml_client_secret, "ML_CLIENT_SECRET"),
        "code": code,
        "redirect_uri": _require(settings.ml_redirect_uri, "ML_REDIRECT_URI"),
    }
    if code_verifier:
        payload["code_verifier"] = code_verifier
    return _token_request(payload)


def refresh_token(refresh_token: str | None = None) -> dict[str, object]:
    settings = get_settings()
    payload = {
        "grant_type": "refresh_token",
        "client_id": _require(settings.ml_client_id, "ML_CLIENT_ID"),
        "client_secret": _require(settings.ml_client_secret, "ML_CLIENT_SECRET"),
        "refresh_token": _require(refresh_token or settings.ml_refresh_token, "ML_REFRESH_TOKEN"),
    }
    return _token_request(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OAuth Mercado Livre para ambiente local.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    auth_parser = subparsers.add_parser("auth-url", help="Gera a URL de autorizacao.")
    auth_parser.add_argument("--state", default="local-dev", help="Valor opcional para validar retorno.")
    auth_parser.add_argument(
        "--pkce",
        action="store_true",
        help="Gera code_challenge/code_verifier para apps com PKCE habilitado.",
    )

    exchange_parser = subparsers.add_parser("exchange", help="Troca o code por tokens.")
    exchange_parser.add_argument("--code", required=True, help="Code recebido no callback.")
    exchange_parser.add_argument(
        "--code-verifier",
        default=None,
        help="Obrigatorio se a URL de autorizacao foi gerada com --pkce.",
    )

    refresh_parser = subparsers.add_parser("refresh", help="Renova access token com refresh token.")
    refresh_parser.add_argument("--refresh-token", default=None, help="Opcional; padrao vem de ML_REFRESH_TOKEN.")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "auth-url":
        data = _authorization_url(state=args.state, use_pkce=args.pkce)
    elif args.command == "exchange":
        data = exchange_code(args.code, code_verifier=args.code_verifier)
    elif args.command == "refresh":
        data = refresh_token(args.refresh_token)
    else:
        raise SystemExit(f"Comando desconhecido: {args.command}")

    print(json.dumps(data, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
