from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass

from fastapi import Request
from starlette.responses import Response

from app.core.settings import get_settings


CSRF_COOKIE_NAME = "sillo_review_csrf"
CSRF_MAX_AGE_SECONDS = 12 * 60 * 60


@dataclass(frozen=True)
class CsrfContext:
    token: str
    cookie_value: str
    set_cookie: bool


def _signature(cookie_value: str, timestamp: str) -> str:
    secret = get_settings().review_csrf_secret.encode("utf-8")
    payload = f"{cookie_value}:{timestamp}".encode("utf-8")
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def csrf_context(request: Request) -> CsrfContext:
    cookie_value = request.cookies.get(CSRF_COOKIE_NAME)
    set_cookie = not cookie_value or not (20 <= len(cookie_value) <= 200)
    if set_cookie:
        cookie_value = secrets.token_urlsafe(32)
    timestamp = str(int(time.time()))
    token = f"{timestamp}.{_signature(cookie_value, timestamp)}"
    return CsrfContext(token=token, cookie_value=cookie_value, set_cookie=set_cookie)


def set_csrf_cookie(response: Response, context: CsrfContext) -> None:
    if not context.set_cookie:
        return
    response.set_cookie(
        CSRF_COOKIE_NAME,
        context.cookie_value,
        max_age=CSRF_MAX_AGE_SECONDS,
        httponly=True,
        samesite="strict",
        secure=False,
        path="/review",
    )


def verify_csrf(request: Request, token: str | None) -> bool:
    cookie_value = request.cookies.get(CSRF_COOKIE_NAME)
    if not cookie_value or not token:
        return False
    try:
        timestamp, provided_signature = token.split(".", 1)
        issued_at = int(timestamp)
    except (TypeError, ValueError):
        return False
    now = int(time.time())
    if issued_at > now + 60 or now - issued_at > CSRF_MAX_AGE_SECONDS:
        return False
    expected_signature = _signature(cookie_value, timestamp)
    return hmac.compare_digest(provided_signature, expected_signature)
