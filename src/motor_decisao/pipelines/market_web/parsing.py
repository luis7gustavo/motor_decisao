from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def parse_brl_price(value: Any) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"R\$\s*([0-9.]+,\d{2}|[0-9]+(?:[.,]\d{2})?)", text)
    if not match:
        return None
    number = match.group(1).replace(".", "").replace(",", ".")
    try:
        return float(Decimal(number))
    except (InvalidOperation, ValueError):
        return None


def parse_int_text(value: Any) -> int | None:
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*(mil|k)?", text.lower())
    if not match:
        return None
    try:
        multiplier = 1000 if match.group(2) in {"mil", "k"} else 1
        return int(_parse_localized_number(match.group(1)) * multiplier)
    except ValueError:
        return None


def parse_sold_quantity(value: Any) -> int | None:
    text = clean_text(value)
    if not text:
        return None
    lowered = text.lower()
    if "vend" not in lowered and "sold" not in lowered and "compras" not in lowered:
        return None
    match = re.search(r"(\d[\d.,]*)\s*(mil|k)?\+?\s*vendid", lowered)
    if not match:
        match = re.search(r"(?:mais\s+de\s+)?(\d[\d.,]*)\s*(mil|k)?\+?\s+compras?", lowered)
    if not match:
        return None
    multiplier = 1000 if match.group(2) in {"mil", "k"} else 1
    return int(_parse_localized_number(match.group(1)) * multiplier)


def first_price_from_text(value: Any) -> float | None:
    return parse_brl_price(value)


def _parse_localized_number(value: str) -> float:
    number = value.strip()
    if "," in number and "." in number:
        number = number.replace(".", "").replace(",", ".")
    elif "." in number:
        groups = number.split(".")
        if len(groups) > 1 and all(len(group) == 3 for group in groups[1:]):
            number = number.replace(".", "")
    elif "," in number:
        number = number.replace(",", ".")
    return float(number)


def detect_block(text: str) -> str | None:
    lowered = text.lower()
    signals = {
        "captcha": [
            "captcha",
            "captcha interception",
            "digite os caracteres",
            "verifique que voce",
            "verify you are",
            "deslize para verificar",
            "unusual traffic",
        ],
        "bot_block": ["robo", "robot", "automated access", "acesso automatizado"],
        "access_denied": [
            "access denied",
            "acesso negado",
            "not authorized",
            "unauthorized",
            "nao e possivel acessar",
            "não é possível acessar",
            "erro 403",
            "403 forbidden",
            "request blocked",
        ],
        "login_wall": ["faca login", "entre para continuar", "sign in to continue"],
    }
    for reason, patterns in signals.items():
        if any(pattern in lowered for pattern in patterns):
            return reason
    return None
