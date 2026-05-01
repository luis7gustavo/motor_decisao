from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def to_json_text(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def payload_hash(value: Any) -> str:
    canonical = to_json_text(value)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

