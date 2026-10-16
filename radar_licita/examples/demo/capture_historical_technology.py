"""Capture the identified PNCP technology chains as an offline presentation snapshot.

This script only reads public PNCP endpoints and writes a new fixture. It never
modifies the business database, and refuses to overwrite an existing capture.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from app.technology.historical import FIXTURE, capture_historical_technology  # noqa: E402


if __name__ == "__main__":
    if FIXTURE.exists():
        raise SystemExit(f"Arquivo existente preservado: {FIXTURE}")
    data = asyncio.run(capture_historical_technology())
    FIXTURE.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"fixture": str(FIXTURE), "requests": data["request_count"],
                      "purchases": len(data["records"]),
                      "items": sum(len(row["items"]) for row in data["records"])}, ensure_ascii=False))
