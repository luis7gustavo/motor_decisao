"""Synthetic, read-only discovery catalog for the isolated Radar Licita preview.

This module has no PNCP client or database dependency. It must never be used as
evidence that a real procurement is open or suitable for participation.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo" / "opportunities.json"
PAGE_SIZE = 10


@dataclass(frozen=True)
class Catalog:
    observed_at: datetime
    records: tuple[dict, ...]
    source_available: bool = True


def load_catalog(path: Path = FIXTURE_PATH) -> Catalog:
    payload = json.loads(path.read_text(encoding="utf-8"))
    versions: dict[tuple[str, int], dict] = {}
    for row in payload["versions"]:
        key = (row["source_id"], row["version"])
        if key in versions and versions[key] != row:
            raise ValueError(f"Conflicting synthetic version: {key}")
        versions[key] = row
    return Catalog(
        observed_at=datetime.fromisoformat(payload["observed_at"].replace("Z", "+00:00")),
        records=tuple(versions.values()),
        source_available=payload["source_available"],
    )


def grouped_versions(catalog: Catalog) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for row in catalog.records:
        groups.setdefault(row["source_id"], []).append(row)
    for rows in groups.values():
        rows.sort(key=lambda row: row["version"])
    return groups


def opening_state(row: dict, today: date) -> str:
    official_state = row["opening_state"]
    deadline = date.fromisoformat(row["deadline"]) if row["deadline"] else None
    if official_state == "closed" or (deadline is not None and deadline < today):
        return "closed"
    if official_state == "open" and deadline is not None:
        return "open"
    return "unknown"


def is_fresh(catalog: Catalog, now: datetime) -> bool:
    age = now.astimezone(timezone.utc) - catalog.observed_at.astimezone(timezone.utc)
    return catalog.source_available and timedelta(0) <= age <= timedelta(hours=24)


def search(
    catalog: Catalog,
    *,
    today: date,
    q: str = "",
    uf: str = "",
    category: str = "",
    state: str = "",
    deadline_from: date | None = None,
    deadline_to: date | None = None,
    page: int = 1,
) -> tuple[list[dict], int]:
    current = [rows[-1] for rows in grouped_versions(catalog).values()]
    filtered = []
    for row in current:
        deadline = date.fromisoformat(row["deadline"]) if row["deadline"] else None
        if q and q.casefold() not in row["object"].casefold():
            continue
        if uf and row["uf"].casefold() != uf.casefold():
            continue
        if category and row["category"].casefold() != category.casefold():
            continue
        if state and opening_state(row, today) != state:
            continue
        if deadline_from and (deadline is None or deadline < deadline_from):
            continue
        if deadline_to and (deadline is None or deadline > deadline_to):
            continue
        filtered.append({**row, "display_state": opening_state(row, today)})
    filtered.sort(key=lambda row: (row["deadline"] is None, row["deadline"] or "", row["source_id"]))
    start = (page - 1) * PAGE_SIZE
    return filtered[start:start + PAGE_SIZE], len(filtered)
