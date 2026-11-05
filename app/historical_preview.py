"""Offline comparison of synthetic public result observations.

This module has no PNCP, database, supplier quotation, or viability dependency.
Its prices describe historical observations only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo" / "historical_results.json"
MIN_SAMPLE = 3


@dataclass(frozen=True)
class Target:
    specification: str
    unit: str
    currency: str
    quantity_min: Decimal
    quantity_max: Decimal
    result_from: date
    result_to: date
    terms: str


@dataclass(frozen=True)
class Observation:
    result_key: str
    version: int
    source_item: str
    specification: str
    unit: str
    currency: str
    stage: str
    unit_price: Decimal
    quantity: Decimal
    result_date: date
    supplier_id: str | None
    supplier_name: str
    source_ref: str
    terms: str
    review_ref: str
    revoked: bool = False
    duplicate_pending: bool = False


@dataclass(frozen=True)
class Comparison:
    usable: tuple[Observation, ...]
    excluded: tuple[tuple[str, str], ...]
    candidates: tuple[tuple[str, str, int], ...]
    sample_sufficient: bool
    price_min: Decimal | None
    price_median: Decimal | None
    price_max: Decimal | None


def _decimal(value: object, field: str) -> Decimal:
    if isinstance(value, (float, bool)):
        raise ValueError(f"{field} must be a decimal string")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"invalid {field}") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field} must be finite and positive")
    return result


def load_fixture(path: Path = FIXTURE_PATH) -> tuple[Target, tuple[Observation, ...]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("synthetic") is not True:
        raise ValueError("historical preview accepts only synthetic fixtures")
    raw_target = payload["target"]
    target = Target(
        specification=raw_target["specification"], unit=raw_target["unit"],
        currency=raw_target["currency"],
        quantity_min=_decimal(raw_target["quantity_min"], "quantity_min"),
        quantity_max=_decimal(raw_target["quantity_max"], "quantity_max"),
        result_from=date.fromisoformat(raw_target["result_from"]),
        result_to=date.fromisoformat(raw_target["result_to"]), terms=raw_target["terms"],
    )
    observations = tuple(
        Observation(
            result_key=row["result_key"],
            version=int(row["version"]),
            source_item=row["source_item"],
            specification=row["specification"],
            unit=row["unit"],
            currency=row["currency"],
            stage=row["stage"],
            unit_price=_decimal(row["unit_price"], "unit_price"),
            quantity=_decimal(row["quantity"], "quantity"),
            result_date=date.fromisoformat(row["result_date"]),
            supplier_id=row.get("supplier_id"),
            supplier_name=row["supplier_name"],
            source_ref=row["source_ref"],
            terms=row["terms"],
            review_ref=row["review_ref"],
            revoked=row.get("revoked", False),
            duplicate_pending=row.get("duplicate_pending", False),
        )
        for row in payload["observations"]
    )
    return target, observations


def compare(target: Target, observations: tuple[Observation, ...], *, min_sample: int = MIN_SAMPLE) -> Comparison:
    if min_sample < 1:
        raise ValueError("min_sample must be positive")
    if target.quantity_min > target.quantity_max or target.result_from > target.result_to or not target.terms:
        raise ValueError("invalid comparison context")
    latest: dict[str, Observation] = {}
    seen_versions: dict[tuple[str, int], Observation] = {}
    for row in observations:
        if row.version < 1 or not row.result_key or not row.source_ref:
            raise ValueError("incomplete result identity or provenance")
        _decimal(row.unit_price, "unit_price")
        _decimal(row.quantity, "quantity")
        identity = (row.result_key, row.version)
        if identity in seen_versions and seen_versions[identity] != row:
            raise ValueError(f"conflicting version for {row.result_key}")
        seen_versions[identity] = row
        previous = latest.get(row.result_key)
        if previous and previous.source_item != row.source_item:
            raise ValueError(f"result identity spans source items: {row.result_key}")
        if previous is None or row.version > previous.version:
            latest[row.result_key] = row

    usable: list[Observation] = []
    excluded: list[tuple[str, str]] = []
    for row in sorted(latest.values(), key=lambda item: item.result_key):
        reason = None
        if row.revoked:
            reason = "revoked_latest_version"
        elif row.duplicate_pending:
            reason = "possible_duplicate_pending"
        elif row.stage != "homologated":
            reason = "different_stage"
        elif row.specification != target.specification:
            reason = "technical_mismatch_or_unproven"
        elif row.unit != target.unit:
            reason = "unit_conversion_unproven"
        elif row.currency != target.currency:
            reason = "currency_conversion_unproven"
        elif not target.quantity_min <= row.quantity <= target.quantity_max:
            reason = "quantity_outside_reviewed_range"
        elif not target.result_from <= row.result_date <= target.result_to:
            reason = "result_outside_reviewed_period"
        elif row.terms != target.terms:
            reason = "conditions_not_comparable"
        elif not row.review_ref:
            reason = "human_comparability_review_pending"
        elif not row.supplier_id:
            reason = "supplier_identity_pending"
        if reason:
            excluded.append((row.result_key, reason))
        else:
            usable.append(row)

    supplier_counts: dict[str, tuple[str, int]] = {}
    for row in usable:
        name, count = supplier_counts.get(row.supplier_id, (row.supplier_name, 0))
        supplier_counts[row.supplier_id] = (name, count + 1)
    candidates = tuple(
        (supplier_id, name, count)
        for supplier_id, (name, count) in sorted(supplier_counts.items())
    )
    prices = [row.unit_price for row in usable]
    return Comparison(
        usable=tuple(usable),
        excluded=tuple(excluded),
        candidates=candidates,
        sample_sufficient=len(prices) >= min_sample,
        price_min=min(prices) if prices else None,
        price_median=median(prices) if prices else None,
        price_max=max(prices) if prices else None,
    )
