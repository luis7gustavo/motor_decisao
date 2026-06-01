from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.database import engine


# Deliberately excludes decision_score: the heuristic score remains the
# baseline and hybrid guardrail, rather than leaking the teacher answer into
# the ML student.
FEATURE_NAMES = (
    "supplier_price",
    "estimated_market_price",
    "estimated_net_profit",
    "net_margin_pct",
    "total_fee_pct",
    "market_offer_count",
    "market_source_count",
    "price_history_count",
    "mercado_livre_count",
    "demand_score",
    "match_confidence",
    "risk_flag_count",
    "has_market_price",
    "has_positive_margin",
    "has_multiple_market_sources",
    "has_mercado_livre_signal",
)


@dataclass(frozen=True)
class FeatureRecord:
    supplier_product_id: str
    supplier_raw_id: str
    decision_run_id: str | None
    supplier_slug: str
    product_title: str
    heuristic_score: float
    heuristic_decision: str
    opportunity_label: int
    opportunity_class: int
    sample_weight: float
    features: dict[str, float]


def _to_float(value: Any, *, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def proxy_class(recommendation: str) -> int:
    return {
        "ignorar": 0,
        "revisar": 1,
        "comprar_teste": 2,
    }.get(str(recommendation), 0)


def proxy_label(recommendation: str) -> int:
    return 1 if proxy_class(recommendation) > 0 else 0


def teacher_sample_weight(recommendation: str) -> float:
    return {
        "ignorar": 0.50,
        "revisar": 0.80,
        "comprar_teste": 1.00,
    }.get(str(recommendation), 0.50)


def build_feature_snapshot(row: dict[str, Any]) -> dict[str, float]:
    risk_flags = row.get("risk_flags") or []
    market_source_count = _to_float(row.get("market_source_count"))
    mercado_livre_count = _to_float(row.get("mercado_livre_count"))
    estimated_market_price = _to_float(row.get("estimated_market_price"))
    net_margin_pct = _to_float(row.get("net_margin_pct"))
    features = {
        "supplier_price": _to_float(row.get("supplier_price")),
        "estimated_market_price": estimated_market_price,
        "estimated_net_profit": _to_float(row.get("estimated_net_profit")),
        "net_margin_pct": net_margin_pct,
        "total_fee_pct": _to_float(row.get("total_fee_pct")),
        "market_offer_count": _to_float(row.get("market_offer_count")),
        "market_source_count": market_source_count,
        "price_history_count": _to_float(row.get("price_history_count")),
        "mercado_livre_count": mercado_livre_count,
        "demand_score": _to_float(row.get("demand_score")),
        "match_confidence": _to_float(row.get("match_confidence")),
        "risk_flag_count": float(len(risk_flags)),
        "has_market_price": 1.0 if estimated_market_price > 0 else 0.0,
        "has_positive_margin": 1.0 if net_margin_pct > 0 else 0.0,
        "has_multiple_market_sources": 1.0 if market_source_count >= 2 else 0.0,
        "has_mercado_livre_signal": 1.0 if mercado_livre_count > 0 else 0.0,
    }
    return {feature: features[feature] for feature in FEATURE_NAMES}


def feature_record_from_row(row: dict[str, Any]) -> FeatureRecord:
    recommendation = str(row.get("recommendation") or "ignorar")
    return FeatureRecord(
        supplier_product_id=str(row["supplier_product_id"]),
        supplier_raw_id=str(row["supplier_raw_id"]),
        decision_run_id=str(row["decision_run_id"]) if row.get("decision_run_id") else None,
        supplier_slug=str(row["supplier_slug"]),
        product_title=str(row["product_title"]),
        heuristic_score=_to_float(row.get("decision_score")) / 100.0,
        heuristic_decision=recommendation,
        opportunity_label=proxy_label(recommendation),
        opportunity_class=proxy_class(recommendation),
        sample_weight=teacher_sample_weight(recommendation),
        features=build_feature_snapshot(row),
    )


def fetch_current_feature_records() -> list[FeatureRecord]:
    sql = text(
        """
        SELECT
            supplier_product_id::text AS supplier_product_id,
            supplier_raw_id::text AS supplier_raw_id,
            decision_run_id::text AS decision_run_id,
            supplier_slug,
            product_title,
            supplier_price,
            estimated_market_price,
            estimated_net_profit,
            net_margin_pct,
            total_fee_pct,
            market_offer_count,
            market_source_count,
            price_history_count,
            mercado_livre_count,
            demand_score,
            match_confidence,
            decision_score,
            recommendation,
            risk_flags
        FROM gold.decision_opportunities
        ORDER BY supplier_slug, product_title
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(sql).mappings().all()
    return [feature_record_from_row(dict(row)) for row in rows]


def records_as_matrix(records: list[FeatureRecord]) -> list[list[float]]:
    return [[record.features[name] for name in FEATURE_NAMES] for record in records]


def write_training_csv(path: Path, records: list[FeatureRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "supplier_product_id",
        "supplier_slug",
        "product_title",
        "heuristic_score",
        "heuristic_decision",
        "opportunity_label",
        "opportunity_class",
        "sample_weight",
        *FEATURE_NAMES,
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "supplier_product_id": record.supplier_product_id,
                    "supplier_slug": record.supplier_slug,
                    "product_title": record.product_title,
                    "heuristic_score": record.heuristic_score,
                    "heuristic_decision": record.heuristic_decision,
                    "opportunity_label": record.opportunity_label,
                    "opportunity_class": record.opportunity_class,
                    "sample_weight": record.sample_weight,
                    **record.features,
                }
            )


def write_dataset_metadata(path: Path, records: list[FeatureRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "rows": len(records),
        "positive_rows": sum(record.opportunity_label for record in records),
        "proxy_classes": {
            str(proxy): sum(record.opportunity_class == proxy for record in records)
            for proxy in (0, 1, 2)
        },
        "feature_names": list(FEATURE_NAMES),
        "label_definition": {
            "opportunity_label": "1 for revisar or comprar_teste; 0 for ignorar",
            "opportunity_class": "0 ignorar; 1 revisar; 2 comprar_teste",
            "caveat": "Proxy labels imitate heuristic knowledge and are not ground truth sales outcomes.",
        },
    }
    path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
