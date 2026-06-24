from __future__ import annotations

import hashlib
import math
import re
import statistics
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal
from difflib import SequenceMatcher
from typing import Any
from uuid import UUID

from sqlalchemy import text

from motor_decisao.app.core.database import engine
from motor_decisao.app.core.settings import get_settings
from motor_decisao.entity_resolution.product_attributes import (
    ProductAttributes,
    attribute_conflicts,
    conflict_penalty,
    extract_product_attributes,
)
from motor_decisao.pipelines.common.run_manager import (
    create_pipeline_run,
    create_source_run,
    finish_pipeline_run,
    finish_source_run,
    record_quality_check,
)
from motor_decisao.pipelines.common.serialization import to_json_text


STOPWORDS = {
    "a",
    "as",
    "ao",
    "aos",
    "com",
    "da",
    "das",
    "de",
    "do",
    "dos",
    "e",
    "em",
    "na",
    "nas",
    "no",
    "nos",
    "o",
    "os",
    "para",
    "por",
    "sem",
    "uma",
    "um",
    "un",
}

GENERIC_PRODUCT_TOKENS = {
    "adaptador",
    "audio",
    "cabo",
    "carregador",
    "combo",
    "controle",
    "femea",
    "fio",
    "fone",
    "gamer",
    "hub",
    "kit",
    "macho",
    "mouse",
    "notebook",
    "para",
    "preto",
    "sem",
    "suporte",
    "teclado",
    "usb",
    "video",
    "wireless",
}

SCORING_VERSION = "heuristic_v3_attribute_guard"
QUALIFIED_MATCH_SCORE = 58.0
BUY_BLOCKING_FLAGS = {
    "demanda_fraca",
    "demanda_incompleta",
    "fontes_insuficientes",
    "margem_baixa",
    "margem_indisponivel",
    "match_fraco",
    "match_revisar",
    "modelo_nao_confirmado",
    "atributo_tecnico_conflitante",
    "poucas_ofertas_mercado",
    "preco_mercado_muito_disperso",
    "sem_preco_mercado",
    "ticket_fornecedor_muito_baixo",
    "titulo_generico_sem_identificador",
}


@dataclass(frozen=True)
class SupplierProduct:
    id: str
    supplier_raw_id: str
    supplier_slug: str
    raw_title: str
    normalized_title: str
    tokens: tuple[str, ...]
    supplier_price: float | None
    sku: str | None
    ean: str | None
    source_url: str | None
    fetched_date: str | None
    attributes: ProductAttributes


@dataclass(frozen=True)
class EvidenceItem:
    evidence_id: str
    source_kind: str
    source_name: str
    title: str
    normalized_title: str
    tokens: tuple[str, ...]
    price: float
    position: int | None
    reviews_count: int | None
    sold_quantity: int | None
    demand_signal_value: float | None
    item_url: str | None
    fetched_date: str | None
    attributes: ProductAttributes


@dataclass(frozen=True)
class DecisionEngineResult:
    pipeline_run_id: str
    source_run_id: str
    decision_run_id: str
    scoring_version: str
    suppliers_normalized: int
    opportunities_scored: int
    comprar_teste: int
    revisar: int
    ignorar: int


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return int(number) if number is not None else None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\u00a0", " ")).strip()


def normalize_text(value: Any) -> str:
    text_value = _clean_text(value).lower()
    text_value = unicodedata.normalize("NFKD", text_value)
    text_value = "".join(char for char in text_value if not unicodedata.combining(char))
    text_value = re.sub(r"[^a-z0-9]+", " ", text_value)
    return _clean_text(text_value)


def tokenize(value: Any) -> tuple[str, ...]:
    tokens: list[str] = []
    for token in normalize_text(value).split():
        if token in STOPWORDS:
            continue
        if len(token) < 3 and not token.isdigit() and not any(char.isdigit() for char in token):
            continue
        tokens.append(token)
    return tuple(dict.fromkeys(tokens))


def _model_tokens(tokens: tuple[str, ...]) -> set[str]:
    return {token for token in tokens if any(char.isdigit() for char in token)}


def _has_product_identifier(product: SupplierProduct) -> bool:
    return bool(_model_tokens(product.tokens))


def _is_generic_supplier_title(product: SupplierProduct) -> bool:
    supplier_tokens = set(product.tokens)
    if not supplier_tokens or _has_product_identifier(product):
        return False
    supplier_specific = supplier_tokens - GENERIC_PRODUCT_TOKENS
    return len(supplier_specific) <= 1 and len(supplier_tokens) <= 5


def _source_product_key(row: dict[str, Any], normalized_title: str) -> str:
    supplier_slug = _clean_text(row["supplier_slug"])
    ean = _clean_text(row.get("ean"))
    sku = _clean_text(row.get("sku"))
    if ean:
        return f"{supplier_slug}:ean:{ean}"[:240]
    if sku:
        return f"{supplier_slug}:sku:{sku}"[:240]
    seed = f"{supplier_slug}|{normalized_title}|{row.get('raw_price')}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]
    return f"{supplier_slug}:title:{digest}"


def normalize_supplier_products() -> int:
    select_sql = text(
        """
        SELECT
            id,
            supplier_slug,
            raw_title,
            raw_price,
            currency_id,
            sku,
            ean,
            source_url,
            fetched_date,
            fetched_at,
            payload
        FROM bronze.supplier_products_raw
        WHERE raw_title IS NOT NULL
        """
    )
    upsert_sql = text(
        """
        INSERT INTO silver.supplier_products_normalized (
            supplier_raw_id,
            supplier_slug,
            source_product_key,
            raw_title,
            normalized_title,
            title_tokens,
            token_count,
            supplier_price,
            currency_id,
            sku,
            ean,
            source_url,
            fetched_date,
            fetched_at,
            payload
        )
        VALUES (
            :supplier_raw_id,
            :supplier_slug,
            :source_product_key,
            :raw_title,
            :normalized_title,
            :title_tokens,
            :token_count,
            :supplier_price,
            :currency_id,
            :sku,
            :ean,
            :source_url,
            :fetched_date,
            :fetched_at,
            CAST(:payload AS jsonb)
        )
        ON CONFLICT (supplier_raw_id) DO UPDATE
        SET
            supplier_slug = EXCLUDED.supplier_slug,
            source_product_key = EXCLUDED.source_product_key,
            raw_title = EXCLUDED.raw_title,
            normalized_title = EXCLUDED.normalized_title,
            title_tokens = EXCLUDED.title_tokens,
            token_count = EXCLUDED.token_count,
            supplier_price = EXCLUDED.supplier_price,
            currency_id = EXCLUDED.currency_id,
            sku = EXCLUDED.sku,
            ean = EXCLUDED.ean,
            source_url = EXCLUDED.source_url,
            fetched_date = EXCLUDED.fetched_date,
            fetched_at = EXCLUDED.fetched_at,
            payload = EXCLUDED.payload,
            updated_at = NOW()
        RETURNING id
        """
    )

    normalized = 0
    with engine.begin() as connection:
        rows = connection.execute(select_sql).mappings().all()
        for row in rows:
            row_dict = dict(row)
            title = _clean_text(row_dict["raw_title"])
            normalized_title = normalize_text(title)
            tokens = tokenize(title)
            if not normalized_title or not tokens:
                continue
            connection.execute(
                upsert_sql,
                {
                    "supplier_raw_id": row_dict["id"],
                    "supplier_slug": row_dict["supplier_slug"],
                    "source_product_key": _source_product_key(row_dict, normalized_title),
                    "raw_title": title,
                    "normalized_title": normalized_title,
                    "title_tokens": list(tokens),
                    "token_count": len(tokens),
                    "supplier_price": row_dict["raw_price"],
                    "currency_id": row_dict["currency_id"] or "BRL",
                    "sku": row_dict["sku"],
                    "ean": row_dict["ean"],
                    "source_url": row_dict["source_url"],
                    "fetched_date": row_dict["fetched_date"],
                    "fetched_at": row_dict["fetched_at"],
                    "payload": to_json_text(row_dict["payload"] or {}),
                },
            )
            normalized += 1
    return normalized


def _fetch_supplier_products() -> list[SupplierProduct]:
    sql = text(
        """
        WITH latest_products AS (
            SELECT
                id::text AS id,
                supplier_raw_id::text AS supplier_raw_id,
                supplier_slug,
                raw_title,
                normalized_title,
                title_tokens,
                supplier_price,
                sku,
                ean,
                source_url,
                payload,
                fetched_date::text AS fetched_date,
                ROW_NUMBER() OVER (
                    PARTITION BY supplier_slug, source_product_key
                    ORDER BY fetched_at DESC NULLS LAST, fetched_date DESC, id DESC
                ) AS row_num
            FROM silver.supplier_products_normalized
            WHERE supplier_price IS NOT NULL
              AND supplier_price > 0
        )
        SELECT
            id,
            supplier_raw_id,
            supplier_slug,
            raw_title,
            normalized_title,
            title_tokens,
            supplier_price,
            sku,
            ean,
            source_url,
            payload,
            fetched_date
        FROM latest_products
        WHERE row_num = 1
        ORDER BY supplier_slug, supplier_price DESC, raw_title
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(sql).mappings().all()
    products = []
    for row in rows:
        products.append(
            SupplierProduct(
                id=row["id"],
                supplier_raw_id=row["supplier_raw_id"],
                supplier_slug=row["supplier_slug"],
                raw_title=row["raw_title"],
                normalized_title=row["normalized_title"],
                tokens=tuple(row["title_tokens"] or tokenize(row["raw_title"])),
                supplier_price=_to_float(row["supplier_price"]),
                sku=row["sku"],
                ean=row["ean"],
                source_url=row["source_url"],
                fetched_date=row["fetched_date"],
                attributes=extract_product_attributes(row["raw_title"], payload=row["payload"] or {}),
            )
        )
    return products


def _fetch_evidence_items() -> list[EvidenceItem]:
    market_sql = text(
        """
        SELECT
            id::text AS evidence_id,
            'market_web' AS source_kind,
            source_name,
            COALESCE(title, query, '') AS title,
            price,
            position,
            reviews_count,
            sold_quantity,
            demand_signal_value,
            item_url,
            payload,
            fetched_date::text AS fetched_date
        FROM bronze.market_web_listings_raw
        WHERE price IS NOT NULL
          AND price > 0
          AND blocked = FALSE
        """
    )
    price_history_sql = text(
        """
        SELECT
            id::text AS evidence_id,
            'price_history' AS source_kind,
            source_name,
            COALESCE(title, query, product_key, '') AS title,
            current_price AS price,
            NULL::integer AS position,
            NULL::integer AS reviews_count,
            NULL::integer AS sold_quantity,
            NULL::numeric AS demand_signal_value,
            product_url AS item_url,
            payload,
            fetched_date::text AS fetched_date
        FROM bronze.price_history_raw
        WHERE current_price IS NOT NULL
          AND current_price > 0
          AND blocked = FALSE
        """
    )
    mercado_livre_sql = text(
        """
        SELECT
            id::text AS evidence_id,
            'mercado_livre' AS source_kind,
            source_name,
            COALESCE(product_name, query, '') AS title,
            COALESCE(avg_price, min_price, max_price) AS price,
            NULL::integer AS position,
            NULL::integer AS reviews_count,
            offers_count AS sold_quantity,
            offers_count::numeric AS demand_signal_value,
            NULL::text AS item_url,
            payload,
            fetched_date::text AS fetched_date
        FROM silver.mercado_livre_product_prices
        WHERE COALESCE(avg_price, min_price, max_price) IS NOT NULL
          AND COALESCE(avg_price, min_price, max_price) > 0
        """
    )
    evidence: list[EvidenceItem] = []
    with engine.connect() as connection:
        for sql in (market_sql, price_history_sql, mercado_livre_sql):
            for row in connection.execute(sql).mappings():
                title = _clean_text(row["title"])
                tokens = tokenize(title)
                if not title or not tokens:
                    continue
                evidence.append(
                    EvidenceItem(
                        evidence_id=row["evidence_id"],
                        source_kind=row["source_kind"],
                        source_name=row["source_name"],
                        title=title,
                        normalized_title=normalize_text(title),
                        tokens=tokens,
                        price=float(row["price"]),
                        position=_to_int(row["position"]),
                        reviews_count=_to_int(row["reviews_count"]),
                        sold_quantity=_to_int(row["sold_quantity"]),
                        demand_signal_value=_to_float(row["demand_signal_value"]),
                        item_url=row["item_url"],
                        fetched_date=row["fetched_date"],
                        attributes=extract_product_attributes(title, payload=row["payload"] or {}),
                    )
                )
    return evidence


def _build_candidate_index(evidence_items: list[EvidenceItem]) -> dict[str, list[int]]:
    token_counts = Counter(token for item in evidence_items for token in item.tokens)
    index: dict[str, list[int]] = defaultdict(list)
    max_common = max(120, len(evidence_items) // 4)
    for item_index, item in enumerate(evidence_items):
        for token in item.tokens:
            if token_counts[token] <= max_common:
                index[token].append(item_index)
    return index


def _match_confidence(product: SupplierProduct, item: EvidenceItem) -> float:
    supplier_tokens = set(product.tokens)
    evidence_tokens = set(item.tokens)
    if not supplier_tokens or not evidence_tokens:
        return 0.0
    overlap = supplier_tokens & evidence_tokens
    if not overlap:
        return 0.0

    supplier_specific = supplier_tokens - GENERIC_PRODUCT_TOKENS
    specific_overlap = supplier_specific & evidence_tokens
    coverage = len(overlap) / max(1, min(len(supplier_tokens), 10))
    precision = len(overlap) / max(1, min(len(evidence_tokens), 14))
    sequence = SequenceMatcher(None, product.normalized_title, item.normalized_title).ratio()
    specific_bonus = min(1.0, len(specific_overlap) / max(1, min(len(supplier_specific), 5)))
    score = ((0.45 * coverage) + (0.20 * precision) + (0.20 * sequence) + (0.15 * specific_bonus)) * 100

    if len(overlap) < 2:
        score *= 0.45
    if supplier_specific and not specific_overlap:
        score *= 0.72
    product_models = _model_tokens(product.tokens)
    evidence_models = _model_tokens(item.tokens)
    if product_models and evidence_models and not (product_models & evidence_models):
        score *= 0.35
    elif product_models and not evidence_models:
        score *= 0.82
    conflicts = attribute_conflicts(product.attributes, item.attributes)
    score *= conflict_penalty(conflicts)
    if product.supplier_price and item.price > product.supplier_price * 20 and product.supplier_price < 15:
        score *= 0.85
    return round(min(score, 100.0), 2)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[int(position)]
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * (position - lower))


def _estimate_price(matches: list[tuple[EvidenceItem, float]]) -> float | None:
    prices = [item.price for item, score in matches[:30] if score >= QUALIFIED_MATCH_SCORE and item.price > 0]
    if not prices:
        return None
    return round(_percentile(prices, 0.35) or 0, 2)


def _demand_score(matches: list[tuple[EvidenceItem, float]], estimated_price: float | None) -> float:
    if not matches:
        return 0.0
    source_names = {item.source_name for item, _score in matches}
    market_count = sum(1 for item, _score in matches if item.source_kind == "market_web")
    price_history_count = sum(1 for item, _score in matches if item.source_kind == "price_history")
    ml_count = sum(1 for item, _score in matches if item.source_kind == "mercado_livre")
    review_signal = max((item.reviews_count or 0) for item, _score in matches)
    sold_signal = max((item.sold_quantity or 0) for item, _score in matches)
    top_position_hits = sum(1 for item, _score in matches if item.position is not None and item.position <= 12)

    score = 0.0
    score += min(34.0, market_count * 1.5)
    score += min(28.0, len(source_names) * 7.0)
    score += min(12.0, price_history_count * 1.5)
    score += min(12.0, ml_count * 3.0)
    score += min(8.0, top_position_hits * 1.0)
    if review_signal >= 100:
        score += 8
    elif review_signal >= 20:
        score += 5
    if sold_signal >= 20:
        score += 6
    elif sold_signal >= 5:
        score += 3
    if estimated_price is not None:
        if 30 <= estimated_price <= 300:
            score += 6
        elif 300 < estimated_price <= 800:
            score += 3
        elif estimated_price < 20:
            score -= 4
    return round(max(0.0, min(score, 100.0)), 2)


def _risk_flags(
    *,
    product: SupplierProduct,
    matches: list[tuple[EvidenceItem, float]],
    estimated_price: float | None,
    net_margin_pct: float | None,
    demand_score: float,
    match_confidence: float,
    market_offer_count: int,
    market_source_count: int,
    price_history_count: int,
    mercado_livre_count: int,
    min_net_margin_pct: float,
) -> list[str]:
    flags: list[str] = []
    if estimated_price is None:
        flags.append("sem_preco_mercado")
    if match_confidence < 65:
        flags.append("match_fraco")
    elif match_confidence < 75:
        flags.append("match_revisar")
    if demand_score < 40:
        flags.append("demanda_fraca")
    elif demand_score < 60:
        flags.append("demanda_incompleta")
    if net_margin_pct is None:
        flags.append("margem_indisponivel")
    elif net_margin_pct < min_net_margin_pct:
        flags.append("margem_baixa")
    if product.supplier_price is not None and product.supplier_price < 5:
        flags.append("ticket_fornecedor_muito_baixo")
    if _is_generic_supplier_title(product):
        flags.append("titulo_generico_sem_identificador")
    source_count = market_source_count + (1 if price_history_count else 0) + (1 if mercado_livre_count else 0)
    if source_count < 2:
        flags.append("fontes_insuficientes")
    if market_offer_count < 3:
        flags.append("poucas_ofertas_mercado")

    model_tokens = _model_tokens(product.tokens)
    if model_tokens and not any(model_tokens & set(item.tokens) for item, score in matches if score >= QUALIFIED_MATCH_SCORE):
        flags.append("modelo_nao_confirmado")
    if any(attribute_conflicts(product.attributes, item.attributes) for item, _score in matches[:12]):
        flags.append("atributo_tecnico_conflitante")

    prices = [item.price for item, score in matches if score >= QUALIFIED_MATCH_SCORE and item.price > 0]
    if len(prices) >= 4:
        median_price = statistics.median(prices)
        if median_price and statistics.pstdev(prices) / median_price > 0.85:
            flags.append("preco_mercado_muito_disperso")
    return flags


def _confidence_level(
    *,
    net_margin_pct: float | None,
    demand_score: float,
    match_confidence: float,
    market_source_count: int,
    price_history_count: int,
    mercado_livre_count: int,
    risk_flags: list[str],
    min_net_margin_pct: float,
) -> str:
    source_count = market_source_count + (1 if price_history_count else 0) + (1 if mercado_livre_count else 0)
    blocking_flags = set(risk_flags) & BUY_BLOCKING_FLAGS
    if (
        net_margin_pct is not None
        and net_margin_pct >= min_net_margin_pct + 0.05
        and demand_score >= 65
        and match_confidence >= 82
        and source_count >= 3
        and market_source_count >= 2
        and not blocking_flags
    ):
        return "alta"
    if (
        net_margin_pct is not None
        and net_margin_pct >= min_net_margin_pct
        and demand_score >= 45
        and match_confidence >= 68
        and source_count >= 2
        and not ({"sem_preco_mercado", "margem_baixa", "match_fraco"} & set(risk_flags))
    ):
        return "media"
    return "baixa"


def _recommendation(
    *,
    net_margin_pct: float | None,
    demand_score: float,
    match_confidence: float,
    min_net_margin_pct: float,
    risk_flags: list[str],
    confidence_level: str,
) -> str:
    if (
        net_margin_pct is not None
        and net_margin_pct >= min_net_margin_pct + 0.05
        and demand_score >= 65
        and match_confidence >= 82
        and confidence_level == "alta"
        and not (set(risk_flags) & BUY_BLOCKING_FLAGS)
    ):
        return "comprar_teste"
    if (
        net_margin_pct is not None
        and net_margin_pct >= min_net_margin_pct
        and demand_score >= 40
        and match_confidence >= 65
        and confidence_level in {"alta", "media"}
    ):
        return "revisar"
    return "ignorar"


def _decision_score(
    *,
    net_margin_pct: float | None,
    demand_score: float,
    match_confidence: float,
    source_count: int,
    risk_flags: list[str],
) -> float:
    if net_margin_pct is None:
        margin_score = 0.0
    else:
        margin_score = max(0.0, min(100.0, (net_margin_pct / 0.50) * 100.0))
    source_score = min(100.0, source_count * 20.0)
    risk_penalty = min(24.0, len(risk_flags) * 6.0)
    score = (margin_score * 0.35) + (demand_score * 0.25) + (match_confidence * 0.25) + (source_score * 0.15)
    return round(max(0.0, min(100.0, score - risk_penalty)), 2)


def _best_matches_for_product(
    product: SupplierProduct,
    evidence_items: list[EvidenceItem],
    index: dict[str, list[int]],
) -> list[tuple[EvidenceItem, float]]:
    candidate_counter: Counter[int] = Counter()
    for token in product.tokens:
        for item_index in index.get(token, []):
            candidate_counter[item_index] += 1
    if not candidate_counter:
        return []

    matches: list[tuple[EvidenceItem, float]] = []
    for item_index, _overlap in candidate_counter.most_common(350):
        item = evidence_items[item_index]
        score = _match_confidence(product, item)
        if score >= 42:
            matches.append((item, score))
    matches.sort(key=lambda pair: (pair[1], pair[0].price), reverse=True)
    return matches[:60]


def _evidence_payload(
    product: SupplierProduct,
    matches: list[tuple[EvidenceItem, float]],
    estimated_price: float | None,
) -> dict[str, Any]:
    top_matches = matches[:12]
    return {
        "scoring_version": SCORING_VERSION,
        "estimated_price_method": "p35_matched_prices",
        "estimated_market_price": estimated_price,
        "product_attributes": product.attributes.__dict__,
        "top_matches": [
            {
                "source_kind": item.source_kind,
                "source_name": item.source_name,
                "title": item.title,
                "price": item.price,
                "match_confidence": score,
                "position": item.position,
                "reviews_count": item.reviews_count,
                "sold_quantity": item.sold_quantity,
                "item_url": item.item_url,
                "fetched_date": item.fetched_date,
                "evidence_attributes": item.attributes.__dict__,
                "attribute_conflicts": attribute_conflicts(product.attributes, item.attributes),
            }
            for item, score in top_matches
        ],
    }


def _create_decision_run(
    connection,
    *,
    pipeline_run_id: UUID,
    project_config: dict[str, Any],
    metadata: dict[str, Any],
) -> UUID:
    row = connection.execute(
        text(
            """
            INSERT INTO gold.decision_engine_runs (
                pipeline_run_id,
                scoring_version,
                status,
                config_snapshot,
                metadata
            )
            VALUES (
                :pipeline_run_id,
                :scoring_version,
                'running',
                CAST(:config_snapshot AS jsonb),
                CAST(:metadata AS jsonb)
            )
            RETURNING id
            """
        ),
        {
            "pipeline_run_id": pipeline_run_id,
            "scoring_version": SCORING_VERSION,
            "config_snapshot": to_json_text(project_config),
            "metadata": to_json_text(metadata),
        },
    ).one()
    return row.id


def _finish_decision_run(
    connection,
    *,
    decision_run_id: UUID,
    status: str,
    metadata: dict[str, Any],
    error_message: str | None,
) -> None:
    connection.execute(
        text(
            """
            UPDATE gold.decision_engine_runs
            SET
                status = :status,
                metadata = CAST(:metadata AS jsonb),
                error_message = :error_message,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE id = :decision_run_id
            """
        ),
        {
            "decision_run_id": decision_run_id,
            "status": status,
            "metadata": to_json_text(metadata),
            "error_message": error_message,
        },
    )


def _upsert_opportunity(connection, opportunity: dict[str, Any]) -> None:
    connection.execute(
        text(
            """
            INSERT INTO gold.decision_opportunities (
                decision_run_id,
                scoring_version,
                supplier_product_id,
                supplier_raw_id,
                supplier_slug,
                product_title,
                normalized_title,
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
                confidence_level,
                risk_flags,
                evidence
            )
            VALUES (
                CAST(:decision_run_id AS uuid),
                :scoring_version,
                CAST(:supplier_product_id AS uuid),
                CAST(:supplier_raw_id AS uuid),
                :supplier_slug,
                :product_title,
                :normalized_title,
                :supplier_price,
                :estimated_market_price,
                :estimated_net_profit,
                :net_margin_pct,
                :total_fee_pct,
                :market_offer_count,
                :market_source_count,
                :price_history_count,
                :mercado_livre_count,
                :demand_score,
                :match_confidence,
                :decision_score,
                :recommendation,
                :confidence_level,
                :risk_flags,
                CAST(:evidence AS jsonb)
            )
            ON CONFLICT (supplier_product_id) DO UPDATE
            SET
                decision_run_id = EXCLUDED.decision_run_id,
                scoring_version = EXCLUDED.scoring_version,
                supplier_raw_id = EXCLUDED.supplier_raw_id,
                supplier_slug = EXCLUDED.supplier_slug,
                product_title = EXCLUDED.product_title,
                normalized_title = EXCLUDED.normalized_title,
                supplier_price = EXCLUDED.supplier_price,
                estimated_market_price = EXCLUDED.estimated_market_price,
                estimated_net_profit = EXCLUDED.estimated_net_profit,
                net_margin_pct = EXCLUDED.net_margin_pct,
                total_fee_pct = EXCLUDED.total_fee_pct,
                market_offer_count = EXCLUDED.market_offer_count,
                market_source_count = EXCLUDED.market_source_count,
                price_history_count = EXCLUDED.price_history_count,
                mercado_livre_count = EXCLUDED.mercado_livre_count,
                demand_score = EXCLUDED.demand_score,
                match_confidence = EXCLUDED.match_confidence,
                decision_score = EXCLUDED.decision_score,
                recommendation = EXCLUDED.recommendation,
                confidence_level = EXCLUDED.confidence_level,
                risk_flags = EXCLUDED.risk_flags,
                evidence = EXCLUDED.evidence,
                generated_at = NOW(),
                updated_at = NOW()
            """
        ),
        opportunity,
    )
    connection.execute(
        text(
            """
            INSERT INTO gold.decision_opportunity_snapshots (
                decision_run_id,
                scoring_version,
                supplier_product_id,
                supplier_raw_id,
                supplier_slug,
                product_title,
                normalized_title,
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
                confidence_level,
                risk_flags,
                evidence
            )
            VALUES (
                CAST(:decision_run_id AS uuid),
                :scoring_version,
                CAST(:supplier_product_id AS uuid),
                CAST(:supplier_raw_id AS uuid),
                :supplier_slug,
                :product_title,
                :normalized_title,
                :supplier_price,
                :estimated_market_price,
                :estimated_net_profit,
                :net_margin_pct,
                :total_fee_pct,
                :market_offer_count,
                :market_source_count,
                :price_history_count,
                :mercado_livre_count,
                :demand_score,
                :match_confidence,
                :decision_score,
                :recommendation,
                :confidence_level,
                :risk_flags,
                CAST(:evidence AS jsonb)
            )
            ON CONFLICT (decision_run_id, supplier_product_id) DO UPDATE
            SET
                scoring_version = EXCLUDED.scoring_version,
                supplier_raw_id = EXCLUDED.supplier_raw_id,
                supplier_slug = EXCLUDED.supplier_slug,
                product_title = EXCLUDED.product_title,
                normalized_title = EXCLUDED.normalized_title,
                supplier_price = EXCLUDED.supplier_price,
                estimated_market_price = EXCLUDED.estimated_market_price,
                estimated_net_profit = EXCLUDED.estimated_net_profit,
                net_margin_pct = EXCLUDED.net_margin_pct,
                total_fee_pct = EXCLUDED.total_fee_pct,
                market_offer_count = EXCLUDED.market_offer_count,
                market_source_count = EXCLUDED.market_source_count,
                price_history_count = EXCLUDED.price_history_count,
                mercado_livre_count = EXCLUDED.mercado_livre_count,
                demand_score = EXCLUDED.demand_score,
                match_confidence = EXCLUDED.match_confidence,
                decision_score = EXCLUDED.decision_score,
                recommendation = EXCLUDED.recommendation,
                confidence_level = EXCLUDED.confidence_level,
                risk_flags = EXCLUDED.risk_flags,
                evidence = EXCLUDED.evidence,
                generated_at = NOW()
            """
        ),
        opportunity,
    )


def _delete_stale_current_opportunities(connection, *, decision_run_id: UUID) -> int:
    row = connection.execute(
        text(
            """
            DELETE FROM gold.decision_opportunities
            WHERE decision_run_id IS DISTINCT FROM CAST(:decision_run_id AS uuid)
            """
        ),
        {"decision_run_id": decision_run_id},
    )
    return int(row.rowcount or 0)


def build_decision_opportunities(*, triggered_by: str = "local_cli_decision_engine") -> DecisionEngineResult:
    settings = get_settings()
    project_config = settings.load_project_config()
    margin_config = project_config.get("margin", {})
    total_fee_pct = (
        float(margin_config.get("ml_fee_pct", 0.12))
        + float(margin_config.get("tax_pct", 0.17))
        + float(margin_config.get("shipping_pct", 0.03))
    )
    min_net_margin_pct = float(margin_config.get("min_net_margin_pct", 0.20))

    suppliers_normalized = normalize_supplier_products()
    products = _fetch_supplier_products()
    evidence_items = _fetch_evidence_items()
    index = _build_candidate_index(evidence_items)
    metadata = {
        "supplier_products": len(products),
        "evidence_items": len(evidence_items),
        "scoring_version": SCORING_VERSION,
        "total_fee_pct": total_fee_pct,
        "min_net_margin_pct": min_net_margin_pct,
    }

    with engine.begin() as connection:
        pipeline_run_id = create_pipeline_run(
            connection,
            pipeline_name="decision_engine_scoring",
            triggered_by=triggered_by,
            config_snapshot=project_config,
            metadata=metadata,
        )
        source_run_id = create_source_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_name="decision_engine",
            source_type="internal",
            raw_table_name="gold.decision_opportunity_snapshots",
            metadata=metadata,
        )
        decision_run_id = _create_decision_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            project_config=project_config,
            metadata=metadata,
        )

    counts = Counter()
    confidence_counts = Counter()
    scored = 0
    stale_current_deleted = 0
    error_message = None
    try:
        with engine.begin() as connection:
            for product in products:
                matches = _best_matches_for_product(product, evidence_items, index)
                estimated_price = _estimate_price(matches)
                supplier_price = product.supplier_price
                if estimated_price is None or supplier_price is None:
                    estimated_net_profit = None
                    net_margin_pct = None
                else:
                    estimated_net_profit = round((estimated_price * (1 - total_fee_pct)) - supplier_price, 2)
                    net_margin_pct = round(estimated_net_profit / estimated_price, 4)

                market_offer_count = sum(1 for item, _score in matches if item.source_kind == "market_web")
                market_source_count = len({item.source_name for item, _score in matches if item.source_kind == "market_web"})
                price_history_count = sum(1 for item, _score in matches if item.source_kind == "price_history")
                mercado_livre_count = sum(1 for item, _score in matches if item.source_kind == "mercado_livre")
                demand_score = _demand_score(matches, estimated_price)
                match_confidence = round(max((score for _item, score in matches), default=0.0), 2)
                risk_flags = _risk_flags(
                    product=product,
                    matches=matches,
                    estimated_price=estimated_price,
                    net_margin_pct=net_margin_pct,
                    demand_score=demand_score,
                    match_confidence=match_confidence,
                    market_offer_count=market_offer_count,
                    market_source_count=market_source_count,
                    price_history_count=price_history_count,
                    mercado_livre_count=mercado_livre_count,
                    min_net_margin_pct=min_net_margin_pct,
                )
                confidence_level = _confidence_level(
                    net_margin_pct=net_margin_pct,
                    demand_score=demand_score,
                    match_confidence=match_confidence,
                    market_source_count=market_source_count,
                    price_history_count=price_history_count,
                    mercado_livre_count=mercado_livre_count,
                    risk_flags=risk_flags,
                    min_net_margin_pct=min_net_margin_pct,
                )
                recommendation = _recommendation(
                    net_margin_pct=net_margin_pct,
                    demand_score=demand_score,
                    match_confidence=match_confidence,
                    min_net_margin_pct=min_net_margin_pct,
                    risk_flags=risk_flags,
                    confidence_level=confidence_level,
                )
                decision_score = _decision_score(
                    net_margin_pct=net_margin_pct,
                    demand_score=demand_score,
                    match_confidence=match_confidence,
                    source_count=market_source_count + (1 if price_history_count else 0) + (1 if mercado_livre_count else 0),
                    risk_flags=risk_flags,
                )
                _upsert_opportunity(
                    connection,
                    {
                        "decision_run_id": str(decision_run_id),
                        "scoring_version": SCORING_VERSION,
                        "supplier_product_id": product.id,
                        "supplier_raw_id": product.supplier_raw_id,
                        "supplier_slug": product.supplier_slug,
                        "product_title": product.raw_title,
                        "normalized_title": product.normalized_title,
                        "supplier_price": supplier_price,
                        "estimated_market_price": estimated_price,
                        "estimated_net_profit": estimated_net_profit,
                        "net_margin_pct": net_margin_pct,
                        "total_fee_pct": total_fee_pct,
                        "market_offer_count": market_offer_count,
                        "market_source_count": market_source_count,
                        "price_history_count": price_history_count,
                        "mercado_livre_count": mercado_livre_count,
                        "demand_score": demand_score,
                        "match_confidence": match_confidence,
                        "decision_score": decision_score,
                        "recommendation": recommendation,
                        "confidence_level": confidence_level,
                        "risk_flags": risk_flags,
                        "evidence": to_json_text(_evidence_payload(product, matches, estimated_price)),
                    },
                )
                counts[recommendation] += 1
                confidence_counts[confidence_level] += 1
                scored += 1
            if scored > 0:
                stale_current_deleted = _delete_stale_current_opportunities(
                    connection,
                    decision_run_id=decision_run_id,
                )
        status = "success"
    except Exception as exc:
        status = "failed"
        error_message = str(exc)

    final_metadata = {
        **metadata,
        "decision_run_id": str(decision_run_id),
        "opportunities_scored": scored,
        "stale_current_deleted": stale_current_deleted,
        "recommendations": dict(counts),
        "confidence_levels": dict(confidence_counts),
        **dict(counts),
    }

    with engine.begin() as connection:
        finish_source_run(
            connection,
            source_run_id=source_run_id,
            status=status,
            records_extracted=len(products),
            records_loaded=scored,
            records_skipped=max(0, len(products) - scored),
            metadata=final_metadata,
            error_message=error_message,
        )
        record_quality_check(
            connection,
            pipeline_run_id=pipeline_run_id,
            source_run_id=source_run_id,
            schema_name="gold",
            table_name="decision_opportunities",
            check_name="opportunities_scored_gt_zero",
            status="passed" if scored > 0 else "failed",
            metric_name="opportunities_scored",
            metric_value=scored,
            threshold_value=1,
            details=final_metadata,
            message=None if scored > 0 else "No supplier products were scored.",
        )
        finish_pipeline_run(
            connection,
            pipeline_run_id=pipeline_run_id,
            status=status,
            metadata=final_metadata,
            error_message=error_message,
        )
        _finish_decision_run(
            connection,
            decision_run_id=decision_run_id,
            status=status,
            metadata=final_metadata,
            error_message=error_message,
        )

    if error_message:
        raise RuntimeError(error_message)

    return DecisionEngineResult(
        pipeline_run_id=str(pipeline_run_id),
        source_run_id=str(source_run_id),
        decision_run_id=str(decision_run_id),
        scoring_version=SCORING_VERSION,
        suppliers_normalized=suppliers_normalized,
        opportunities_scored=scored,
        comprar_teste=counts["comprar_teste"],
        revisar=counts["revisar"],
        ignorar=counts["ignorar"],
    )
