from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


csv.field_size_limit(1024 * 1024 * 512)


MARKET_PATTERN = "market_web_listings_raw_*.csv"
ML_ITEMS_PATTERN = "mercado_livre_items_raw_*.csv"
ML_PRODUCTS_PATTERN = "mercado_livre_products_raw_*.csv"


OFFER_FIELDS = [
    "source_name",
    "query",
    "category",
    "title",
    "title_key",
    "price",
    "old_price",
    "discount_pct",
    "currency_id",
    "sold_quantity",
    "demand_signal_type",
    "demand_signal_value",
    "rating",
    "reviews_count",
    "seller_text",
    "shipping_text",
    "item_url",
    "image_url",
    "is_sponsored",
    "is_full",
    "is_catalog",
    "fetched_date",
    "fetched_at",
    "source_file",
    "source_record_id",
]


ML_PRODUCT_FIELDS = [
    "product_id",
    "product_name",
    "product_name_key",
    "domain_id",
    "category",
    "status",
    "brand",
    "line",
    "model",
    "gtin",
    "main_color",
    "first_image_url",
    "queries",
    "query_count",
    "fetched_date",
    "fetched_at",
    "source_file",
]


OPPORTUNITY_FIELDS = [
    "rank",
    "opportunity_score",
    "query",
    "category",
    "source_name",
    "title",
    "price",
    "query_median_price",
    "query_min_price",
    "query_offer_count",
    "query_source_count",
    "price_gap_to_median_pct",
    "demand_proxy",
    "confidence",
    "item_url",
    "image_url",
    "fetched_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL rapido dos exports Bronze CSV para Silver local."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "data",
        help="Pasta com os CSVs exportados da Bronze.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Pasta de saida. Padrao: <data-dir>/processed.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Sufixo dos arquivos de saida. Padrao: timestamp atual.",
    )
    return parser.parse_args()


def latest_file(data_dir: Path, pattern: str) -> Path | None:
    files = sorted(data_dir.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    return files[0] if files else None


def read_rows(path: Path | None) -> list[dict[str, str]]:
    if path is None:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def normalize_spaces(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text).strip()


def fix_mojibake(value: str | None) -> str:
    text = normalize_spaces(value)
    if not text or not any(marker in text for marker in ("Ã", "Â", "â€", "�")):
        return text
    try:
        fixed = text.encode("latin1", errors="strict").decode("utf-8", errors="strict")
    except UnicodeError:
        return text
    return fixed if len(fixed) >= max(3, len(text) // 2) else text


def ascii_key(value: str | None) -> str:
    text = fix_mojibake(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return normalize_spaces(text)


def compact_title_key(value: str | None) -> str:
    text = ascii_key(value)
    stopwords = {
        "de",
        "da",
        "do",
        "das",
        "dos",
        "com",
        "para",
        "por",
        "em",
        "a",
        "o",
        "e",
        "r",
        "rs",
        "br",
    }
    tokens = [token for token in text.split() if token not in stopwords]
    return " ".join(tokens[:18])


def to_float(value: object) -> float | None:
    if value is None:
        return None
    text = normalize_spaces(str(value))
    if not text:
        return None
    text = text.replace("R$", "").replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        number = float(text)
    except ValueError:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def to_int(value: object) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def to_bool(value: object) -> bool:
    return normalize_spaces(str(value)).lower() in {"1", "true", "yes", "sim"}


def money(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def pct(value: float | None) -> str:
    return "" if value is None else f"{value:.4f}"


def clean_url(value: str | None) -> str:
    text = normalize_spaces(value)
    if not text:
        return ""
    parts = urlsplit(text)
    if not parts.scheme or not parts.netloc:
        return text
    if "amazon.com.br" in parts.netloc:
        match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})", parts.path)
        if match:
            return urlunsplit((parts.scheme, parts.netloc, f"/dp/{match.group(1)}", "", ""))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def parse_json_object(value: str | None) -> dict[str, object]:
    text = normalize_spaces(value)
    if not text:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def get_attribute(payload: dict[str, object], attribute_id: str) -> str:
    attributes = payload.get("attributes")
    if not isinstance(attributes, list):
        return ""
    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        if attribute.get("id") == attribute_id:
            return fix_mojibake(attribute.get("value_name"))
    return ""


def get_first_picture(payload: dict[str, object]) -> str:
    pictures = payload.get("pictures")
    if not isinstance(pictures, list) or not pictures:
        return ""
    first = pictures[0]
    if not isinstance(first, dict):
        return ""
    return normalize_spaces(first.get("url"))


def infer_category(query: str | None, title: str | None, domain_id: str | None = None) -> str:
    text = " ".join([ascii_key(query), ascii_key(title), ascii_key(domain_id)])
    padded = f" {text} "
    tokens = set(text.split())

    def has_term(term: str) -> bool:
        return f" {term} " in padded if " " in term else term in tokens

    rules = [
        ("mousepad", ("mouse pad", "mousepad")),
        ("webcam", ("webcam", "camera web")),
        ("mouse", ("mouse", "mice")),
        ("teclado", ("teclado", "keyboard")),
        ("headset_fone", ("headset", "fone", "fones", "headphone", "earphone")),
        ("hub_adaptador", ("hub", "adaptador", "adapter", "wifi usb")),
        ("cabo", ("cabo", "cable", "usb c", "hdmi")),
        ("notebook_acessorio", ("suporte notebook", "cooler notebook")),
        ("notebook", ("notebook", "laptop")),
    ]
    for category, terms in rules:
        if any(has_term(term) for term in terms):
            return category
    return "outros"


def parse_rating(value: str | None) -> str:
    text = normalize_spaces(value)
    if not text:
        return ""
    match = re.search(r"([0-5](?:[.,]\d)?)", text)
    if not match:
        return ""
    number = to_float(match.group(1))
    if number is None or number > 5:
        return ""
    return f"{number:.1f}"


def row_fetched_at(row: dict[str, str]) -> str:
    return normalize_spaces(row.get("fetched_at") or row.get("created_at") or "")


def transform_market_offers(rows: list[dict[str, str]], source_file: Path | None) -> tuple[list[dict[str, object]], Counter]:
    stats: Counter = Counter()
    output: list[dict[str, object]] = []
    source_name = source_file.name if source_file else ""

    for row in rows:
        stats["market_rows_in"] += 1
        if to_bool(row.get("blocked")):
            stats["market_blocked_dropped"] += 1
            continue

        price = to_float(row.get("price"))
        title = fix_mojibake(row.get("title"))
        if price is None or price <= 0:
            stats["market_without_price_dropped"] += 1
            continue
        if not title:
            stats["market_without_title_dropped"] += 1
            continue

        old_price = to_float(row.get("old_price"))
        discount = None
        if old_price and old_price > price:
            discount = (old_price - price) / old_price

        query = fix_mojibake(row.get("query"))
        category = infer_category(query, title)
        output.append(
            {
                "source_name": normalize_spaces(row.get("source_name")),
                "query": query,
                "category": category,
                "title": title,
                "title_key": compact_title_key(title),
                "price": money(price),
                "old_price": money(old_price),
                "discount_pct": pct(discount),
                "currency_id": normalize_spaces(row.get("currency_id")) or "BRL",
                "sold_quantity": to_int(row.get("sold_quantity")) or "",
                "demand_signal_type": normalize_spaces(row.get("demand_signal_type")),
                "demand_signal_value": money(to_float(row.get("demand_signal_value"))),
                "rating": parse_rating(row.get("rating_text")),
                "reviews_count": to_int(row.get("reviews_count")) or "",
                "seller_text": fix_mojibake(row.get("seller_text")),
                "shipping_text": fix_mojibake(row.get("shipping_text")),
                "item_url": clean_url(row.get("item_url")),
                "image_url": normalize_spaces(row.get("image_url")),
                "is_sponsored": to_bool(row.get("is_sponsored")),
                "is_full": to_bool(row.get("is_full")),
                "is_catalog": to_bool(row.get("is_catalog")),
                "fetched_date": normalize_spaces(row.get("fetched_date")),
                "fetched_at": row_fetched_at(row),
                "source_file": source_name,
                "source_record_id": normalize_spaces(row.get("id")),
            }
        )
    stats["market_rows_transformed"] = len(output)
    return dedupe_offers(output, stats), stats


def transform_ml_items(rows: list[dict[str, str]], source_file: Path | None) -> tuple[list[dict[str, object]], Counter]:
    stats: Counter = Counter()
    output: list[dict[str, object]] = []
    source_name = source_file.name if source_file else ""

    for row in rows:
        stats["ml_items_rows_in"] += 1
        price = to_float(row.get("price"))
        title = fix_mojibake(row.get("title"))
        if price is None or price <= 0:
            stats["ml_items_without_price_dropped"] += 1
            continue
        if not title:
            stats["ml_items_without_title_dropped"] += 1
            continue

        payload = parse_json_object(row.get("payload"))
        product_id = normalize_spaces(
            payload.get("_catalog_product_id")
            or payload.get("catalog_product_id")
            or row.get("external_id")
        )
        domain_id = normalize_spaces(payload.get("_catalog_domain_id") or payload.get("domain_id"))
        query = fix_mojibake(row.get("query"))
        output.append(
            {
                "source_name": "mercado_livre",
                "query": query,
                "category": infer_category(query, title, domain_id),
                "title": title,
                "title_key": compact_title_key(title),
                "price": money(price),
                "old_price": "",
                "discount_pct": "",
                "currency_id": normalize_spaces(row.get("currency_id")) or "BRL",
                "sold_quantity": "",
                "demand_signal_type": "",
                "demand_signal_value": "",
                "rating": "",
                "reviews_count": "",
                "seller_text": normalize_spaces(row.get("seller_id")),
                "shipping_text": "",
                "item_url": clean_url(row.get("permalink")),
                "image_url": get_first_picture(payload),
                "is_sponsored": False,
                "is_full": False,
                "is_catalog": True,
                "fetched_date": normalize_spaces(row.get("fetched_date")),
                "fetched_at": row_fetched_at(row),
                "source_file": source_name,
                "source_record_id": normalize_spaces(row.get("id") or product_id),
            }
        )
    stats["ml_items_rows_transformed"] = len(output)
    return dedupe_offers(output, stats), stats


def offer_sort_key(row: dict[str, object]) -> tuple[int, float, str]:
    sponsored_penalty = 1 if row.get("is_sponsored") else 0
    price = to_float(row.get("price")) or 0.0
    fetched_at = str(row.get("fetched_at") or "")
    return (sponsored_penalty, price, fetched_at)


def dedupe_offers(rows: list[dict[str, object]], stats: Counter) -> list[dict[str, object]]:
    best_by_key: dict[tuple[str, str, str], dict[str, object]] = {}
    for row in rows:
        item_key = str(row.get("item_url") or row.get("title_key") or "")
        key = (
            str(row.get("source_name") or ""),
            ascii_key(str(row.get("query") or "")),
            item_key,
        )
        current = best_by_key.get(key)
        if current is None or offer_sort_key(row) < offer_sort_key(current):
            best_by_key[key] = row
    stats["offer_duplicate_rows_dropped"] += len(rows) - len(best_by_key)
    return list(best_by_key.values())


def transform_ml_products(rows: list[dict[str, str]], source_file: Path | None) -> tuple[list[dict[str, object]], Counter]:
    stats: Counter = Counter()
    best_by_product: dict[str, dict[str, object]] = {}
    queries_by_product: defaultdict[str, set[str]] = defaultdict(set)
    source_name = source_file.name if source_file else ""

    for row in rows:
        stats["ml_products_rows_in"] += 1
        product_id = normalize_spaces(row.get("product_id"))
        if not product_id:
            stats["ml_products_without_id_dropped"] += 1
            continue

        query = fix_mojibake(row.get("query"))
        if query:
            queries_by_product[product_id].add(query)

        payload = parse_json_object(row.get("payload"))
        product_name = fix_mojibake(row.get("product_name") or payload.get("name"))
        domain_id = normalize_spaces(row.get("domain_id") or payload.get("domain_id"))
        candidate = {
            "product_id": product_id,
            "product_name": product_name,
            "product_name_key": compact_title_key(product_name),
            "domain_id": domain_id,
            "category": infer_category(query, product_name, domain_id),
            "status": normalize_spaces(row.get("status") or payload.get("status")),
            "brand": get_attribute(payload, "BRAND"),
            "line": get_attribute(payload, "LINE"),
            "model": get_attribute(payload, "MODEL"),
            "gtin": get_attribute(payload, "GTIN"),
            "main_color": get_attribute(payload, "MAIN_COLOR"),
            "first_image_url": get_first_picture(payload),
            "queries": "",
            "query_count": 0,
            "fetched_date": normalize_spaces(row.get("fetched_date")),
            "fetched_at": row_fetched_at(row),
            "source_file": source_name,
        }
        current = best_by_product.get(product_id)
        if current is None or str(candidate["fetched_at"]) > str(current.get("fetched_at") or ""):
            best_by_product[product_id] = candidate

    output = []
    for product_id, row in best_by_product.items():
        queries = sorted(queries_by_product.get(product_id, set()))
        row["queries"] = " | ".join(queries)
        row["query_count"] = len(queries)
        output.append(row)
    stats["ml_products_duplicate_rows_dropped"] = stats["ml_products_rows_in"] - len(output)
    stats["ml_products_rows_transformed"] = len(output)
    return output, stats


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def make_opportunities(offers: list[dict[str, object]], limit: int = 250) -> list[dict[str, object]]:
    by_query: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for offer in offers:
        price = to_float(offer.get("price"))
        query = ascii_key(str(offer.get("query") or ""))
        if price is None or not query:
            continue
        by_query[query].append(offer)

    max_demand = 1.0
    for offer in offers:
        max_demand = max(max_demand, demand_proxy(offer))

    ranked: list[dict[str, object]] = []
    for query_key, group in by_query.items():
        prices = [to_float(row.get("price")) for row in group]
        prices = [price for price in prices if price is not None and price > 0]
        if len(prices) < 5:
            continue
        query_median = median(prices)
        query_min = min(prices)
        if not query_median:
            continue
        source_count = len({str(row.get("source_name") or "") for row in group})
        offer_count = len(group)

        for offer in group:
            price = to_float(offer.get("price"))
            if price is None or price <= 0:
                continue
            price_gap = max(0.0, (query_median - price) / query_median)
            if price_gap <= 0:
                continue
            demand = demand_proxy(offer)
            demand_score = math.log1p(demand) / math.log1p(max_demand)
            source_diversity = min(source_count / 4.0, 1.0)
            confidence = source_confidence(str(offer.get("source_name") or ""))
            score = (price_gap * 70.0) + (demand_score * 15.0) + (source_diversity * 10.0) + (confidence * 5.0)
            if offer.get("is_sponsored"):
                score -= 3.0
            ranked.append(
                {
                    "rank": 0,
                    "opportunity_score": f"{max(score, 0.0):.2f}",
                    "query": offer.get("query"),
                    "category": offer.get("category"),
                    "source_name": offer.get("source_name"),
                    "title": offer.get("title"),
                    "price": offer.get("price"),
                    "query_median_price": money(query_median),
                    "query_min_price": money(query_min),
                    "query_offer_count": offer_count,
                    "query_source_count": source_count,
                    "price_gap_to_median_pct": pct(price_gap),
                    "demand_proxy": f"{demand:.2f}",
                    "confidence": f"{confidence:.2f}",
                    "item_url": offer.get("item_url"),
                    "image_url": offer.get("image_url"),
                    "fetched_at": offer.get("fetched_at"),
                }
            )

    ranked.sort(key=lambda row: float(row["opportunity_score"]), reverse=True)
    for index, row in enumerate(ranked[:limit], start=1):
        row["rank"] = index
    return ranked[:limit]


def demand_proxy(offer: dict[str, object]) -> float:
    values = [
        to_float(offer.get("sold_quantity")),
        to_float(offer.get("demand_signal_value")),
        to_float(offer.get("reviews_count")),
    ]
    return max([value for value in values if value is not None] or [0.0])


def source_confidence(source_name: str) -> float:
    return {
        "kabum": 0.95,
        "terabyte": 0.95,
        "amazon": 0.90,
        "mercado_livre": 0.85,
        "shopee": 0.75,
        "aliexpress": 0.65,
        "magalu": 0.70,
        "pichau": 0.70,
    }.get(source_name, 0.60)


def summarize_offers(offers: list[dict[str, object]]) -> dict[str, object]:
    by_source = Counter(str(row.get("source_name") or "") for row in offers)
    by_query = Counter(str(row.get("query") or "") for row in offers)
    by_category = Counter(str(row.get("category") or "") for row in offers)
    prices = [to_float(row.get("price")) for row in offers]
    prices = [price for price in prices if price is not None]
    return {
        "rows": len(offers),
        "sources": dict(by_source.most_common()),
        "queries_top": dict(by_query.most_common(20)),
        "categories": dict(by_category.most_common()),
        "min_price": money(min(prices) if prices else None),
        "median_price": money(median(prices) if prices else None),
        "max_price": money(max(prices) if prices else None),
    }


def main() -> int:
    args = parse_args()
    data_dir = args.data_dir.resolve()
    output_dir = (args.output_dir or data_dir / "processed").resolve()
    stamp = args.timestamp or datetime.now().strftime("%Y%m%d%H%M")

    market_file = latest_file(data_dir, MARKET_PATTERN)
    ml_items_file = latest_file(data_dir, ML_ITEMS_PATTERN)
    ml_products_file = latest_file(data_dir, ML_PRODUCTS_PATTERN)

    market_rows = read_rows(market_file)
    ml_item_rows = read_rows(ml_items_file)
    ml_product_rows = read_rows(ml_products_file)

    market_offers, market_stats = transform_market_offers(market_rows, market_file)
    ml_item_offers, ml_item_stats = transform_ml_items(ml_item_rows, ml_items_file)
    ml_products, ml_product_stats = transform_ml_products(ml_product_rows, ml_products_file)

    all_offers = dedupe_offers(market_offers + ml_item_offers, Counter())
    opportunities = make_opportunities(all_offers)

    offers_path = output_dir / f"offers_silver_quick_{stamp}.csv"
    ml_products_path = output_dir / f"mercado_livre_products_silver_quick_{stamp}.csv"
    opportunities_path = output_dir / f"opportunities_quick_{stamp}.csv"
    summary_path = output_dir / f"etl_summary_{stamp}.json"

    write_csv(offers_path, OFFER_FIELDS, all_offers)
    write_csv(ml_products_path, ML_PRODUCT_FIELDS, ml_products)
    write_csv(opportunities_path, OPPORTUNITY_FIELDS, opportunities)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "output_dir": str(output_dir),
        "inputs": {
            "market_web": str(market_file) if market_file else None,
            "mercado_livre_items": str(ml_items_file) if ml_items_file else None,
            "mercado_livre_products": str(ml_products_file) if ml_products_file else None,
        },
        "outputs": {
            "offers": str(offers_path),
            "mercado_livre_products": str(ml_products_path),
            "opportunities": str(opportunities_path),
            "summary": str(summary_path),
        },
        "stats": {
            **market_stats,
            **ml_item_stats,
            **ml_product_stats,
            "offers_silver_rows": len(all_offers),
            "opportunities_rows": len(opportunities),
        },
        "offers_summary": summarize_offers(all_offers),
        "top_opportunities": opportunities[:10],
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
