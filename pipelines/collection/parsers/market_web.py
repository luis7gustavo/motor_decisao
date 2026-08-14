from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from parsel import Selector

from pipelines.market_web.base import MarketListingSnapshot
from pipelines.market_web.parsing import clean_text, parse_int_text, parse_sold_quantity
from pipelines.market_web.sources import (
    SourceConfig,
    _clean_title_candidate,
    _contains,
    _extract_line,
    _extract_market_price,
    _extract_reviews_text,
    _extract_sold_text,
    _guess_title_from_text,
)


def _css_first(node: Selector, css: str | None, *, attr: str | None = None) -> str | None:
    if not css:
        return None
    selected = node.css(css)
    if attr:
        return clean_text(selected.attrib.get(attr)) if selected else None
    return clean_text(" ".join(selected.css("::text").getall()))


def _self_or_descendant_href(card: Selector, config: SourceConfig) -> str | None:
    href = card.attrib.get("href")
    if href and "self" in config.link_selector:
        return href
    selectors = [item.strip() for item in config.link_selector.split(",")]
    for selector in selectors:
        if selector in {"self", "closest"}:
            continue
        value = card.css(f"{selector}::attr(href)").get()
        if value:
            return value
    return href


def parse_market_listings(
    html: str,
    *,
    config: SourceConfig,
    query: str,
    page_url: str,
    max_results: int,
) -> list[MarketListingSnapshot]:
    """Parser puro, compartilhado por HTTP renderizado e browser."""
    selector = Selector(text=html)
    snapshots: list[MarketListingSnapshot] = []
    seen: set[str] = set()
    for card in selector.css(config.card_selector):
        text = clean_text(" ".join(card.css("::text").getall()))
        if not text:
            continue
        raw_href = _self_or_descendant_href(card, config)
        item_url = urljoin(page_url, raw_href) if raw_href else None
        key = item_url or text[:180]
        if key in seen:
            continue
        seen.add(key)

        title_text = _css_first(card, config.title_selector)
        if not title_text and config.source_name in {"kabum", "pichau", "terabyte"}:
            title_text = _css_first(card, "strong, h2, h3")
        price_text = _css_first(card, config.price_selector)
        title = _clean_title_candidate(title_text, text) or _guess_title_from_text(text)
        price = _extract_market_price(price_text, text, config.source_name)
        if not title and price is None:
            continue
        sold_text = _extract_sold_text(text) or _extract_line(text, ["vend", "sold"])
        reviews_text = _extract_reviews_text(text) or _extract_line(text, ["avalia", "review"])
        sold_quantity = parse_sold_quantity(sold_text)
        reviews_count = parse_int_text(reviews_text)
        review_match = re.search(r"(\d[\d.,]*)\s+(?:avalia[cç][oõ]es|reviews?)", text, flags=re.I)
        if review_match:
            reviews_count = parse_int_text(review_match.group(1))
        demand_value = sold_quantity if sold_quantity is not None else reviews_count
        image_url = None
        if config.image_selector:
            image_node = card.css(config.image_selector)
            raw_image = None
            if image_node:
                raw_image = image_node.attrib.get("src") or image_node.attrib.get("data-src")
            image_url = urljoin(page_url, raw_image) if raw_image else None

        payload: dict[str, Any] = {
            "text": text,
            "title_text": title_text,
            "price_text": price_text,
            "href": raw_href,
            "html": card.get()[:5000],
        }
        snapshots.append(
            MarketListingSnapshot(
                source_name=config.source_name,
                source_role=config.source_role,
                query=query,
                position=len(snapshots) + 1,
                title=title,
                price=price,
                old_price=None,
                currency_id="BRL" if price is not None else None,
                sold_quantity_text=sold_text,
                sold_quantity=sold_quantity,
                demand_signal_type=config.demand_signal_type,
                demand_signal_value=float(demand_value) if demand_value is not None else None,
                bsr_text=_extract_line(text, ["mais vendido", "best sellers", "ranking"]),
                rating_text=_extract_line(text, ["estrela", "rating", "classifica"]),
                reviews_count=reviews_count,
                seller_text=_extract_line(text, ["vendido por", "loja", "seller"]),
                shipping_text=_extract_line(text, ["frete", "envio", "full", "prime"]),
                installments_text=_extract_line(text, ["x de", "sem juros", "parcel"]),
                item_url=item_url,
                image_url=image_url,
                is_sponsored=_contains(text, ["patrocinado", "sponsored"]),
                is_full=_contains(text, ["full", "prime"]),
                is_catalog=None,
                blocked=False,
                block_reason=None,
                payload=payload,
            )
        )
        if len(snapshots) >= max_results:
            break
    return snapshots
