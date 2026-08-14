from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from parsel import Selector

from pipelines.suppliers.base import SupplierProductSnapshot
from pipelines.suppliers.generic_html import parse_brl_price, parse_int


def _selector_parts(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None
    match = re.search(r"@(href|src|content|data-[a-zA-Z0-9_-]+)$", value)
    if not match:
        return value, None
    return value[: match.start()].strip(), match.group(1)


def _extract(node: Selector, selector_value: str | None) -> str | None:
    css, attr = _selector_parts(selector_value)
    if not css and attr:
        value = node.attrib.get(attr)
        cleaned = " ".join(str(value).split()) if value else ""
        return cleaned or None
    if not css:
        return None
    selected = node.css(css)
    if not selected:
        return None
    if attr:
        value = selected.attrib.get(attr)
    else:
        value = " ".join(selected.css("::text").getall())
    cleaned = " ".join(str(value).split()) if value else ""
    return cleaned or None


def parse_supplier_products(
    html: str,
    *,
    supplier_slug: str,
    page_url: str,
    selectors: dict[str, Any],
) -> list[SupplierProductSnapshot]:
    """Extrai um catalogo HTML sem executar rede ou persistencia."""
    product_selector = selectors.get("product")
    if not product_selector:
        raise ValueError("Missing supplier selector: product")
    document = Selector(text=html)
    snapshots: list[SupplierProductSnapshot] = []
    for product in document.css(str(product_selector)):
        title = _extract(product, selectors.get("title"))
        if not title:
            continue
        raw_url = _extract(product, selectors.get("product_url"))
        product_url = urljoin(page_url, raw_url) if raw_url else page_url
        price_text = _extract(product, selectors.get("price"))
        stock_text = _extract(product, selectors.get("stock"))
        snapshots.append(
            SupplierProductSnapshot(
                supplier_slug=supplier_slug,
                source_url=product_url,
                raw_title=title,
                raw_price=parse_brl_price(price_text),
                raw_stock=parse_int(stock_text),
                sku=_extract(product, selectors.get("sku")),
                ean=_extract(product, selectors.get("ean")),
                payload={
                    "source_url": product_url,
                    "raw_title": title,
                    "raw_price_text": price_text,
                    "raw_stock_text": stock_text,
                    "html_excerpt": product.get()[:5000],
                },
            )
        )
    return snapshots
