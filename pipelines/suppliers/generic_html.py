from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from pipelines.suppliers.base import SupplierProductSnapshot


class SupplierScraperError(RuntimeError):
    pass


def _attr_or_text(element, attr: str | None = None) -> str | None:
    if element is None:
        return None
    if attr:
        value = element.get(attr)
        return str(value).strip() if value else None
    text = element.get_text(" ", strip=True)
    return text or None


def parse_brl_price(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9,\.]", "", value)
    if not cleaned:
        return None
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\d+", value)
    return int(match.group(0)) if match else None


class GenericHtmlSupplierScraper:
    """Config-driven supplier scraper for simple static HTML catalogs."""

    def __init__(
        self,
        *,
        supplier_config: dict[str, Any],
        timeout_seconds: int = 30,
    ) -> None:
        self.supplier_config = supplier_config
        self.supplier_slug = supplier_config["slug"]
        self.urls = supplier_config.get("urls") or []
        self.selectors = supplier_config.get("selectors") or {}
        self.rate_limit_seconds = max(int(supplier_config.get("rate_limit_ms", 1500)), 0) / 1000
        self._last_request_at = 0.0
        self.client = httpx.Client(
            timeout=timeout_seconds,
            follow_redirects=True,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/122 Safari/537.36"
                ),
            },
        )

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "GenericHtmlSupplierScraper":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.rate_limit_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _extract_attr_selector(self, selector_name: str) -> tuple[str | None, str | None]:
        selector = self.selectors.get(selector_name)
        if not selector:
            return None, None
        attr = None
        match = re.search(r"@(href|src|content|data-[a-zA-Z0-9_-]+)$", selector)
        if match:
            attr = match.group(1)
            selector = selector[: match.start()].strip()
        return selector, attr

    def _extract_value(self, root, selector_name: str) -> str | None:
        selector, attr = self._extract_attr_selector(selector_name)
        if not selector:
            return None
        return _attr_or_text(root.select_one(selector), attr)

    def _parse_page(self, *, url: str, html: str) -> list[SupplierProductSnapshot]:
        product_selector = self.selectors.get("product")
        if not product_selector:
            raise SupplierScraperError("Missing supplier selector: product")
        soup = BeautifulSoup(html, "html.parser")
        snapshots: list[SupplierProductSnapshot] = []
        for product in soup.select(product_selector):
            title = self._extract_value(product, "title")
            if not title:
                continue
            raw_url = self._extract_value(product, "product_url")
            product_url = urljoin(url, raw_url) if raw_url else url
            snapshot = SupplierProductSnapshot(
                supplier_slug=self.supplier_slug,
                source_url=product_url,
                raw_title=title,
                raw_price=parse_brl_price(self._extract_value(product, "price")),
                raw_stock=parse_int(self._extract_value(product, "stock")),
                sku=self._extract_value(product, "sku"),
                ean=self._extract_value(product, "ean"),
                payload={
                    "source_url": product_url,
                    "raw_title": title,
                    "raw_price_text": self._extract_value(product, "price"),
                    "raw_stock_text": self._extract_value(product, "stock"),
                    "html_excerpt": str(product)[:2000],
                },
            )
            snapshots.append(snapshot)
        return snapshots

    def fetch(self) -> list[SupplierProductSnapshot]:
        if not self.urls:
            raise SupplierScraperError(f"No URLs configured for supplier {self.supplier_slug}")
        snapshots: list[SupplierProductSnapshot] = []
        for url in self.urls:
            self._throttle()
            response = self.client.get(url)
            if response.status_code >= 400:
                raise SupplierScraperError(f"Status {response.status_code} for {url}: {response.text[:300]}")
            snapshots.extend(self._parse_page(url=url, html=response.text))
        return snapshots

