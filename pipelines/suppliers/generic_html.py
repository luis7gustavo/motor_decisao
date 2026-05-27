from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from pipelines.suppliers.base import SupplierProductSnapshot


RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
TERMINAL_PAGINATION_STATUS_CODES = {404, 410}


class SupplierScraperError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        status_code: int | None = None,
        retryable: bool = False,
        terminal: bool = False,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.retryable = retryable
        self.terminal = terminal


def _attr_or_text(element, attr: str | None = None) -> str | None:
    if element is None:
        return None
    if attr:
        value = element.get(attr)
        return str(value).strip() if value else None
    text = element.get_text(" ", strip=True)
    return text or None


def parse_brl_price(value: str | None) -> float | None:
    """Extrai o primeiro preco BRL do texto. Ex: 'R$9,45 a vista ...' -> 9.45"""
    if not value:
        return None
    match = re.search(r"R\$\s*(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2})", value)
    if not match:
        return None
    cleaned = match.group(1).replace(".", "").replace(",", ".")
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
        self.max_retries = max(int(supplier_config.get("max_retries", 3)), 0)
        self.retry_backoff_seconds = max(float(supplier_config.get("retry_backoff_seconds", 1.0)), 0.0)
        self._last_request_at = 0.0
        self.pages_fetched = 0
        self.page_errors: list[dict[str, Any]] = []
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

    def _retry_sleep(self, *, attempt: int, retry_after: str | None = None) -> None:
        if retry_after:
            try:
                delay = min(float(retry_after), 30.0)
            except ValueError:
                delay = 0.0
        else:
            delay = min(self.retry_backoff_seconds * (2 ** max(attempt - 1, 0)), 30.0)
        if delay > 0:
            time.sleep(delay)

    def _record_page_error(self, error: SupplierScraperError, *, page_url: str, page_num: int | None) -> None:
        self.page_errors.append(
            {
                "page_url": page_url,
                "page_num": page_num,
                "status_code": error.status_code,
                "retryable": error.retryable,
                "terminal": error.terminal,
                "message": str(error)[:500],
            }
        )

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

    def _fetch_url(self, url: str) -> str:
        """Fetches a single URL with conservative retry behavior."""
        attempts = self.max_retries + 1
        for attempt in range(1, attempts + 1):
            self._throttle()
            try:
                response = self.client.get(url)
            except (httpx.TimeoutException, httpx.TransportError) as error:
                if attempt < attempts:
                    self._retry_sleep(attempt=attempt)
                    continue
                raise SupplierScraperError(
                    f"Network error after {attempts} attempts for {url}: {error}",
                    url=url,
                    retryable=True,
                ) from error

            status_code = response.status_code
            if status_code < 400:
                self.pages_fetched += 1
                return response.text

            retryable = status_code in RETRYABLE_STATUS_CODES
            terminal = status_code in TERMINAL_PAGINATION_STATUS_CODES
            if retryable and attempt < attempts:
                self._retry_sleep(attempt=attempt, retry_after=response.headers.get("Retry-After"))
                continue

            raise SupplierScraperError(
                f"Status {status_code} for {url}: {response.text[:300]}",
                url=url,
                status_code=status_code,
                retryable=retryable,
                terminal=terminal,
            )

        raise SupplierScraperError(f"Failed to fetch {url}", url=url, retryable=True)

    def _fetch_with_pagination(self, base_url: str) -> list[SupplierProductSnapshot]:
        """Fetches a base URL and follows pagination if configured."""
        pagination = self.supplier_config.get("pagination") or {}
        all_snapshots: list[SupplierProductSnapshot] = []

        html = self._fetch_url(base_url)
        page_snapshots = self._parse_page(url=base_url, html=html)
        all_snapshots.extend(page_snapshots)

        if not pagination.get("enabled") or not page_snapshots:
            return all_snapshots

        url_pattern: str = pagination.get("url_pattern", "{url}?p={page}")
        max_pages: int = int(pagination.get("max_pages", 50))
        start_page: int = int(pagination.get("start_page", 2))

        for page_num in range(start_page, max_pages + 1):
            if not page_snapshots:
                break
            page_url = url_pattern.format(url=base_url, page=page_num)
            try:
                html = self._fetch_url(page_url)
            except SupplierScraperError as error:
                self._record_page_error(error, page_url=page_url, page_num=page_num)
                if error.terminal or all_snapshots:
                    break
                raise
            page_snapshots = self._parse_page(url=page_url, html=html)
            all_snapshots.extend(page_snapshots)

        return all_snapshots

    def fetch(self) -> list[SupplierProductSnapshot]:
        if not self.urls:
            raise SupplierScraperError(f"No URLs configured for supplier {self.supplier_slug}")
        snapshots: list[SupplierProductSnapshot] = []
        for url in self.urls:
            try:
                snapshots.extend(self._fetch_with_pagination(url))
            except SupplierScraperError as error:
                self._record_page_error(error, page_url=url, page_num=None)
                if error.terminal or snapshots:
                    continue
                raise
        return snapshots
