from __future__ import annotations

import json
import re
import time
from html import unescape
from typing import Any
from urllib.parse import urlencode, urljoin

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from pipelines.common.playwright_browser import chromium_page
from pipelines.market_web.parsing import clean_text, detect_block, parse_brl_price
from pipelines.price_history.base import PriceHistorySnapshot
from pipelines.price_history.comparison_scraper import SOURCE_BASE_URLS


class ComparisonWebHistoryError(RuntimeError):
    pass


class ComparisonWebHistoryScraper:
    """Browser-backed Buscape/Zoom scraper for product price-history pages.

    It captures search hits, opens product pages and stores the raw page state
    needed to evolve the historical parser without recollecting the page.
    """

    def __init__(
        self,
        *,
        source_name: str,
        headless: bool = True,
        navigation_timeout_seconds: int = 45,
        action_timeout_seconds: int = 15,
        scroll_steps: int = 5,
        scroll_pause_ms: int = 900,
        rate_limit_ms: int = 1500,
        max_body_chars: int = 50000,
    ) -> None:
        if source_name not in SOURCE_BASE_URLS:
            raise ValueError(f"Unsupported comparison source: {source_name}")
        self.source_name = source_name
        self.base_url = SOURCE_BASE_URLS[source_name]
        self.headless = headless
        self.navigation_timeout_seconds = navigation_timeout_seconds
        self.action_timeout_seconds = action_timeout_seconds
        self.scroll_steps = scroll_steps
        self.scroll_pause_seconds = max(scroll_pause_ms, 0) / 1000
        self.rate_limit_seconds = max(rate_limit_ms, 0) / 1000
        self.max_body_chars = max_body_chars
        self._last_request_at = 0.0
        self._page_context = None
        self._page: Page | None = None

    def __enter__(self) -> "ComparisonWebHistoryScraper":
        self._page_context = chromium_page(
            headless=self.headless,
            navigation_timeout_seconds=self.navigation_timeout_seconds,
            action_timeout_seconds=self.action_timeout_seconds,
        )
        self._page = self._page_context.__enter__()
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._page_context is not None:
            self._page_context.__exit__(*_exc)
        self._page_context = None
        self._page = None

    @property
    def page(self) -> Page:
        if self._page is None:
            raise RuntimeError("ComparisonWebHistoryScraper must be used as a context manager")
        return self._page

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.rate_limit_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _build_search_url(self, query: str) -> str:
        return f"{self.base_url}/search?{urlencode({'q': query})}"

    def _goto(self, url: str) -> tuple[str, str, str | None]:
        self._throttle()
        page = self.page
        try:
            page.goto(url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except PlaywrightTimeoutError:
                pass
            for _ in range(self.scroll_steps):
                page.mouse.wheel(0, 1000)
                page.wait_for_timeout(int(self.scroll_pause_seconds * 1000))
            body_text = page.locator("body").inner_text(timeout=8000)
            block_reason = detect_block(body_text[:5000])
            return page.content(), body_text, block_reason
        except PlaywrightTimeoutError as error:
            return "", "", f"navigation_timeout: {error}"

    def _extract_search_hits(self, html: str) -> list[dict[str, Any]]:
        next_data = _extract_next_data(html)
        state = next_data.get("props", {}).get("initialReduxState", {})
        hits = state.get("hits", {}).get("hits", [])
        if not isinstance(hits, list):
            raise ComparisonWebHistoryError("Search hits are not a list")
        return [hit for hit in hits if isinstance(hit, dict)]

    def fetch(
        self,
        *,
        query: str,
        max_results: int = 30,
        product_detail_limit: int | None = None,
    ) -> list[PriceHistorySnapshot]:
        search_url = self._build_search_url(query)
        html = ""
        body_text = ""
        block_reason = None
        for attempt in range(1, 4):
            html, body_text, block_reason = self._goto(search_url)
            if not block_reason:
                break
            if attempt < 3 and _is_retryable_block_reason(block_reason):
                # 2026-05-01: retry transient anti-bot/search timeouts before giving up on the query.
                _sleep_before_retry(attempt)
                continue
            return [
                _blocked_snapshot(
                    self.source_name,
                    query,
                    search_url,
                    block_reason,
                    body_text,
                    attempt=attempt,
                )
            ]

        hits = self._extract_search_hits(html)
        limit = min(max_results, product_detail_limit or max_results)
        snapshots: list[PriceHistorySnapshot] = []
        for position, hit in enumerate(hits[:limit], start=1):
            snapshot = self._snapshot_from_hit_detail(hit, query=query, position=position)
            if snapshot is not None:
                snapshots.append(snapshot)
        return snapshots

    def _snapshot_from_hit_detail(
        self,
        hit: dict[str, Any],
        *,
        query: str,
        position: int,
    ) -> PriceHistorySnapshot | None:
        product_path = hit.get("url")
        title = clean_text(hit.get("name") or hit.get("shortName"))
        current_price = _as_float(hit.get("price"))
        if not product_path and not title:
            return None

        product_url = urljoin(self.base_url, str(product_path)) if product_path else None
        product_key = str(hit.get("objectId") or hit.get("sourceId") or product_url or title)
        if not product_url:
            return PriceHistorySnapshot(
                source_name=self.source_name,
                product_key=product_key,
                product_url=None,
                title=title,
                current_price=current_price,
                min_price=current_price,
                median_price=None,
                max_price=current_price,
                history_window_days=None,
                payload={"query": query, "position": position, "search_hit": hit},
                avg_price=None,
                query=query,
                source_mode="product_history_web",
            )

        html = ""
        body_text = ""
        block_reason = None
        for attempt in range(1, 4):
            html, body_text, block_reason = self._goto(product_url)
            if not block_reason:
                break
            if attempt < 3 and _is_retryable_block_reason(block_reason):
                # 2026-05-01: product pages on Buscape/Zoom sometimes recover on a second open.
                _sleep_before_retry(attempt)
                continue
            return _blocked_snapshot(
                self.source_name,
                query,
                product_url,
                block_reason,
                body_text,
                attempt=attempt,
            )

        detail = _extract_product_detail(
            html=html,
            body_text=body_text,
            source_name=self.source_name,
            max_body_chars=self.max_body_chars,
        )
        history = detail.get("history_summary") or {}
        schema = detail.get("schema_summary") or {}
        offers = schema.get("offers") or []

        extracted_title = clean_text(schema.get("name") or detail.get("title") or title)
        aggregate = schema.get("aggregate_offer") or {}
        low_price = _as_float(aggregate.get("lowPrice"))
        high_price = _as_float(aggregate.get("highPrice"))
        current = _first_number(
            [
                current_price,
                low_price,
                parse_brl_price(detail.get("lowest_price_text")),
                _first_offer_price(offers),
            ]
        )
        avg_price = _as_float(history.get("avg_price"))

        payload = {
            "query": query,
            "position": position,
            "search_hit": hit,
            "product_page": detail,
        }
        return PriceHistorySnapshot(
            source_name=self.source_name,
            product_key=product_key,
            product_url=product_url,
            title=extracted_title,
            current_price=current,
            min_price=_first_number([low_price, current]),
            median_price=None,
            max_price=_first_number([high_price, current]),
            history_window_days=history.get("window_days"),
            payload=payload,
            avg_price=avg_price,
            query=query,
            source_mode="product_history_web",
            blocked=False,
            block_reason=None,
        )


def _extract_next_data(html: str) -> dict[str, Any]:
    match = re.search(
        # 2026-05-02: Buscape started varying the script tag attributes/order,
        # so the parser must match __NEXT_DATA__ even when nonce/data attrs move.
        r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    if not match:
        raise ComparisonWebHistoryError("__NEXT_DATA__ not found")
    data = json.loads(unescape(match.group(1)))
    if not isinstance(data, dict):
        raise ComparisonWebHistoryError("__NEXT_DATA__ is not a JSON object")
    return data


def _extract_product_detail(
    *,
    html: str,
    body_text: str,
    source_name: str,
    max_body_chars: int,
) -> dict[str, Any]:
    next_data: dict[str, Any] | None
    try:
        next_data = _extract_next_data(html)
    except Exception:
        next_data = None

    json_ld = _extract_json_ld(html)
    schema_summary = _summarize_json_ld(json_ld)
    history_context = _extract_history_context(body_text)
    history_summary = _parse_history_summary(history_context)
    lowest_price_text = _extract_lowest_price_text(body_text)
    title = _extract_title(html)

    return {
        "source_name": source_name,
        "title": title,
        "history_context": history_context,
        "history_summary": history_summary,
        "lowest_price_text": lowest_price_text,
        "schema_summary": schema_summary,
        "json_ld": json_ld,
        "next_data": next_data,
        "body_text": body_text[:max_body_chars],
    }


def _extract_json_ld(html: str) -> list[Any]:
    records: list[Any] = []
    for match in re.finditer(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html,
        flags=re.DOTALL,
    ):
        try:
            records.append(json.loads(unescape(match.group(1))))
        except json.JSONDecodeError:
            continue
    return records


def _summarize_json_ld(records: list[Any]) -> dict[str, Any]:
    product: dict[str, Any] | None = None
    for record in records:
        candidates = []
        if isinstance(record, dict):
            graph = record.get("@graph")
            if isinstance(graph, list):
                candidates.extend(item for item in graph if isinstance(item, dict))
            candidates.append(record)
        for candidate in candidates:
            candidate_type = candidate.get("@type")
            if candidate_type == "Product" or (
                isinstance(candidate_type, list) and "Product" in candidate_type
            ):
                product = candidate
                break
        if product:
            break

    if not product:
        return {}

    offers_raw = product.get("offers")
    aggregate_offer: dict[str, Any] = {}
    offers: list[dict[str, Any]] = []
    if isinstance(offers_raw, dict):
        if offers_raw.get("@type") == "AggregateOffer":
            aggregate_offer = {
                "lowPrice": offers_raw.get("lowPrice"),
                "highPrice": offers_raw.get("highPrice"),
                "offerCount": offers_raw.get("offerCount"),
            }
            raw_offers = offers_raw.get("offers")
            if isinstance(raw_offers, list):
                offers = [_compact_offer(offer) for offer in raw_offers if isinstance(offer, dict)]
        else:
            offers = [_compact_offer(offers_raw)]
    elif isinstance(offers_raw, list):
        offers = [_compact_offer(offer) for offer in offers_raw if isinstance(offer, dict)]

    return {
        "name": product.get("name"),
        "brand": product.get("brand"),
        "sku": product.get("sku"),
        "aggregate_rating": product.get("aggregateRating"),
        "aggregate_offer": aggregate_offer,
        "offers": offers,
        "review_count": len(product.get("review") or []) if isinstance(product.get("review"), list) else None,
    }


def _compact_offer(offer: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": offer.get("id"),
        "name": offer.get("name"),
        "offeredBy": offer.get("offeredBy"),
        "priceCurrency": offer.get("priceCurrency"),
        "price": offer.get("price"),
        "url": offer.get("url"),
    }


def _extract_history_context(body_text: str) -> str | None:
    lowered = body_text.lower()
    anchors = ["histórico de preços", "historico de precos", "com base nos últimos", "com base nos ultimos"]
    indexes = [lowered.find(anchor) for anchor in anchors if lowered.find(anchor) >= 0]
    if not indexes:
        return None
    start = max(0, min(indexes) - 120)
    end = min(len(body_text), min(indexes) + 900)
    return clean_text(body_text[start:end])


def _parse_history_summary(history_context: str | None) -> dict[str, Any]:
    if not history_context:
        return {}
    normalized = history_context.replace("\xa0", " ")
    window_days: int | None = None
    avg_price: float | None = None
    match = re.search(
        r"(?:últimos|ultimos)\s+(\d+)\s+dias.*?m[eé]dia\s+de\s+(R\$\s*[\d.,]+)",
        normalized,
        flags=re.I,
    )
    if match:
        window_days = int(match.group(1))
        avg_price = parse_brl_price(match.group(2))
    price_status = None
    status_match = re.search(r"O preço está\s+([A-Za-zÀ-ÿ ]+)", normalized, flags=re.I)
    if status_match:
        price_status = clean_text(status_match.group(1).split(" Com base")[0])
    return {
        "window_days": window_days,
        "avg_price": avg_price,
        "price_status": price_status,
        "raw_text": history_context,
    }


def _extract_lowest_price_text(body_text: str) -> str | None:
    match = re.search(
        r"O menor preço encontrado.*?atualmente é\s+(R\$\s*[\d.,]+)",
        body_text,
        flags=re.I | re.S,
    )
    if match:
        return clean_text(match.group(1))
    return None


def _extract_title(html: str) -> str | None:
    match = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
    if not match:
        return None
    title = clean_text(unescape(re.sub(r"<.*?>", "", match.group(1))))
    if not title:
        return None
    return re.split(r"\s+(?:em Promoção|com o Melhor Preço|é no)", title, maxsplit=1)[0]


def _blocked_snapshot(
    source_name: str,
    query: str,
    url: str,
    block_reason: str,
    body_text: str,
    *,
    attempt: int = 1,
) -> PriceHistorySnapshot:
    return PriceHistorySnapshot(
        source_name=source_name,
        product_key=url,
        product_url=url,
        title=None,
        current_price=None,
        min_price=None,
        median_price=None,
        max_price=None,
        history_window_days=None,
        payload={
            "query": query,
            "url": url,
            "block_reason": block_reason,
            "attempt": attempt,
            "body_text": body_text[:5000],
        },
        avg_price=None,
        query=query,
        source_mode="product_history_web",
        blocked=True,
        block_reason=block_reason,
    )


def _is_retryable_block_reason(reason: str | None) -> bool:
    if not reason:
        return False
    return reason in {"captcha", "bot_block", "access_denied"} or reason.startswith(
        "navigation_timeout"
    )


def _sleep_before_retry(attempt: int) -> None:
    time.sleep(min(4 * attempt, 12))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return parse_brl_price(value)


def _first_number(values: list[Any]) -> float | None:
    for value in values:
        number = _as_float(value)
        if number is not None:
            return number
    return None


def _first_offer_price(offers: list[dict[str, Any]]) -> float | None:
    for offer in offers:
        price = _as_float(offer.get("price"))
        if price is not None:
            return price
    return None
