from __future__ import annotations

import json
import re
import time
from html import unescape
from typing import Any
from urllib.parse import urlencode, urljoin

import httpx

from pipelines.price_history.base import PriceHistorySnapshot


class ComparisonSearchError(RuntimeError):
    pass


SOURCE_BASE_URLS = {
    "buscape": "https://www.buscape.com.br",
    "zoom": "https://www.zoom.com.br",
}


class ComparisonSearchScraper:
    """Conservative HTML extractor for Buscape/Zoom search result pages.

    This source captures current comparison prices from the rendered Next.js
    state. It does not bypass login, CAPTCHA or blocking.
    """

    def __init__(
        self,
        *,
        source_name: str,
        timeout_seconds: int = 30,
        rate_limit_ms: int = 1000,
    ) -> None:
        if source_name not in SOURCE_BASE_URLS:
            raise ValueError(f"Unsupported comparison source: {source_name}")
        self.source_name = source_name
        self.base_url = SOURCE_BASE_URLS[source_name]
        self.timeout_seconds = timeout_seconds
        self.rate_limit_seconds = max(rate_limit_ms, 0) / 1000
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

    def __enter__(self) -> "ComparisonSearchScraper":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.rate_limit_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _build_search_url(self, query: str) -> str:
        return f"{self.base_url}/search?{urlencode({'q': query})}"

    def _extract_next_data(self, html: str) -> dict[str, Any]:
        match = re.search(
            # 2026-05-02: Buscape/Zoom can change attribute order on the Next.js
            # payload script without changing the actual data contract.
            r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html,
            flags=re.DOTALL,
        )
        if not match:
            raise ComparisonSearchError("__NEXT_DATA__ not found in search page")
        data = json.loads(unescape(match.group(1)))
        if not isinstance(data, dict):
            raise ComparisonSearchError("__NEXT_DATA__ is not a JSON object")
        return data

    def _extract_hits(self, next_data: dict[str, Any]) -> list[dict[str, Any]]:
        state = next_data.get("props", {}).get("initialReduxState", {})
        hits = state.get("hits", {}).get("hits", [])
        if not isinstance(hits, list):
            raise ComparisonSearchError("Search hits are not a list")
        return [hit for hit in hits if isinstance(hit, dict)]

    def _snapshot_from_hit(
        self,
        hit: dict[str, Any],
        *,
        query: str | None = None,
        source_mode: str = "search_current",
    ) -> PriceHistorySnapshot | None:
        title = hit.get("name") or hit.get("shortName")
        product_path = hit.get("url")
        price = hit.get("price")
        if not title or price is None:
            return None
        product_url = urljoin(self.base_url, product_path) if product_path else None
        product_key = str(hit.get("objectId") or hit.get("sourceId") or product_url or title)
        return PriceHistorySnapshot(
            source_name=self.source_name,
            product_key=product_key,
            product_url=product_url,
            title=title,
            current_price=float(price),
            min_price=float(price),
            median_price=None,
            max_price=float(price),
            history_window_days=None,
            payload=hit,
            avg_price=None,
            query=query,
            source_mode=source_mode,
        )

    def fetch(
        self,
        *,
        query: str | None = None,
        product_url: str | None = None,
        max_results: int = 30,
    ) -> list[PriceHistorySnapshot]:
        if not query:
            raise ValueError("query is required for comparison search")
        self._throttle()
        response = self.client.get(self._build_search_url(query))
        if response.status_code >= 400:
            raise ComparisonSearchError(f"Status {response.status_code}: {response.text[:300]}")

        next_data = self._extract_next_data(response.text)
        snapshots: list[PriceHistorySnapshot] = []
        for hit in self._extract_hits(next_data):
            snapshot = self._snapshot_from_hit(hit, query=query)
            if snapshot is not None:
                snapshots.append(snapshot)
            if len(snapshots) >= max_results:
                break
        return snapshots
