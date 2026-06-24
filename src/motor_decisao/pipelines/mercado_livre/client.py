from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx


class MercadoLivreApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class MercadoLivreSearchPage:
    results: list[dict[str, Any]]
    total: int | None
    offset: int
    limit: int
    raw: dict[str, Any]


class MercadoLivreClient:
    def __init__(
        self,
        *,
        api_base: str,
        site_id: str,
        timeout_seconds: int = 20,
        max_retries: int = 3,
        rate_limit_ms: int = 250,
        access_token: str | None = None,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.site_id = site_id
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.rate_limit_seconds = max(rate_limit_ms, 0) / 1000
        self._last_request_at = 0.0
        headers = {
            "Accept": "application/json",
            "User-Agent": "motor-decisao-compra/0.1",
        }
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        self.client = httpx.Client(timeout=timeout_seconds, headers=headers)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "MercadoLivreClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.rate_limit_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.api_base}{path}"
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                response = self.client.get(url, params=params)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise MercadoLivreApiError(
                        f"Retryable status {response.status_code}: {response.text[:300]}"
                    )
                if response.status_code >= 400:
                    raise MercadoLivreApiError(
                        f"Status {response.status_code}: {response.text[:500]}"
                    )
                data = response.json()
                if not isinstance(data, dict):
                    raise MercadoLivreApiError("Expected JSON object from Mercado Livre")
                return data
            except (httpx.HTTPError, MercadoLivreApiError) as error:
                last_error = error
                if attempt >= self.max_retries:
                    break
                time.sleep(min(2**attempt, 8))

        raise MercadoLivreApiError(str(last_error))

    def search_items(
        self,
        *,
        category_id: str | None = None,
        query: str | None = None,
        offset: int = 0,
        limit: int = 50,
        sort: str | None = "sold_quantity_desc",
    ) -> MercadoLivreSearchPage:
        safe_limit = min(max(limit, 1), 50)
        params: dict[str, Any] = {
            "offset": offset,
            "limit": safe_limit,
        }
        if category_id:
            params["category"] = category_id
        if query:
            params["q"] = query
        if sort:
            params["sort"] = sort

        raw = self._get(f"/sites/{self.site_id}/search", params=params)
        paging = raw.get("paging") or {}
        results = raw.get("results") or []
        if not isinstance(results, list):
            raise MercadoLivreApiError("Expected 'results' list from Mercado Livre")

        return MercadoLivreSearchPage(
            results=results,
            total=paging.get("total"),
            offset=int(paging.get("offset", offset)),
            limit=int(paging.get("limit", safe_limit)),
            raw=raw,
        )

    def search_catalog_products(
        self,
        *,
        query: str,
        offset: int = 0,
        limit: int = 50,
        status: str | None = "active",
    ) -> MercadoLivreSearchPage:
        safe_limit = min(max(limit, 1), 50)
        params: dict[str, Any] = {
            "site_id": self.site_id,
            "q": query,
            "offset": offset,
            "limit": safe_limit,
        }
        if status:
            params["status"] = status

        raw = self._get("/products/search", params=params)
        paging = raw.get("paging") or {}
        results = raw.get("results") or []
        if not isinstance(results, list):
            raise MercadoLivreApiError("Expected 'results' list from Mercado Livre")

        return MercadoLivreSearchPage(
            results=results,
            total=paging.get("total"),
            offset=int(paging.get("offset", offset)),
            limit=int(paging.get("limit", safe_limit)),
            raw=raw,
        )

    def search_product_items(
        self,
        *,
        product_id: str,
        offset: int = 0,
        limit: int = 50,
    ) -> MercadoLivreSearchPage:
        safe_limit = min(max(limit, 1), 100)
        params: dict[str, Any] = {
            "offset": offset,
            "limit": safe_limit,
        }

        try:
            raw = self._get(f"/products/{product_id}/items", params=params)
        except MercadoLivreApiError as error:
            if "Status 404" in str(error) and "No winners found" in str(error):
                return MercadoLivreSearchPage(
                    results=[],
                    total=0,
                    offset=offset,
                    limit=safe_limit,
                    raw={"message": "No winners found", "product_id": product_id},
                )
            raise

        paging = raw.get("paging") or {}
        results = raw.get("results") or []
        if not isinstance(results, list):
            raise MercadoLivreApiError("Expected 'results' list from Mercado Livre")

        return MercadoLivreSearchPage(
            results=results,
            total=paging.get("total"),
            offset=int(paging.get("offset", offset)),
            limit=int(paging.get("limit", safe_limit)),
            raw=raw,
        )
