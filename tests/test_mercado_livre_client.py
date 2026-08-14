from __future__ import annotations

import httpx
import pytest

from pipelines.mercado_livre.client import MercadoLivreApiError, MercadoLivreClient


def test_unauthorized_response_is_not_retried() -> None:
    def unauthorized(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "invalid access token"})

    client = MercadoLivreClient(
        api_base="https://api.example.test",
        site_id="MLB",
        max_retries=3,
        rate_limit_ms=0,
    )
    client.client.close()
    client.client = httpx.Client(transport=httpx.MockTransport(unauthorized))

    with client, pytest.raises(MercadoLivreApiError) as captured:
        client.search_items(query="mouse", limit=1)

    assert captured.value.status_code == 401
    assert not captured.value.retryable
    assert client.requests_total == 1
    assert client.retry_count == 0


def test_unauthorized_response_refreshes_token_once() -> None:
    authorization_headers: list[str | None] = []

    def refresh_then_authorize(request: httpx.Request) -> httpx.Response:
        authorization_headers.append(request.headers.get("authorization"))
        if request.headers.get("authorization") == "Bearer renewed-token":
            return httpx.Response(
                200,
                json={"paging": {"total": 1, "offset": 0, "limit": 1}, "results": [{"id": "MLB1"}]},
            )
        return httpx.Response(401, json={"message": "invalid access token"})

    refresh_calls: list[bool] = []
    client = MercadoLivreClient(
        api_base="https://api.example.test",
        site_id="MLB",
        max_retries=1,
        rate_limit_ms=0,
        access_token="expired-token",
        refresh_access_token=lambda: refresh_calls.append(True) or "renewed-token",
    )
    client.client.close()
    client.client = httpx.Client(
        transport=httpx.MockTransport(refresh_then_authorize),
        headers={"Authorization": "Bearer expired-token"},
    )

    with client:
        page = client.search_items(query="mouse", limit=1)

    assert page.results == [{"id": "MLB1"}]
    assert refresh_calls == [True]
    assert authorization_headers == ["Bearer expired-token", "Bearer renewed-token"]
    assert client.requests_total == 2
    assert client.requests_success == 1
    assert client.requests_failed == 1
    assert client.retry_count == 1
