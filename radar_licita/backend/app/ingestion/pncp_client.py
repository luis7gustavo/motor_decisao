import asyncio
import random
from collections.abc import AsyncIterator
from datetime import date
from typing import Any
from urllib.parse import urlparse

import httpx
import structlog

from app.core.config import settings

log = structlog.get_logger()


class PNCPError(RuntimeError):
    pass


class PNCPClient:
    """Client for the official PNCP read-only APIs documented by the portal."""

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(45, connect=10),
            headers={"User-Agent": "RadarLicita/0.1 (local public-procurement analysis)"},
            follow_redirects=True,
        )
        self._owns_client = client is None
        self._lock = asyncio.Lock()
        self._last_request = 0.0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        if self._owns_client:
            await self._client.aclose()

    async def _throttle(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            wait = max(0, (1 / settings.pncp_requests_per_second) - (loop.time() - self._last_request))
            if wait:
                await asyncio.sleep(wait)
            self._last_request = loop.time()

    async def _request(self, path: str, *, params: dict[str, Any] | None = None, binary: bool = False):
        base = settings.pncp_api_base_url if path.startswith("/v1/orgaos/") else settings.pncp_consulta_base_url
        url = f"{base.rstrip('/')}{path}"
        last_error: Exception | None = None
        for attempt in range(5):
            await self._throttle()
            try:
                response = await self._client.get(url, params=params)
                if response.status_code in {429, 500, 502, 503, 504}:
                    raise httpx.HTTPStatusError("temporary PNCP failure", request=response.request, response=response)
                response.raise_for_status()
                return response.content if binary else response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt == 4 or (isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 500 and exc.response.status_code != 429):
                    break
                delay = min(16, 2**attempt) + random.uniform(0, 0.4)
                log.warning("pncp_retry", url=url, attempt=attempt + 1, delay=round(delay, 2), error=str(exc))
                await asyncio.sleep(delay)
        raise PNCPError(f"PNCP request failed: {url}: {last_error}")

    async def iter_open_proposals(self, end_date: date, max_pages: int | None = None) -> AsyncIterator[dict[str, Any]]:
        page = 1
        while max_pages is None or page <= max_pages:
            payload = await self._request(
                "/v1/contratacoes/proposta",
                params={"dataFinal": end_date.strftime("%Y%m%d"), "pagina": page, "tamanhoPagina": settings.pncp_page_size},
            )
            for row in payload.get("data", []):
                yield row
            if page >= int(payload.get("totalPaginas", page)):
                break
            page += 1

    async def iter_by_period(
        self,
        endpoint: str,
        start_date: date,
        end_date: date,
        modality_id: int,
        max_pages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        if endpoint not in {"publicacao", "atualizacao"}:
            raise ValueError("Unsupported period endpoint")
        page = 1
        while max_pages is None or page <= max_pages:
            payload = await self._request(
                f"/v1/contratacoes/{endpoint}",
                params={
                    "dataInicial": start_date.strftime("%Y%m%d"),
                    "dataFinal": end_date.strftime("%Y%m%d"),
                    "codigoModalidadeContratacao": modality_id,
                    "pagina": page,
                    "tamanhoPagina": settings.pncp_page_size,
                },
            )
            for row in payload.get("data", []):
                yield row
            if page >= int(payload.get("totalPaginas", page)):
                break
            page += 1

    async def get_procurement(self, cnpj: str, year: int, sequence: int) -> dict[str, Any]:
        return await self._request(f"/v1/orgaos/{cnpj}/compras/{year}/{sequence}")

    async def get_items(self, cnpj: str, year: int, sequence: int) -> list[dict[str, Any]]:
        result = await self._request(
            f"/v1/orgaos/{cnpj}/compras/{year}/{sequence}/itens",
            params={"pagina": 1, "tamanhoPagina": 500},
        )
        return result if isinstance(result, list) else result.get("data", [])

    async def get_documents(self, cnpj: str, year: int, sequence: int) -> list[dict[str, Any]]:
        result = await self._request(
            f"/v1/orgaos/{cnpj}/compras/{year}/{sequence}/arquivos",
            params={"pagina": 1, "tamanhoPagina": 500},
        )
        return result if isinstance(result, list) else result.get("data", [])

    async def get_history(self, cnpj: str, year: int, sequence: int) -> list[dict[str, Any]]:
        result = await self._request(
            f"/v1/orgaos/{cnpj}/compras/{year}/{sequence}/historico",
            params={"pagina": 1, "tamanhoPagina": 500},
        )
        return result if isinstance(result, list) else result.get("data", [])

    async def download_document(self, source_url: str) -> bytes:
        parsed = urlparse(source_url)
        if parsed.scheme != "https" or parsed.hostname not in {"pncp.gov.br", "www.pncp.gov.br"}:
            raise PNCPError("Document URL is outside the official PNCP host")
        response = await self._client.get(source_url)
        response.raise_for_status()
        content_length = int(response.headers.get("content-length", 0))
        if content_length > settings.document_max_bytes or len(response.content) > settings.document_max_bytes:
            raise PNCPError("Document exceeds configured size limit")
        return response.content

