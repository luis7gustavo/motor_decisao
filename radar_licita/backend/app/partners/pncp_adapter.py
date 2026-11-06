from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
import time
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import httpx

from app.core.config import settings


class PartnerSourceError(RuntimeError):
    def __init__(self, message: str, *, kind: str = "unavailable"):
        super().__init__(message)
        self.kind = kind


def payload_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class PNCPPartnerAdapter:
    """Small, read-only adapter for the public PNCP publication/items/results chain."""

    def __init__(self, client: httpx.AsyncClient | None = None, *, max_requests: int = 30, max_bytes: int = 2 * 1024 * 1024, deadline_seconds: float = 90.0):
        self.client = client or httpx.AsyncClient(timeout=httpx.Timeout(30, connect=10), follow_redirects=True, headers={"User-Agent": "RadarLicita/0.1 (local public-procurement analysis)"})
        self._owns_client = client is None
        self.max_requests = max_requests
        self.max_bytes = max_bytes
        self.deadline_seconds = deadline_seconds
        self.request_count = 0
        self.started = time.monotonic()
        self._last_request = 0.0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        if self._owns_client:
            await self.client.aclose()

    async def _get(self, base: str, path: str, params: dict[str, Any] | None = None) -> Any:
        if self.request_count >= self.max_requests or time.monotonic() - self.started >= self.deadline_seconds:
            raise PartnerSourceError("Limite local da busca atingido", kind="partial")
        wait = 1 / max(settings.pncp_requests_per_second, 1)
        elapsed = time.monotonic() - self._last_request
        if elapsed < wait:
            await asyncio.sleep(wait - elapsed)
        url = f"{base.rstrip('/')}{path}"
        last_error: Exception | None = None
        for attempt in range(3):
            self.request_count += 1
            self._last_request = time.monotonic()
            try:
                response = await self.client.get(url, params=params)
                if response.status_code in {429, 500, 502, 503, 504}:
                    if response.status_code == 429 and attempt == 2:
                        raise PartnerSourceError("Fonte retornou 429 após as tentativas locais", kind="unavailable")
                    retry_after = response.headers.get("Retry-After")
                    delay = min(float(retry_after), 16) if retry_after and retry_after.replace('.', '', 1).isdigit() else min(2**attempt, 8) + random.uniform(0, .2)
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                if len(response.content) > self.max_bytes:
                    raise PartnerSourceError("Resposta excede o limite local de bytes", kind="partial")
                return response.json()
            except PartnerSourceError:
                raise
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(min(2**attempt, 8))
        raise PartnerSourceError(f"Falha na fonte pública: {last_error}", kind="unavailable")

    @staticmethod
    def _publication_identity(row: dict[str, Any]) -> tuple[str | None, int | None, int | None]:
        org = row.get("orgaoEntidade") or row.get("orgao") or {}
        cnpj = row.get("cnpjOrgao") or row.get("cnpj") or org.get("cnpj")
        year = row.get("anoCompra") or row.get("ano")
        sequence = row.get("sequencialCompra") or row.get("sequencial")
        return (str(cnpj) if cnpj else None, int(year) if year is not None else None, int(sequence) if sequence is not None else None)

    async def open_proposal_rows(self, end: date, *, uf: str | None = None, max_pages: int = 2) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            params = {"dataFinal": end.strftime("%Y%m%d"), "codigoModalidadeContratacao": 6,
                      "pagina": page, "tamanhoPagina": min(settings.pncp_page_size, 50)}
            if uf:
                params["uf"] = uf.upper()
            payload = await self._get(settings.pncp_consulta_base_url, "/v1/contratacoes/proposta", params)
            rows.extend(payload.get("data", []) or [])
            if page >= int(payload.get("totalPaginas", page)):
                break
        return rows

    async def publication_rows(self, start: date, end: date, *, modality_id: int = 6, max_pages: int = 2, uf: str | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            params = {"dataInicial": start.strftime("%Y%m%d"), "dataFinal": end.strftime("%Y%m%d"), "codigoModalidadeContratacao": modality_id, "pagina": page, "tamanhoPagina": min(settings.pncp_page_size, 50)}
            if uf:
                params["uf"] = uf.upper()
            payload = await self._get(settings.pncp_consulta_base_url, "/v1/contratacoes/publicacao", params)
            page_rows = payload.get("data", []) if isinstance(payload, dict) else payload
            rows.extend(page_rows or [])
            if not isinstance(payload, dict) or page >= int(payload.get("totalPaginas", page)):
                break
        return rows

    async def items(self, cnpj: str, year: int, sequence: int) -> list[dict[str, Any]]:
        path = f"/v1/orgaos/{quote(cnpj, safe='')}/compras/{year}/{sequence}/itens"
        payload = await self._get(settings.pncp_api_base_url, path, {"pagina": 1, "tamanhoPagina": 500})
        return payload if isinstance(payload, list) else payload.get("data", [])

    async def results(self, cnpj: str, year: int, sequence: int, item_number: int) -> list[dict[str, Any]]:
        path = f"/v1/orgaos/{quote(cnpj, safe='')}/compras/{year}/{sequence}/itens/{item_number}/resultados"
        payload = await self._get(settings.pncp_api_base_url, path)
        return payload if isinstance(payload, list) else payload.get("data", [])

    async def inspect_purchase(self, cnpj: str, year: int, sequence: int, item_number: int) -> dict[str, Any]:
        items = await self.items(cnpj, year, sequence)
        selected = next((row for row in items if int(row.get("numeroItem", -1)) == item_number), None)
        if selected is None:
            return {"item": None, "results": [], "request_count": self.request_count}
        return {"item": selected, "results": await self.results(cnpj, year, sequence, item_number), "request_count": self.request_count}

    async def search(self, query: str | None, uf: str | None, start: date, end: date, max_results: int, *, target_item_number: int | None = None) -> dict[str, Any]:
        terms = [term.casefold() for term in (query or "").split() if len(term.strip()) >= 3]
        matches: list[dict[str, Any]] = []
        limitations: list[str] = ["Busca textual e UF são filtradas localmente após a publicação limitada.", "A fonte consultada é somente leitura e modalidade 6 (Pregão - Eletrônico)."]
        try:
            control = re.fullmatch(r"([A-Z0-9]{14})-1-(\d+)/(\d{4})", (query or "").strip(), re.IGNORECASE)
            if control:
                limitations = ["Consulta pública direcionada pelo número PNCP; somente o item e a janela informados seriam verificados."]
                cnpj, sequence_text, year_text = control.groups()
                year, sequence = int(year_text), int(sequence_text)
                publication = await self._get(settings.pncp_consulta_base_url, f"/v1/orgaos/{quote(cnpj, safe='')}/compras/{year}/{sequence}")
                published = str(publication.get("dataPublicacaoPncp") or "")[:10]
                source_uf = str((publication.get("unidadeOrgao") or {}).get("ufSigla") or "")
                if (str(publication.get("numeroControlePNCP") or "").casefold() != (query or "").strip().casefold()
                        or int(publication.get("modalidadeId") or 0) != 6
                        or not published or not start <= date.fromisoformat(published) <= end
                        or (uf and source_uf.upper() != uf.upper())):
                    return {"status": "success_empty", "complete": True, "matches": [], "limitations": ["Consulta direta por número PNCP; compra fora do recorte, modalidade ou UF informados."]}
                items = await self.items(cnpj, year, sequence)
                selected = [row for row in items if target_item_number is None or int(row.get("numeroItem", -1)) == target_item_number]
                for item in selected:
                    if not item.get("temResultado", False):
                        continue
                    for result in await self.results(cnpj, year, sequence, int(item["numeroItem"])):
                        matches.append({"publication": publication, "item": item, "result": result, "cnpj": cnpj, "year": year, "sequence": sequence})
                        if len(matches) >= max_results:
                            break
                    if len(matches) >= max_results:
                        break
                return {"status": "success" if matches else "success_empty", "complete": True, "matches": matches,
                        "limitations": ["Consulta pública direcionada pelo número PNCP; apenas o item e a janela informados foram verificados."]}
            for publication in await self.publication_rows(start, end):
                if uf and str(publication.get("ufSigla") or publication.get("uf") or "").upper() != uf.upper():
                    continue
                cnpj, year, sequence = self._publication_identity(publication)
                if not cnpj or year is None or sequence is None:
                    continue
                for item in await self.items(cnpj, year, sequence):
                    description = str(item.get("descricao") or "")
                    if terms and not all(term in description.casefold() for term in terms):
                        continue
                    if not item.get("temResultado", False):
                        continue
                    for result in await self.results(cnpj, year, sequence, int(item.get("numeroItem"))):
                        if len(matches) >= max_results:
                            return {"status": "success", "complete": True, "matches": matches, "limitations": limitations}
                        matches.append({"publication": publication, "item": item, "result": result, "cnpj": cnpj, "year": year, "sequence": sequence})
        except PartnerSourceError as exc:
            limitations.append(str(exc))
            return {"status": exc.kind, "complete": False, "matches": matches, "limitations": limitations}
        return {"status": "success_empty" if not matches else "success", "complete": True, "matches": matches, "limitations": limitations}

