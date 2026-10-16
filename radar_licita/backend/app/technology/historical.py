"""Small, evidence-backed historical technology exhibit for the local demo."""
from __future__ import annotations

import json
import hashlib
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from app.partners.pncp_adapter import PNCPPartnerAdapter, payload_hash

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "examples" / "demo" / "fixtures" / "official" / "PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json"
FIXTURE_SHA256 = "503ce560e9a0ef8ca755cec706ac0be2a3170a78878777e337fa661bda9f064d"
SELECTION = (
    ("00394452000103", 2026, 15447, (1,)),
    ("03238755000117", 2026, 31, (5363, 5365, 5366)),
    ("05522917000170", 2026, 138, (1, 2, 3, 4, 5, 6, 7)),
    ("18316281000151", 2026, 112, (1,)),
    ("90871831000121", 2026, 23, (1,)),
    ("03681070000140", 2026, 8, (1,)),
    ("63025530000104", 2026, 2341, (2, 3, 4, 5, 6)),
)


async def capture_historical_technology(adapter: PNCPPartnerAdapter | None = None) -> dict[str, Any]:
    """Read identified pregões, selected items and their PJ results."""
    own_adapter = adapter is None
    pncp = adapter or PNCPPartnerAdapter(max_requests=50, deadline_seconds=150)
    records = []
    try:
        for cnpj, year, sequence, numbers in SELECTION:
            purchase_url = f"https://pncp.gov.br/api/consulta/v1/orgaos/{cnpj}/compras/{year}/{sequence}"
            items_url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{year}/{sequence}/itens"
            purchase = await pncp._get("https://pncp.gov.br/api/consulta", f"/v1/orgaos/{cnpj}/compras/{year}/{sequence}")
            expected_control = f"{cnpj}-1-{sequence:06d}/{year}"
            if purchase.get("numeroControlePNCP") != expected_control or int(purchase.get("modalidadeId") or 0) != 6:
                raise ValueError(f"Compra inesperada: {expected_control}")
            source_items = await pncp.items(cnpj, year, sequence)
            chosen = []
            for number in numbers:
                item = next((row for row in source_items if int(row.get("numeroItem") or 0) == number), None)
                if item is None or not item.get("temResultado"):
                    raise ValueError(f"Item sem resultado: {expected_control}/{number}")
                result_url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{year}/{sequence}/itens/{number}/resultados"
                results = [row for row in await pncp.results(cnpj, year, sequence, number)
                           if str(row.get("tipoPessoa") or "").upper() == "PJ" and row.get("niFornecedor")
                           and row.get("nomeRazaoSocialFornecedor") and not row.get("dataCancelamento")]
                if not results:
                    raise ValueError(f"Sem resultado PJ: {expected_control}/{number}")
                chosen.append({"item": item, "item_hash": payload_hash(item), "result_url": result_url,
                               "results": results, "result_hashes": [payload_hash(row) for row in results]})
            records.append({"purchase_url": purchase_url, "purchase": purchase,
                            "purchase_hash": payload_hash(purchase), "items_url": items_url,
                            "items": chosen})
        return {"kind": "official_historical_technology", "captured_at": datetime.now(UTC).isoformat(),
                "source": "PNCP public production API", "selection": "identified closed-window electronic auctions and selected items with PJ results",
                "publication_search": {"windows": [{"start": "2026-07-01", "end": "2026-07-02"},
                                                    {"start": "2026-08-03", "end": "2026-08-04"}],
                                       "modality_id": 6, "scope": "publication pages inspected; controls then queried directly"},
                "request_count": pncp.request_count, "records": records}
    finally:
        if own_adapter:
            await pncp.__aexit__(None, None, None)


def load_historical_snapshot() -> dict[str, Any]:
    raw = FIXTURE.read_bytes()
    if hashlib.sha256(raw).hexdigest() != FIXTURE_SHA256:
        raise ValueError("Captura histórica difere do arquivo oficial conferido")
    return json.loads(raw)


def _money(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        amount = Decimal(str(value))
        return str(amount) if amount.is_finite() and amount >= 0 else None
    except (InvalidOperation, ValueError):
        return None


def _quantity(value: Any) -> str | None:
    amount = _money(value)
    if amount is None:
        return None
    number = Decimal(amount)
    return str(int(number)) if number == number.to_integral_value() else format(number.normalize(), "f")


def historical_view(snapshot: dict[str, Any], *, mode: str) -> dict[str, Any]:
    if snapshot.get("kind") != "official_historical_technology":
        raise ValueError("Snapshot histórico não identificado")
    cases = []
    tax_ids = set()
    result_count = 0
    for record in snapshot["records"]:
        purchase = record["purchase"]
        control = purchase["numeroControlePNCP"]
        if payload_hash(purchase) != record["purchase_hash"] or int(purchase.get("modalidadeId") or 0) != 6:
            raise ValueError(f"Compra sem integridade: {control}")
        close_raw = purchase.get("dataEncerramentoProposta")
        close = datetime.fromisoformat(close_raw) if close_raw else None
        if close is None or close.date() >= datetime.now(UTC).date():
            raise ValueError(f"Compra não confirmada como histórica: {control}")
        items = []
        for entry in record["items"]:
            item = entry["item"]
            number = int(item["numeroItem"])
            if payload_hash(item) != entry["item_hash"]:
                raise ValueError(f"Item sem integridade: {control}/{number}")
            if len(entry["results"]) != len(entry["result_hashes"]):
                raise ValueError(f"Resultado sem hash: {control}/{number}")
            companies = []
            for result, digest in zip(entry["results"], entry["result_hashes"], strict=True):
                if (payload_hash(result) != digest or str(result.get("tipoPessoa")) != "PJ"
                        or result.get("dataCancelamento") or int(result.get("numeroItem") or 0) != number):
                    raise ValueError(f"Resultado inválido: {control}/{number}")
                if result.get("numeroControlePNCPCompra") not in (None, control):
                    raise ValueError(f"Resultado de outra compra: {control}/{number}")
                tax_id = str(result["niFornecedor"])
                tax_ids.add(tax_id)
                result_count += 1
                companies.append({"tax_id": tax_id, "legal_name": result["nomeRazaoSocialFornecedor"],
                                  "result_date": result.get("dataResultado"),
                                  "unit_price": _money(result.get("valorUnitarioHomologado")),
                                  "status": result.get("situacaoCompraItemResultadoNome"),
                                  "source_url": entry["result_url"], "source_hash": digest,
                                  "technical_match": "pending"})
            items.append({"item_number": number, "description": item["descricao"],
                          "quantity": _quantity(item.get("quantidade")), "unit": item.get("unidadeMedida"),
                          "public_estimate": _money(item.get("valorTotal")),
                          "item_url": f"{record['items_url']}/{number}", "item_hash": entry["item_hash"],
                          "companies": companies})
        cases.append({"control_number": control, "object_description": purchase.get("objetoCompra"),
                      "organization": (purchase.get("orgaoEntidade") or {}).get("razaoSocial"),
                      "uf": (purchase.get("unidadeOrgao") or {}).get("ufSigla"),
                      "published_at": purchase.get("dataPublicacaoPncp"),
                      "proposal_close_at": close_raw,
                      "pncp_url": f"https://pncp.gov.br/app/editais/{purchase['orgaoEntidade']['cnpj']}/{purchase['anoCompra']}/{purchase['sequencialCompra']}",
                      "purchase_url": record["purchase_url"], "purchase_hash": record["purchase_hash"],
                      "items": items})
    return {"mode": mode, "captured_at": snapshot["captured_at"],
            "request_count": snapshot.get("request_count", 0),
            "selection": snapshot.get("selection"), "cases": cases,
            "totals": {"procurements": len(cases), "items": sum(len(case["items"]) for case in cases),
                       "results": result_count, "distinct_companies": len(tax_ids)},
            "limitations": ["Recorte direcionado de pregões eletrônicos com prazo encerrado e resultado publicado.",
                            "Resultado publicado identifica participante histórico, sem comprovar contrato executado, estoque, interesse ou cotação atual."]}
