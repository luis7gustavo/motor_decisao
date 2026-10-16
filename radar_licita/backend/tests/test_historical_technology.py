from __future__ import annotations

import asyncio
import copy

import pytest

from app.technology.historical import capture_historical_technology, historical_view, load_historical_snapshot


def test_recorded_pncp_chains_are_linked_by_purchase_and_item():
    view = historical_view(load_historical_snapshot(), mode="snapshot")
    assert view["totals"] == {"procurements": 7, "items": 19, "results": 19, "distinct_companies": 11}
    assert [case["control_number"] for case in view["cases"][:2]] == [
        "00394452000103-1-015447/2026", "03238755000117-1-000031/2026"]
    first = view["cases"][0]["items"][0]
    assert first["item_number"] == 1
    assert first["quantity"] == "12"
    assert first["companies"][0]["tax_id"] == "42145571000164"
    assert "/compras/2026/15447/itens/1/resultados" in first["companies"][0]["source_url"]
    assert all(company["technical_match"] == "pending" for case in view["cases"]
               for item in case["items"] for company in item["companies"])


def test_modified_official_result_is_rejected():
    snapshot = copy.deepcopy(load_historical_snapshot())
    snapshot["records"][0]["items"][0]["results"][0]["niFornecedor"] = "00000000000000"
    with pytest.raises(ValueError, match="Resultado inválido"):
        historical_view(snapshot, mode="snapshot")


def test_live_extractor_requests_only_selected_items_and_pj_results():
    snapshot = load_historical_snapshot()

    class RecordedAdapter:
        request_count = 33

        async def _get(self, _base, path):
            return next(row["purchase"] for row in snapshot["records"]
                        if path.endswith("/" + str(row["purchase"]["sequencialCompra"])))

        async def items(self, cnpj, year, sequence):
            row = next(row for row in snapshot["records"] if row["purchase"]["orgaoEntidade"]["cnpj"] == cnpj)
            return [entry["item"] for entry in row["items"]]

        async def results(self, cnpj, year, sequence, number):
            row = next(row for row in snapshot["records"] if row["purchase"]["orgaoEntidade"]["cnpj"] == cnpj)
            return next(entry["results"] for entry in row["items"] if entry["item"]["numeroItem"] == number)

    captured = asyncio.run(capture_historical_technology(RecordedAdapter()))
    assert historical_view(captured, mode="live")["totals"] == {
        "procurements": 7, "items": 19, "results": 19, "distinct_companies": 11}
