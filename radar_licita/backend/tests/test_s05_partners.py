from datetime import date, timedelta
from decimal import Decimal

import httpx
import pytest

from app.models import PartnerCandidate, PartnerObservation, ProcurementItem, Supplier
from app.partners.schemas import PartnerSearchRequest
from app.partners.pncp_adapter import PNCPPartnerAdapter
from app.partners.service import candidates_for_item, promote_candidate, run_partner_search


class FakeAdapter:
    def __init__(self, matches=None, status="success", complete=True, error=None):
        self.matches = matches or []
        self.status = status
        self.complete = complete
        self.error = error
        self.request_count = 3

    async def search(self, query, uf, start, end, max_results, *, target_item_number=None):
        if self.error:
            raise RuntimeError(self.error)
        return {"status": self.status, "complete": self.complete, "matches": self.matches[:max_results], "limitations": ["fixture oficial identificada"]}


def match(tax_id="49552222000106", name="COMERCIAL AGRO VOGEL E ZIMMERMANN LTDA", seq=1, price=9600, cancelled=None, description="ARADO SUBSOLADOR DE 07 HASTES"):
    result = {
        "tipoPessoa": "PJ", "niFornecedor": tax_id, "nomeRazaoSocialFornecedor": name,
        "numeroControlePNCPCompra": "91984492000152-1-000082/2026", "sequencialResultado": seq,
        "quantidadeHomologada": 1, "valorUnitarioHomologado": price, "dataResultado": "2026-08-26",
        "dataCancelamento": cancelled, "situacaoCompraItemResultadoNome": "Informado",
    }
    return {"cnpj": "91984492000152", "year": 2026, "sequence": 82, "item": {"numeroItem": 1, "descricao": description, "unidadeMedida": "un", "temResultado": True}, "result": result}


def item(db, procurement):
    value = ProcurementItem(procurement_id=procurement.id, item_number=1, description="Arado subsolador", quantity=1, unit="un", raw_json={})
    db.add(value)
    db.flush()
    return value


@pytest.mark.asyncio
async def test_search_persists_real_shape_and_deduplicates(db, procurement):
    target = item(db, procurement)
    payload = PartnerSearchRequest(query="arado", published_from=date(2026, 8, 1), published_to=date(2026, 8, 31))
    first = await run_partner_search(db, target, payload, FakeAdapter([match()]))
    second = await run_partner_search(db, target, payload, FakeAdapter([match()]))
    assert first.id == second.id
    assert first.status == "success"
    assert len(candidates_for_item(db, target.id)) == 1
    assert db.query(PartnerObservation).count() == 1
    assert first.raw_json["payload_hashes"]


@pytest.mark.asyncio
async def test_cancelled_and_non_pj_are_excluded_but_missing_price_is_kept(db, procurement):
    target = item(db, procurement)
    candidates = [match(price=None), match(tax_id="11111111000111", name="MESMO NOME LTDA", seq=2, cancelled="2026-08-27"), match(tax_id="22222222000122", name="MESMO NOME LTDA", seq=3)]
    run = await run_partner_search(db, target, PartnerSearchRequest(query="arado"), FakeAdapter(candidates))
    assert run.status == "success"
    values = candidates_for_item(db, target.id)
    assert {candidate.tax_id for candidate in values} == {"49552222000106", "22222222000122"}
    assert values[0].evidence_count == 1
    assert db.query(PartnerObservation).count() == 2


@pytest.mark.asyncio
async def test_empty_and_source_failure_are_distinct(db, procurement):
    target = item(db, procurement)
    empty = await run_partner_search(db, target, PartnerSearchRequest(query="inexistente"), FakeAdapter([], status="success_empty"))
    assert empty.status == "success_empty" and empty.complete is True
    second = ProcurementItem(procurement_id=procurement.id, item_number=2, description="Papel A4", quantity=1, unit="un", raw_json={})
    db.add(second)
    db.flush()
    failed = await run_partner_search(db, second, PartnerSearchRequest(query="papel"), FakeAdapter(error="timeout"))
    assert failed.status == "unavailable" and failed.complete is False
    assert "timeout" in failed.limitations[0]
    retried = await run_partner_search(db, second, PartnerSearchRequest(query="papel"), FakeAdapter([], status="success_empty"))
    assert retried.id == failed.id and retried.status == "success_empty"


@pytest.mark.asyncio
async def test_promote_is_idempotent_and_does_not_create_quote(db, procurement):
    target = item(db, procurement)
    await run_partner_search(db, target, PartnerSearchRequest(query="arado"), FakeAdapter([match()]))
    candidate = candidates_for_item(db, target.id)[0]
    promoted = promote_candidate(db, candidate)
    again = promote_candidate(db, promoted)
    assert promoted.supplier_id == again.supplier_id
    assert promoted.discovery_status == "promoted"
    assert db.query(Supplier).count() == 1
    assert not hasattr(promoted, "supplier_quotes")


@pytest.mark.asyncio
async def test_adapter_processes_publication_chain_after_retry():
    calls = {"count": 0}

    def handler(request):
        calls["count"] += 1
        if request.url.path.endswith("/publicacao"):
            if calls["count"] == 1:
                return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
            return httpx.Response(200, json={"data": [{"cnpjOrgao": "91984492000152", "anoCompra": 2026, "sequencialCompra": 82}], "totalPaginas": 1}, request=request)
        if request.url.path.endswith("/itens"):
            return httpx.Response(200, json=[{"numeroItem": 1, "descricao": "Arado subsolador", "unidadeMedida": "un", "temResultado": True}], request=request)
        return httpx.Response(200, json=[{"tipoPessoa": "PJ", "niFornecedor": "49552222000106", "nomeRazaoSocialFornecedor": "Fornecedor", "sequencialResultado": 1}], request=request)

    transport = httpx.MockTransport(handler)
    async with PNCPPartnerAdapter(client=httpx.AsyncClient(transport=transport), max_requests=6) as adapter:
        result = await adapter.search("arado", None, date(2026, 8, 1), date(2026, 8, 31), 10)
    assert result["status"] == "success"
    assert len(result["matches"]) == 1
    assert calls["count"] == 4


@pytest.mark.asyncio
async def test_direct_official_control_reads_purchase_item_and_result():
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith("/82"):
            return httpx.Response(200, json={
                "numeroControlePNCP": "91984492000152-1-000082/2026",
                "dataPublicacaoPncp": "2026-08-03T00:00:49", "modalidadeId": 6,
                "unidadeOrgao": {"ufSigla": "RS"},
            }, request=request)
        if request.url.path.endswith("/itens"):
            return httpx.Response(200, json=[{"numeroItem": 1, "descricao": "ARADO SUBSOLADOR", "temResultado": True}], request=request)
        return httpx.Response(200, json=[match()["result"]], request=request)

    async with PNCPPartnerAdapter(client=httpx.AsyncClient(transport=httpx.MockTransport(handler))) as adapter:
        result = await adapter.search("91984492000152-1-000082/2026", "RS", date(2026, 8, 1), date(2026, 8, 31), 10, target_item_number=1)
    assert result["status"] == "success" and len(result["matches"]) == 1
    assert len(calls) == 3 and calls[0].endswith("/82")
    assert result["matches"][0]["result"]["niFornecedor"] == "49552222000106"


def test_direct_control_cannot_attach_other_purchase(client, db, procurement):
    target = item(db, procurement)
    db.commit()
    response = client.post(f"/api/v1/items/{target.id}/partner-searches", json={"query": "91984492000152-1-000082/2026"})
    assert response.status_code == 422
    assert db.query(PartnerObservation).count() == 0

