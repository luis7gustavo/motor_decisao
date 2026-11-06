from datetime import date

import pytest
from sqlalchemy import func, select

from app.ingestion.service import run_sync, upsert_documents, upsert_items, upsert_procurement
from app.models import Procurement, ProcurementDocument, ProcurementItem, ProcurementVersion


def test_deduplicates_procurements_and_versions(db, procurement_payload):
    first, created, changed = upsert_procurement(db, procurement_payload)
    db.commit()
    second, created_again, changed_again = upsert_procurement(db, procurement_payload)
    db.commit()
    assert first.id == second.id
    assert created is True and changed is True
    assert created_again is False and changed_again is False
    assert db.scalar(select(func.count(Procurement.id))) == 1
    assert db.scalar(select(func.count(ProcurementVersion.id))) == 1


def test_records_update_as_new_version(db, procurement_payload):
    procurement, _, _ = upsert_procurement(db, procurement_payload)
    db.commit()
    updated = {**procurement_payload, "objetoCompra": "Aquisição retificada de notebooks e monitores"}
    same, created, changed = upsert_procurement(db, updated)
    db.commit()
    assert same.id == procurement.id
    assert not created and changed
    assert same.object_description.startswith("Aquisição retificada")
    assert db.scalar(select(func.count(ProcurementVersion.id))) == 2


@pytest.mark.parametrize("value,secret", [(0, False), (None, False), (1500, True)])
def test_zero_missing_or_confidential_estimate_is_not_real_price(db, procurement_payload, value, secret):
    payload = {**procurement_payload, "valorTotalEstimado": value, "orcamentoSigiloso": secret}
    procurement, _, _ = upsert_procurement(db, payload)
    db.commit()
    assert procurement.estimated_total_value is None
    assert procurement.is_price_confidential is True


def test_item_and_document_upserts_are_idempotent(db, procurement):
    items = [{"numeroItem": 1, "descricao": "Notebook", "quantidade": 2, "unidadeMedida": "UN", "valorUnitarioEstimado": 3000, "valorTotal": 6000, "orcamentoSigiloso": False}]
    docs = [{"sequencialDocumento": 1, "titulo": "Edital.pdf", "url": "https://pncp.gov.br/x", "tipoDocumentoNome": "Edital"}]
    upsert_items(db, procurement, items)
    upsert_documents(db, procurement, docs)
    db.commit()
    upsert_items(db, procurement, items)
    upsert_documents(db, procurement, docs)
    db.commit()
    assert db.scalar(select(func.count(ProcurementItem.id))) == 1
    assert db.scalar(select(func.count(ProcurementDocument.id))) == 1


@pytest.mark.asyncio
async def test_reexecuting_sync_job_is_idempotent(db, procurement_payload, monkeypatch):
    class FakeClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def iter_open_proposals(self, *_args, **_kwargs):
            yield procurement_payload
        async def get_items(self, *_): return []
        async def get_documents(self, *_): return []

    monkeypatch.setattr("app.ingestion.service.PNCPClient", FakeClient)
    for _ in range(2):
        job = await run_sync(db, mode="open", start_date=date(2026, 10, 1), end_date=date(2026, 10, 9), modality_ids=[6], max_pages=1, hydrate_details=False)
        assert job.status == "completed"
    assert db.scalar(select(func.count(Procurement.id))) == 1
    assert db.scalar(select(func.count(ProcurementVersion.id))) == 1

