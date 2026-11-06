from decimal import Decimal

import pytest

from app.documents.service import extract_page_evidence, process_document
from app.ingestion.pncp_client import PNCPError
from app.logistics.service import classify_logistics
from app.models import DeliveryClass, DocumentExtraction, ProcurementDocument


def _add_evidence(db, procurement, rows):
    document = ProcurementDocument(procurement_id=procurement.id, sequence=1, source_url="https://pncp.gov.br/doc", raw_json={})
    db.add(document)
    db.flush()
    for row in rows:
        db.add(DocumentExtraction(document_id=document.id, page_number=1, excerpt="cláusula", confidence=Decimal("0.9"), extraction_method="regex", **row))
    db.flush()
    return document


def test_contradictory_delivery_clauses_are_indeterminate(db, procurement):
    _add_evidence(db, procurement, [
        {"field_name": "installation", "normalized_value": "required"},
        {"field_name": "installation", "normalized_value": "not_required"},
    ])
    result = classify_logistics(db, procurement.id)
    assert result.classification == DeliveryClass.UNDETERMINED
    assert result.remote_delivery_score < 50
    assert any("contraditórias" in item for item in result.pending_checks)


def test_explicit_on_site_installation_requires_local_operation(db, procurement):
    _add_evidence(db, procurement, [
        {"field_name": "installation", "normalized_value": "required"},
        {"field_name": "delivery_address", "normalized_value": "Almoxarifado central"},
    ])
    result = classify_logistics(db, procurement.id)
    assert result.classification == DeliveryClass.LOCAL_REQUIRED
    assert result.requires_installation is True


def test_regex_extraction_preserves_page_and_excerpt():
    text = "O prazo máximo de entrega será de 15 dias. Local de entrega: Almoxarifado Central, Rua Um, 10."
    evidence = extract_page_evidence(text, 7)
    deadline = next(row for row in evidence if row["field_name"] == "delivery_deadline")
    assert deadline["page_number"] == 7
    assert deadline["normalized_value"] == "15"
    assert "prazo" in deadline["excerpt"].lower()


@pytest.mark.asyncio
async def test_unavailable_document_keeps_opportunity_and_marks_failure(db, procurement):
    document = ProcurementDocument(procurement_id=procurement.id, sequence=1, source_url="https://pncp.gov.br/doc", raw_json={})
    db.add(document)
    db.commit()

    class FailingClient:
        async def download_document(self, *_): raise PNCPError("temporarily unavailable")

    result = await process_document(db, document, FailingClient())
    assert result.processing_status == "failed"
    assert db.get(type(procurement), procurement.id) is not None

