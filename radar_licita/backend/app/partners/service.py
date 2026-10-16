from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import PartnerCandidate, PartnerObservation, PartnerSearchRun, ProcurementItem, Supplier
from app.partners.pncp_adapter import PNCPPartnerAdapter, payload_hash


def _terms(query: str | None, description: str) -> list[str]:
    values = (description if re.fullmatch(r"[A-Z0-9]{14}-1-\d+/\d{4}", (query or "").strip(), re.IGNORECASE) else query or description).split()
    return list(dict.fromkeys(value.casefold() for value in values if len(value) >= 3))[:12]


def _date(value: Any) -> date | None:
    if not value:
        return None
    return date.fromisoformat(str(value)[:10])


def _source_url(cnpj: str, year: int, sequence: int, item_number: int) -> str:
    return f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{year}/{sequence}/itens/{item_number}/resultados"


def _parameters_hash(item_id: int, payload) -> str:
    raw = {"item_id": item_id, "query": payload.query, "uf": payload.uf.upper() if payload.uf else None, "published_from": payload.published_from.isoformat() if payload.published_from else None, "published_to": payload.published_to.isoformat() if payload.published_to else None, "max_results": payload.max_results, "adapter": "s05-pncp-v1"}
    return hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()


def _candidate(db: Session, result: dict[str, Any], item: ProcurementItem, search: PartnerSearchRun, query: str | None) -> PartnerCandidate | None:
    payload = result["result"]
    if str(payload.get("tipoPessoa") or "").upper() != "PJ" or not payload.get("niFornecedor") or not payload.get("nomeRazaoSocialFornecedor"):
        return None
    if payload.get("dataCancelamento") or "cancel" in str(payload.get("situacaoCompraItemResultadoNome") or "").casefold():
        return None
    tax_id = str(payload["niFornecedor"])
    candidate = db.scalar(select(PartnerCandidate).where(PartnerCandidate.tax_id == tax_id))
    item_description = result["item"].get("descricao") or item.description
    matched_terms = _terms(query, item_description)
    # A historical result identifies a company, but does not verify that its current
    # catalogue product meets this procurement item's technical requirements.
    comparable = "pending"
    reasons = ["Identificação PJ observada em resultado público de pregão eletrônico.", "Preço preservado como homologado histórico; não é cotação atual."]
    if comparable == "pending":
        reasons.append("Equivalência técnica suficiente ainda depende de revisão humana.")
    if candidate is None:
        candidate = PartnerCandidate(legal_name=str(payload["nomeRazaoSocialFornecedor"]), tax_id=tax_id, discovery_status="discovered", comparability=comparable, reasons=reasons, matched_terms=matched_terms, observations=[])
        db.add(candidate)
        db.flush()
    else:
        candidate.legal_name = str(payload["nomeRazaoSocialFornecedor"])
        candidate.matched_terms = list(dict.fromkeys((candidate.matched_terms or []) + matched_terms))
        candidate.reasons = list(dict.fromkeys((candidate.reasons or []) + reasons))
    event_key = f"{result['cnpj']}-{result['year']}-{result['sequence']}-{int(result['item'].get('numeroItem'))}-{int(payload.get('sequencialResultado', 0))}"
    version = payload_hash(payload)
    existing = db.scalar(select(PartnerObservation).where(PartnerObservation.event_key == event_key, PartnerObservation.source_payload_hash == version))
    if existing is None:
        observation = PartnerObservation(candidate_id=candidate.id, search_run_id=search.id, procurement_item_id=item.id, official_control_number=str(payload.get("numeroControlePNCPCompra") or ""), item_number=int(result["item"].get("numeroItem")), result_sequence=int(payload.get("sequencialResultado", 0)), event_key=event_key, source_url=_source_url(result["cnpj"], result["year"], result["sequence"], int(result["item"].get("numeroItem"))), source_payload_hash=version, observed_at=datetime.now(UTC), description=item_description, unit=result["item"].get("unidadeMedida"), quantity=payload.get("quantidadeHomologada"), result_date=_date(payload.get("dataResultado")), person_type="PJ", status=payload.get("situacaoCompraItemResultadoNome"), price_nature="homologado", unit_price=payload.get("valorUnitarioHomologado"), raw_json=payload)
        db.add(observation)
        candidate.evidence_count = (candidate.evidence_count or 0) + 1
        candidate.observations = list(candidate.observations or []) + [{"event_key": event_key, "source_payload_hash": version, "source_url": observation.source_url}]
    candidate.last_observed_at = _date(payload.get("dataResultado")) or candidate.last_observed_at
    return candidate


async def run_partner_search(db: Session, item: ProcurementItem, payload, adapter: PNCPPartnerAdapter | None = None) -> PartnerSearchRun:
    parameters_hash = _parameters_hash(item.id, payload)
    existing = db.scalar(select(PartnerSearchRun).where(PartnerSearchRun.parameters_hash == parameters_hash))
    if existing and existing.status in {"success", "success_empty"}:
        return existing
    start = payload.published_from or date(datetime.now(UTC).year, 1, 1)
    end = payload.published_to or date.today()
    run = existing or PartnerSearchRun(procurement_item_id=item.id, query=payload.query, uf=payload.uf.upper() if payload.uf else None, published_from=start, published_to=end, max_results=payload.max_results, parameters_hash=parameters_hash, source="pncp", source_version="s05-pncp-v1", consulted_window={"published_from": start.isoformat(), "published_to": end.isoformat(), "modality_id": 6})
    run.status = "running"
    run.complete = False
    run.limitations = []
    run.queried_at = datetime.now(UTC)
    db.add(run)
    db.flush()
    owns = adapter is None
    adapter = adapter or PNCPPartnerAdapter()
    try:
        result = await adapter.search(payload.query, payload.uf, start, end, payload.max_results, target_item_number=item.item_number)
        run.status = result["status"]
        run.complete = result["complete"]
        run.limitations = result["limitations"]
        run.request_count = adapter.request_count
        run.raw_json = {"adapter": "s05-pncp-v1", "match_count": len(result["matches"]), "payload_hashes": [payload_hash(row["result"]) for row in result["matches"]]}
        for row in result["matches"]:
            _candidate(db, row, item, run, payload.query)
    except Exception as exc:
        run.status = "unavailable"
        run.complete = False
        run.limitations = [f"Falha controlada na fonte pública: {exc}"]
        run.request_count = adapter.request_count
    finally:
        if owns:
            await adapter.__aexit__(None, None, None)
    db.commit()
    db.refresh(run)
    return run


def candidates_for_item(db: Session, item_id: int) -> list[PartnerCandidate]:
    return list(db.scalars(select(PartnerCandidate).join(PartnerObservation, PartnerObservation.candidate_id == PartnerCandidate.id).where(PartnerObservation.procurement_item_id == item_id).distinct().order_by(PartnerCandidate.id)))


def promote_candidate(db: Session, candidate: PartnerCandidate) -> PartnerCandidate:
    if candidate.supplier_id is None:
        supplier = db.scalar(select(Supplier).where(Supplier.tax_id == candidate.tax_id))
        if supplier is None:
            supplier = Supplier(name=candidate.legal_name, tax_id=candidate.tax_id)
            db.add(supplier)
            db.flush()
        candidate.supplier_id = supplier.id
    candidate.discovery_status = "promoted"
    db.commit()
    db.refresh(candidate)
    return candidate

