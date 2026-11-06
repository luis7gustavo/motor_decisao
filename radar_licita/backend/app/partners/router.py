import re

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import PartnerCandidate, PartnerObservation, ProcurementItem
from app.partners.schemas import PartnerCandidateResponse, PartnerSearchRequest, PartnerSearchResponse
from app.partners.service import candidates_for_item, promote_candidate, run_partner_search

router = APIRouter(prefix="/api/v1", tags=["partners"])


def _with_evidence(db: Session, candidate: PartnerCandidate, item_id: int | None = None) -> dict:
    observations = db.scalars(
        select(PartnerObservation)
        .where(PartnerObservation.candidate_id == candidate.id)
        .where(PartnerObservation.procurement_item_id == item_id if item_id is not None else True)
        .order_by(PartnerObservation.observed_at.desc(), PartnerObservation.id.desc())
    ).all()
    return {
        "id": candidate.id, "legal_name": candidate.legal_name, "tax_id": candidate.tax_id,
        "discovery_status": candidate.discovery_status, "comparability": candidate.comparability,
        "reasons": candidate.reasons, "matched_terms": candidate.matched_terms,
        "supplier_id": candidate.supplier_id, "evidence_count": len(observations),
        "last_observed_at": candidate.last_observed_at, "observations": observations,
    }


@router.post("/items/{item_id}/partner-searches", response_model=PartnerSearchResponse)
async def create_partner_search(item_id: int, payload: PartnerSearchRequest, db: Session = Depends(get_db)):
    item = db.get(ProcurementItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Procurement item not found")
    if re.fullmatch(r"[A-Z0-9]{14}-1-\d+/\d{4}", (payload.query or "").strip(), re.IGNORECASE):
        if item.procurement.official_control_number.casefold() != payload.query.strip().casefold():
            raise HTTPException(status_code=422, detail="O número PNCP deve pertencer à contratação deste item")
    run = await run_partner_search(db, item, payload)
    candidates = candidates_for_item(db, item_id)
    return {"id": run.id, "procurement_item_id": run.procurement_item_id, "status": run.status, "complete": run.complete, "request_count": run.request_count, "consulted_window": run.consulted_window, "queried_at": run.queried_at, "limitations": run.limitations, "candidates": [_with_evidence(db, candidate, item_id) for candidate in candidates]}


@router.get("/items/{item_id}/partner-candidates", response_model=list[PartnerCandidateResponse])
def list_partner_candidates(item_id: int, db: Session = Depends(get_db)):
    return [_with_evidence(db, candidate, item_id) for candidate in candidates_for_item(db, item_id)]


@router.post("/partner-candidates/{candidate_id}/promote", response_model=PartnerCandidateResponse)
def promote_partner_candidate(candidate_id: int, db: Session = Depends(get_db)):
    candidate = db.get(PartnerCandidate, candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail="Partner candidate not found")
    promoted = promote_candidate(db, candidate)
    return _with_evidence(db, promoted)

