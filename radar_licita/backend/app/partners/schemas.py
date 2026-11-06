from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PartnerSearchRequest(BaseModel):
    query: str | None = Field(default=None, max_length=300)
    uf: str | None = Field(default=None, min_length=2, max_length=2)
    published_from: date | None = None
    published_to: date | None = None
    max_results: int = Field(default=10, ge=1, le=10)


class PartnerObservationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    official_control_number: str
    item_number: int
    result_sequence: int
    source_url: str
    observed_at: datetime
    description: str | None
    unit: str | None
    quantity: Decimal | None
    result_date: date | None
    person_type: str | None
    status: str | None
    price_nature: str | None
    unit_price: Decimal | None


class PartnerCandidateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    legal_name: str
    tax_id: str
    discovery_status: str
    comparability: str
    reasons: list[str]
    matched_terms: list[str]
    supplier_id: int | None
    evidence_count: int
    last_observed_at: date | None
    observations: list[PartnerObservationResponse] = Field(default_factory=list)


class PartnerSearchResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    procurement_item_id: int
    status: str
    complete: bool
    request_count: int
    consulted_window: dict[str, Any]
    queried_at: datetime
    limitations: list[str]
    candidates: list[PartnerCandidateResponse]

