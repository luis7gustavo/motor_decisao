from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RecommendationSearchRequest(BaseModel):
    available_capital: Decimal | None = Field(default=None, ge=0)
    q: str | None = Field(default=None, max_length=200)
    uf: str | None = Field(default=None, min_length=2, max_length=2)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=25, ge=1, le=100)


class RecommendationItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    procurement_id: int
    item_id: int
    scenario_id: int | None
    supplier_quote_id: int | None
    eligibility: str
    score: Decimal | None
    required_capital: Decimal | None
    budget_remaining: Decimal | None
    estimated_result: Decimal | None
    margin_rate: Decimal | None
    proposal_close_at: datetime | None
    pending_checks: list[str]
    blockers: list[str]
    explanation: str
    cost_basis: str
    origin: dict[str, Any]


class RecommendationGroup(BaseModel):
    items: list[RecommendationItem]
    total: int


class RecommendationSearchResponse(BaseModel):
    available_capital: Decimal | None
    evaluated_at: datetime
    rules_version: str
    recommendations: list[RecommendationItem]
    needs_analysis: list[RecommendationItem]
    outside_budget: list[RecommendationItem]
    blocked: list[RecommendationItem]
    totals: dict[str, int]
    page: int
    page_size: int

