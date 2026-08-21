from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


HumanDecision = Literal[
    "approved_test_purchase",
    "needs_reanalysis",
    "rejected",
]
ReviewSort = Literal["score", "margin", "profit", "date"]

REANALYSIS_REASONS = {
    "collect_prices_again",
    "find_more_evidence",
    "fix_matching",
    "review_technical_attributes",
    "review_costs_and_fees",
    "confirm_availability",
    "stale_data",
    "other",
}

REJECTION_REASONS = {
    "insufficient_margin",
    "low_absolute_profit",
    "incorrect_matching",
    "conflicting_specifications",
    "low_demand",
    "few_market_evidences",
    "unreliable_market_price",
    "high_shipping_cost",
    "risky_product",
    "stale_data",
    "other",
}


class ReviewDecisionInput(BaseModel):
    decision: HumanDecision
    reason_code: str | None = Field(default=None, max_length=80)
    notes: str | None = Field(default=None, max_length=3000)
    max_purchase_price: Decimal | None = Field(default=None, gt=0, max_digits=12, decimal_places=2)
    reviewer: str | None = Field(default=None, max_length=160)

    @field_validator("reason_code", "notes", "reviewer", mode="before")
    @classmethod
    def blank_to_none(cls, value: Any) -> Any:
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value

    @field_validator("max_purchase_price", mode="before")
    @classmethod
    def normalize_decimal(cls, value: Any) -> Any:
        if value in (None, ""):
            return None
        if isinstance(value, str):
            return value.strip().replace(",", ".")
        return value

    @model_validator(mode="after")
    def validate_decision_fields(self) -> "ReviewDecisionInput":
        if self.decision == "approved_test_purchase":
            if self.reason_code is not None:
                raise ValueError("A aprovação não aceita um motivo de descarte ou reanálise.")
            return self

        allowed_reasons = (
            REANALYSIS_REASONS
            if self.decision == "needs_reanalysis"
            else REJECTION_REASONS
        )
        if self.reason_code not in allowed_reasons:
            raise ValueError("Selecione um motivo válido para esta decisão.")
        if self.reason_code == "other" and not self.notes:
            raise ValueError("Descreva o motivo quando a opção 'Outro' for selecionada.")
        if self.max_purchase_price is not None:
            raise ValueError("Preço máximo de compra só pode ser informado na aprovação.")
        return self


class ReviewFilters(BaseModel):
    supplier: str | None = Field(default=None, max_length=120)
    q: str | None = Field(default=None, max_length=200)
    min_margin: Decimal | None = Field(default=None, ge=0, le=1000)
    min_profit: Decimal | None = Field(default=None, ge=0, max_digits=14, decimal_places=2)
    min_score: Decimal | None = Field(default=None, ge=0, le=100)
    min_match: Decimal | None = Field(default=None, ge=0, le=100)
    risk: str | None = Field(default=None, max_length=120)
    sort: ReviewSort = "score"

    @field_validator("supplier", "q", "risk", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> Any:
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value

    @field_validator("min_margin", "min_profit", "min_score", "min_match", mode="before")
    @classmethod
    def normalize_number(cls, value: Any) -> Any:
        if value in (None, ""):
            return None
        if isinstance(value, str):
            return value.strip().replace(",", ".")
        return value

    def active_values(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for key, value in self.model_dump().items():
            if value is None or (key == "sort" and value == "score"):
                continue
            values[key] = str(value)
        return values
