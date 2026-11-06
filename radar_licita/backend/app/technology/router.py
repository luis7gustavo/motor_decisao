from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session

from app.db import get_db
from app.technology.service import search_technology
from app.technology.historical import capture_historical_technology, historical_view, load_historical_snapshot
from app.partners.pncp_adapter import PartnerSourceError

router = APIRouter(prefix="/api/v1/technology", tags=["technology"])


@router.get("/historical-exhibit")
def historical_exhibit():
    try:
        return historical_view(load_historical_snapshot(), mode="snapshot")
    except (OSError, ValueError, KeyError) as exc:
        raise HTTPException(status_code=503, detail=f"Acervo histórico indisponível: {exc}") from exc


@router.post("/historical-exhibit/refresh")
async def refresh_historical_exhibit():
    try:
        return historical_view(await capture_historical_technology(), mode="live")
    except (PartnerSourceError, OSError, ValueError, KeyError) as exc:
        raise HTTPException(status_code=503, detail=f"Consulta pública indisponível: {exc}") from exc


class TechnologySearchRequest(BaseModel):
    budgets: list[Decimal] = Field(default_factory=lambda: [Decimal(5000), Decimal(10000), Decimal(50000)], min_length=1, max_length=8)
    keyword: str | None = Field(default=None, max_length=100)
    uf: str | None = Field(default=None, pattern=r"^[A-Za-z]{2}$")
    open_horizon_days: int = Field(default=14, ge=0, le=60)
    open_pages: int = Field(default=3, ge=1, le=8)
    max_open_purchases: int = Field(default=24, ge=1, le=40)
    history_start: date | None = None
    history_end: date | None = None
    history_pages: int = Field(default=2, ge=1, le=6)
    max_history_purchases: int = Field(default=20, ge=1, le=40)
    max_historical_results: int = Field(default=30, ge=1, le=100)

    @field_validator("budgets")
    @classmethod
    def positive_budgets(cls, values: list[Decimal]) -> list[Decimal]:
        if any(not value.is_finite() or value <= 0 or value > 100_000_000 for value in values):
            raise ValueError("Os orçamentos devem ser positivos e de até R$ 100 milhões")
        return list(dict.fromkeys(values))


@router.post("/search")
async def technology_search(payload: TechnologySearchRequest, db: Annotated[Session, Depends(get_db)]):
    try:
        return await search_technology(db, payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
