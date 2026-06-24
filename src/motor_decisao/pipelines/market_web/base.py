from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class MarketListingSnapshot:
    source_name: str
    source_role: str
    query: str
    position: int | None
    title: str | None
    price: float | None
    old_price: float | None
    currency_id: str | None
    sold_quantity_text: str | None
    sold_quantity: int | None
    demand_signal_type: str | None
    demand_signal_value: float | None
    bsr_text: str | None
    rating_text: str | None
    reviews_count: int | None
    seller_text: str | None
    shipping_text: str | None
    installments_text: str | None
    item_url: str | None
    image_url: str | None
    is_sponsored: bool | None
    is_full: bool | None
    is_catalog: bool | None
    blocked: bool
    block_reason: str | None
    payload: dict[str, Any]


class MarketWebSource(Protocol):
    source_name: str
    source_role: str

    def fetch(self, *, query: str, max_results: int) -> list[MarketListingSnapshot]:
        """Fetch listings without bypassing CAPTCHA, login walls or hard blocks."""
