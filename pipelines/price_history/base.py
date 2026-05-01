from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class PriceHistorySnapshot:
    source_name: str
    product_key: str | None
    product_url: str | None
    title: str | None
    current_price: float | None
    min_price: float | None
    median_price: float | None
    max_price: float | None
    history_window_days: int | None
    payload: dict[str, Any]
    avg_price: float | None = None
    query: str | None = None
    source_mode: str = "search_current"
    blocked: bool = False
    block_reason: str | None = None


class PriceHistorySource(Protocol):
    source_name: str

    def fetch(self, *, query: str | None = None, product_url: str | None = None) -> list[PriceHistorySnapshot]:
        """Fetch price history snapshots.

        Implementations must respect the source terms, use conservative rates,
        and fail closed when blocking, CAPTCHA or login walls appear.
        """
