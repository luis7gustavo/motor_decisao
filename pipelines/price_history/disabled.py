from __future__ import annotations

from pipelines.price_history.base import PriceHistorySnapshot


class DisabledPriceHistorySource:
    source_name = "disabled"

    def fetch(self, *, query: str | None = None, product_url: str | None = None) -> list[PriceHistorySnapshot]:
        return []

