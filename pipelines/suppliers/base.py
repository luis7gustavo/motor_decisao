from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class SupplierProductSnapshot:
    supplier_slug: str
    source_url: str | None
    raw_title: str
    raw_price: float | None
    raw_stock: int | None
    sku: str | None
    ean: str | None
    payload: dict[str, Any]


class SupplierSource(Protocol):
    supplier_slug: str

    def fetch(self) -> list[SupplierProductSnapshot]:
        """Fetch supplier products with conservative scraping rules."""

