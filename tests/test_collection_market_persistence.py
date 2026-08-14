from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

import pytest

from pipelines.collection.collectors import catalog_market, market_web
from pipelines.market_web.base import MarketListingSnapshot


def _snapshot(price: float | None) -> MarketListingSnapshot:
    return MarketListingSnapshot(
        source_name="amazon",
        source_role="benchmark",
        query="ssd",
        position=1,
        title="SSD de teste",
        price=price,
        old_price=None,
        currency_id="BRL" if price is not None else None,
        sold_quantity_text=None,
        sold_quantity=None,
        demand_signal_type=None,
        demand_signal_value=None,
        bsr_text=None,
        rating_text=None,
        reviews_count=None,
        seller_text=None,
        shipping_text=None,
        installments_text=None,
        item_url="https://example.com/ssd",
        image_url=None,
        is_sponsored=False,
        is_full=None,
        is_catalog=False,
        blocked=False,
        block_reason=None,
        payload={"price": price},
    )


class _FakeEngine:
    @contextmanager
    def begin(self):
        yield object()


@pytest.mark.parametrize("collector_module", [market_web, catalog_market])
def test_market_collectors_persist_only_positive_prices(monkeypatch, collector_module) -> None:
    inserted: list[MarketListingSnapshot] = []

    monkeypatch.setattr(collector_module, "engine", _FakeEngine())
    monkeypatch.setattr(
        collector_module,
        "_insert_listing",
        lambda _connection, *, source_run_id, snapshot: inserted.append(snapshot) or True,
    )

    collector_class = (
        collector_module.CrawleeMarketCollector
        if collector_module is market_web
        else collector_module.CrawleeCatalogMarketCollector
    )
    persisted = collector_class._persist(
        uuid4(),
        [_snapshot(199.9), _snapshot(None), _snapshot(0), _snapshot(-10)],
    )

    assert persisted == 1
    assert [item.price for item in inserted] == [199.9]
