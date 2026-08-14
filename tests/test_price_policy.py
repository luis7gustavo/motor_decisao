from __future__ import annotations

from decimal import Decimal

from app.core.settings import Settings


def test_purchase_candidate_price_boundaries() -> None:
    settings = Settings(
        SILLO_MAX_PRODUCT_PRICE_BRL=Decimal("3000.00"),
        _env_file=None,
    )

    assert settings.is_purchase_candidate_price(Decimal("2999.99"))
    assert settings.is_purchase_candidate_price(Decimal("3000.00"))
    assert not settings.is_purchase_candidate_price(Decimal("3000.01"))


def test_market_price_above_purchase_ceiling_is_not_invalid_data() -> None:
    settings = Settings(
        SILLO_MAX_PRODUCT_PRICE_BRL=Decimal("3000.00"),
        _env_file=None,
    )

    assert not settings.is_purchase_candidate_price(Decimal("3300.00"))
    assert Decimal("3300.00") > 0
