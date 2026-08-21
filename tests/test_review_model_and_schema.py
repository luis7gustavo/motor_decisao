from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.models.opportunity_review import OpportunityReview
from app.schemas.review import ReviewDecisionInput, ReviewFilters


def test_review_model_matches_auditable_table() -> None:
    table = OpportunityReview.__table__

    assert table.schema == "feedback"
    assert table.name == "opportunity_reviews"
    assert table.c.opportunity_id.type.python_type.__name__ == "UUID"
    assert table.c.snapshot_id.type.python_type.__name__ == "UUID"
    assert table.c.run_id.type.python_type.__name__ == "UUID"
    assert {"is_active", "supersedes_review_id", "undoes_review_id"} <= set(table.c.keys())


def test_review_migration_creates_schema_unique_active_pair_and_indexes() -> None:
    migration = (
        Path(__file__).parents[1]
        / "alembic"
        / "versions"
        / "20260817_0009_opportunity_reviews.py"
    ).read_text(encoding="utf-8")

    assert 'down_revision = "20260812_0008"' in migration
    assert "CREATE SCHEMA IF NOT EXISTS feedback" in migration
    assert "feedback.opportunity_reviews" in migration
    assert "uq_opportunity_reviews_active_snapshot" in migration
    assert "WHERE is_active = TRUE" in migration
    for required_index in (
        "idx_opportunity_reviews_opportunity",
        "idx_opportunity_reviews_snapshot",
        "idx_opportunity_reviews_decision",
        "idx_opportunity_reviews_active",
        "idx_opportunity_reviews_reviewed_at",
    ):
        assert required_index in migration


def test_approval_accepts_optional_maximum_purchase_price() -> None:
    decision = ReviewDecisionInput.model_validate(
        {
            "decision": "approved_test_purchase",
            "max_purchase_price": "123,45",
            "reviewer": " Ana ",
        }
    )

    assert decision.max_purchase_price == Decimal("123.45")
    assert decision.reviewer == "Ana"


@pytest.mark.parametrize("decision", ["needs_reanalysis", "rejected"])
def test_reason_is_required_for_non_approval_decisions(decision: str) -> None:
    with pytest.raises(ValidationError, match="motivo válido"):
        ReviewDecisionInput.model_validate({"decision": decision})


@pytest.mark.parametrize("decision", ["needs_reanalysis", "rejected"])
def test_other_reason_requires_notes(decision: str) -> None:
    with pytest.raises(ValidationError, match="Descreva o motivo"):
        ReviewDecisionInput.model_validate(
            {"decision": decision, "reason_code": "other", "notes": ""}
        )


def test_filters_normalize_decimal_comma() -> None:
    filters = ReviewFilters.model_validate(
        {"min_margin": "20,5", "min_profit": "10,25", "sort": "profit"}
    )

    assert filters.min_margin == Decimal("20.5")
    assert filters.min_profit == Decimal("10.25")
    assert filters.sort == "profit"
