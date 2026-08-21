from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Numeric, String, Text, Uuid, func, text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OpportunityReview(Base):
    """Decisão humana ou evento de desfazer, preservado para auditoria e ML."""

    __tablename__ = "opportunity_reviews"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('decision', 'undo')",
            name="ck_opportunity_reviews_event_type",
        ),
        CheckConstraint(
            "human_decision IS NULL OR human_decision IN "
            "('approved_test_purchase', 'needs_reanalysis', 'rejected')",
            name="ck_opportunity_reviews_human_decision",
        ),
        CheckConstraint(
            "(event_type = 'decision' AND human_decision IS NOT NULL) OR "
            "(event_type = 'undo' AND human_decision IS NULL AND is_active = false)",
            name="ck_opportunity_reviews_event_payload",
        ),
        {"schema": "feedback"},
    )

    id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    opportunity_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    snapshot_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    supplier_product_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(20), nullable=False, default="decision")
    human_decision: Mapped[str | None] = mapped_column(String(40))
    reason_code: Mapped[str | None] = mapped_column(String(80))
    notes: Mapped[str | None] = mapped_column(Text)
    max_purchase_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    reviewer: Mapped[str | None] = mapped_column(String(160))
    original_heuristic_recommendation: Mapped[str] = mapped_column(String(40), nullable=False)
    original_heuristic_score: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    ml_score: Mapped[Decimal | None] = mapped_column(Numeric(8, 4))
    heuristic_version: Mapped[str | None] = mapped_column(String(120))
    model_version: Mapped[str | None] = mapped_column(String(120))
    run_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True))
    reviewed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    supersedes_review_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("feedback.opportunity_reviews.id", ondelete="RESTRICT"),
    )
    undoes_review_id: Mapped[UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("feedback.opportunity_reviews.id", ondelete="RESTRICT"),
    )
    invalidated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    invalidated_by: Mapped[str | None] = mapped_column(String(160))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
