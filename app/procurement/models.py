"""Isolated SQLAlchemy mappings; migrations, not create_all, own the DDL."""
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import BigInteger, Boolean, DateTime, Numeric, Text, Uuid, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class PublicBase(DeclarativeBase):
    pass


class Identity:
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, server_default=text('gen_random_uuid()'))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Procurement(Identity, PublicBase):
    __tablename__ = 'procurements'
    __table_args__ = {'schema': 'procurement'}
    source: Mapped[str] = mapped_column(Text)
    source_id: Mapped[str] = mapped_column(Text)
    cnpj_original: Mapped[str | None] = mapped_column(Text)
    cnpj: Mapped[str | None] = mapped_column(Text)
    cnpj_validation_status: Mapped[str] = mapped_column(Text, server_default='pendente')
    cnpj_validator_version: Mapped[str | None] = mapped_column(Text)
    purchase_year: Mapped[int | None]
    purchase_sequence: Mapped[int | None] = mapped_column(BigInteger)
    current_version_id: Mapped[UUID | None] = mapped_column(Uuid)


class ProcurementVersion(Identity, PublicBase):
    __tablename__ = 'procurement_versions'
    __table_args__ = {'schema': 'procurement'}
    procurement_id: Mapped[UUID] = mapped_column(Uuid)
    raw_response_id: Mapped[UUID] = mapped_column(Uuid)
    version_no: Mapped[int]
    content_hash: Mapped[str] = mapped_column(Text)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_time_text: Mapped[str | None] = mapped_column(Text)
    timezone_status: Mapped[str] = mapped_column(Text, server_default='unknown')
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    snapshot: Mapped[dict] = mapped_column(JSONB)
    schema_version: Mapped[str] = mapped_column(Text)
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    official_status: Mapped[str | None] = mapped_column(Text)
    publication_state: Mapped[str] = mapped_column(Text, server_default='rascunho')
    source_manifest: Mapped[dict] = mapped_column(JSONB)
    canonicalizer_version: Mapped[str] = mapped_column(Text)


class ProcurementItem(Identity, PublicBase):
    __tablename__ = 'procurement_items'
    __table_args__ = {'schema': 'procurement'}
    version_id: Mapped[UUID] = mapped_column(Uuid)
    source_item_key: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(24,6))
    unit: Mapped[str | None] = mapped_column(Text)
    attributes: Mapped[dict] = mapped_column(JSONB)
    estimated_unit_price: Mapped[Decimal | None] = mapped_column(Numeric(24,6))
    price_status: Mapped[str] = mapped_column(Text)


class JudgmentUnit(Identity, PublicBase):
    __tablename__ = 'judgment_units'
    __table_args__ = {'schema': 'procurement'}
    version_id: Mapped[UUID] = mapped_column(Uuid)
    source_unit_key: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(Text)
    criterion: Mapped[str | None] = mapped_column(Text)
    composition_status: Mapped[str] = mapped_column(Text)


class JudgmentUnitItem(PublicBase):
    __tablename__ = 'judgment_unit_items'
    __table_args__ = {'schema': 'procurement'}
    unit_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    item_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    version_id: Mapped[UUID] = mapped_column(Uuid)
    required: Mapped[bool] = mapped_column(Boolean)


class VersionObservation(Identity, PublicBase):
    __tablename__ = 'version_observations'
    __table_args__ = {'schema': 'procurement'}
    procurement_id: Mapped[UUID] = mapped_column(Uuid)
    version_id: Mapped[UUID] = mapped_column(Uuid)
    previous_version_id: Mapped[UUID | None] = mapped_column(Uuid)
    raw_response_id: Mapped[UUID] = mapped_column(Uuid)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    actor: Mapped[str] = mapped_column(Text)
    correlation_id: Mapped[UUID] = mapped_column(Uuid)
    changed: Mapped[bool] = mapped_column(Boolean)
