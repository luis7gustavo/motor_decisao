from __future__ import annotations

import enum
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class AnalysisStatus(str, enum.Enum):
    NEW = "new"
    IN_ANALYSIS = "in_analysis"
    AWAITING_QUOTE = "awaiting_quote"
    VIABLE = "viable"
    DISCARDED = "discarded"
    PREPARING_PROPOSAL = "preparing_proposal"
    PROPOSAL_SENT = "proposal_sent"
    WON = "won"
    LOST = "lost"
    CONTRACTED = "contracted"
    COMPLETED = "completed"


class DeliveryClass(str, enum.Enum):
    FAVORABLE = "A"
    CONDITIONAL = "B"
    LOCAL_REQUIRED = "C"
    UNDETERMINED = "D"


class MatchClass(str, enum.Enum):
    COMPATIBLE = "compatible"
    POSSIBLY_COMPATIBLE = "possibly_compatible"
    INCOMPATIBLE = "incompatible"
    INSUFFICIENT = "insufficient_information"


class Procurement(Base):
    __tablename__ = "procurements"
    id: Mapped[int] = mapped_column(primary_key=True)
    official_control_number: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    organization_cnpj: Mapped[str] = mapped_column(String(14), index=True)
    organization_name: Mapped[str] = mapped_column(String(300))
    unit_code: Mapped[str | None] = mapped_column(String(40))
    unit_name: Mapped[str | None] = mapped_column(String(300))
    year: Mapped[int] = mapped_column(index=True)
    sequence: Mapped[int]
    purchase_number: Mapped[str | None] = mapped_column(String(100))
    process_number: Mapped[str | None] = mapped_column(String(100))
    object_description: Mapped[str] = mapped_column(Text)
    additional_information: Mapped[str | None] = mapped_column(Text)
    modality_id: Mapped[int | None] = mapped_column(index=True)
    modality_name: Mapped[str | None] = mapped_column(String(120), index=True)
    dispute_mode_name: Mapped[str | None] = mapped_column(String(120))
    judgment_criterion_name: Mapped[str | None] = mapped_column(String(160))
    status_id: Mapped[int | None]
    status_name: Mapped[str | None] = mapped_column(String(120))
    is_price_confidential: Mapped[bool] = mapped_column(Boolean, default=False)
    estimated_total_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    proposal_open_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    proposal_close_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    uf: Mapped[str | None] = mapped_column(String(2), index=True)
    municipality: Mapped[str | None] = mapped_column(String(160), index=True)
    municipality_ibge: Mapped[str | None] = mapped_column(String(12))
    source_system_url: Mapped[str | None] = mapped_column(Text)
    pncp_url: Mapped[str] = mapped_column(Text)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    analysis_status: Mapped[AnalysisStatus] = mapped_column(
        Enum(AnalysisStatus, native_enum=False), default=AnalysisStatus.NEW, index=True
    )
    is_open: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    items: Mapped[list[ProcurementItem]] = relationship(back_populates="procurement", cascade="all, delete-orphan")
    lots: Mapped[list[ProcurementLot]] = relationship(back_populates="procurement", cascade="all, delete-orphan")
    documents: Mapped[list[ProcurementDocument]] = relationship(back_populates="procurement", cascade="all, delete-orphan")
    versions: Mapped[list[ProcurementVersion]] = relationship(back_populates="procurement", cascade="all, delete-orphan")
    delivery_requirement: Mapped[DeliveryRequirement | None] = relationship(back_populates="procurement", cascade="all, delete-orphan", uselist=False)
    scores: Mapped[list[OpportunityScore]] = relationship(back_populates="procurement", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("organization_cnpj", "year", "sequence", name="uq_procurement_official_key"),
        Index("ix_procurements_radar", "is_open", "analysis_status", "proposal_close_at"),
    )


class ProcurementLot(Base):
    __tablename__ = "procurement_lots"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), index=True)
    lot_number: Mapped[str] = mapped_column(String(60))
    description: Mapped[str | None] = mapped_column(Text)
    estimated_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    judgment_type: Mapped[str | None] = mapped_column(String(80))
    procurement: Mapped[Procurement] = relationship(back_populates="lots")
    items: Mapped[list[ProcurementItem]] = relationship(back_populates="lot")
    __table_args__ = (UniqueConstraint("procurement_id", "lot_number", name="uq_lot_procurement_number"),)


class ProcurementItem(Base):
    __tablename__ = "procurement_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), index=True)
    lot_id: Mapped[int | None] = mapped_column(ForeignKey("procurement_lots.id", ondelete="SET NULL"), index=True)
    item_number: Mapped[int]
    material_or_service: Mapped[str | None] = mapped_column(String(30))
    description: Mapped[str] = mapped_column(Text)
    complementary_description: Mapped[str | None] = mapped_column(Text)
    catmat_catser_code: Mapped[str | None] = mapped_column(String(40), index=True)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    unit: Mapped[str | None] = mapped_column(String(80))
    unit_estimated_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    total_estimated_value: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    price_is_confidential: Mapped[bool] = mapped_column(Boolean, default=False)
    judgment_criterion: Mapped[str | None] = mapped_column(String(160))
    status: Mapped[str | None] = mapped_column(String(120))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    procurement: Mapped[Procurement] = relationship(back_populates="items")
    lot: Mapped[ProcurementLot | None] = relationship(back_populates="items")
    quotes: Mapped[list[SupplierQuote]] = relationship(back_populates="item")
    financial_scenarios: Mapped[list[FinancialScenario]] = relationship(back_populates="item", cascade="all, delete-orphan")
    __table_args__ = (UniqueConstraint("procurement_id", "item_number", name="uq_procurement_item_number"),)


class ProcurementDocument(Base):
    __tablename__ = "procurement_documents"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), index=True)
    sequence: Mapped[int]
    title: Mapped[str | None] = mapped_column(String(500))
    document_type: Mapped[str | None] = mapped_column(String(160))
    source_url: Mapped[str] = mapped_column(Text)
    file_name: Mapped[str | None] = mapped_column(String(300))
    content_type: Mapped[str | None] = mapped_column(String(120))
    local_path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str | None] = mapped_column(String(64), index=True)
    size_bytes: Mapped[int | None]
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_status: Mapped[str] = mapped_column(String(40), default="pending", index=True)
    processing_error: Mapped[str | None] = mapped_column(Text)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    procurement: Mapped[Procurement] = relationship(back_populates="documents")
    extractions: Mapped[list[DocumentExtraction]] = relationship(back_populates="document", cascade="all, delete-orphan")
    __table_args__ = (UniqueConstraint("procurement_id", "sequence", name="uq_procurement_document_sequence"),)


class ProcurementVersion(Base):
    __tablename__ = "procurement_versions"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), index=True)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    content_hash: Mapped[str] = mapped_column(String(64))
    change_type: Mapped[str] = mapped_column(String(40), default="update")
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    procurement: Mapped[Procurement] = relationship(back_populates="versions")
    __table_args__ = (UniqueConstraint("procurement_id", "content_hash", name="uq_procurement_version_hash"),)


class DocumentExtraction(Base):
    __tablename__ = "document_extractions"
    id: Mapped[int] = mapped_column(primary_key=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("procurement_documents.id", ondelete="CASCADE"), index=True)
    field_name: Mapped[str] = mapped_column(String(100), index=True)
    normalized_value: Mapped[str | None] = mapped_column(Text)
    page_number: Mapped[int | None]
    excerpt: Mapped[str] = mapped_column(Text)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4))
    extraction_method: Mapped[str] = mapped_column(String(40), default="regex")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    document: Mapped[ProcurementDocument] = relationship(back_populates="extractions")


class DeliveryRequirement(Base):
    __tablename__ = "delivery_requirements"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), unique=True)
    remote_delivery_score: Mapped[int] = mapped_column(default=0)
    classification: Mapped[DeliveryClass] = mapped_column(Enum(DeliveryClass, native_enum=False), default=DeliveryClass.UNDETERMINED)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), default=Decimal(0))
    direct_ship_score: Mapped[int] = mapped_column(default=0)
    requires_installation: Mapped[bool | None]
    requires_assembly: Mapped[bool | None]
    requires_site_visit: Mapped[bool | None]
    delivery_locations_count: Mapped[int | None]
    delivery_deadline_days: Mapped[int | None]
    delivery_address: Mapped[str | None] = mapped_column(Text)
    justifications: Mapped[list[str]] = mapped_column(JSON, default=list)
    pending_checks: Mapped[list[str]] = mapped_column(JSON, default=list)
    evidence_ids: Mapped[list[int]] = mapped_column(JSON, default=list)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    procurement: Mapped[Procurement] = relationship(back_populates="delivery_requirement")


class Supplier(Base):
    __tablename__ = "suppliers"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(250), index=True)
    tax_id: Mapped[str | None] = mapped_column(String(20), unique=True)
    origin_uf: Mapped[str | None] = mapped_column(String(2))
    contact_name: Mapped[str | None] = mapped_column(String(160))
    email: Mapped[str | None] = mapped_column(String(250))
    phone: Mapped[str | None] = mapped_column(String(40))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    products: Mapped[list[SupplierProduct]] = relationship(back_populates="supplier", cascade="all, delete-orphan")


class SupplierProduct(Base):
    __tablename__ = "supplier_products"
    id: Mapped[int] = mapped_column(primary_key=True)
    supplier_id: Mapped[int] = mapped_column(ForeignKey("suppliers.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column(String(300), index=True)
    brand: Mapped[str | None] = mapped_column(String(120))
    model: Mapped[str | None] = mapped_column(String(160))
    manufacturer_code: Mapped[str | None] = mapped_column(String(120))
    sku: Mapped[str | None] = mapped_column(String(120))
    technical_description: Mapped[str | None] = mapped_column(Text)
    specifications: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    product_url: Mapped[str | None] = mapped_column(Text)
    warranty: Mapped[str | None] = mapped_column(String(200))
    direct_shipping: Mapped[bool | None]
    supplier: Mapped[Supplier] = relationship(back_populates="products")
    quotes: Mapped[list[SupplierQuote]] = relationship(back_populates="product", cascade="all, delete-orphan")
    __table_args__ = (UniqueConstraint("supplier_id", "sku", name="uq_supplier_sku"),)


class SupplierQuote(Base):
    __tablename__ = "supplier_quotes"
    id: Mapped[int] = mapped_column(primary_key=True)
    supplier_product_id: Mapped[int] = mapped_column(ForeignKey("supplier_products.id", ondelete="CASCADE"), index=True)
    procurement_item_id: Mapped[int | None] = mapped_column(ForeignKey("procurement_items.id", ondelete="SET NULL"), index=True)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    minimum_quantity: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    reported_stock: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    quoted_at: Mapped[date] = mapped_column(Date, default=date.today)
    valid_until: Mapped[date | None] = mapped_column(Date, index=True)
    dispatch_days: Mapped[int | None]
    estimated_freight: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    freight_known: Mapped[bool] = mapped_column(Boolean, default=False)
    direct_shipping: Mapped[bool | None]
    quote_document_url: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(30), default="manual")
    freight_scope: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    product: Mapped[SupplierProduct] = relationship(back_populates="quotes")
    item: Mapped[ProcurementItem | None] = relationship(back_populates="quotes")


class ProductMatch(Base):
    __tablename__ = "product_matches"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_item_id: Mapped[int] = mapped_column(ForeignKey("procurement_items.id", ondelete="CASCADE"), index=True)
    supplier_product_id: Mapped[int] = mapped_column(ForeignKey("supplier_products.id", ondelete="CASCADE"), index=True)
    classification: Mapped[MatchClass] = mapped_column(Enum(MatchClass, native_enum=False))
    score: Mapped[int]
    reasons: Mapped[list[str]] = mapped_column(JSON, default=list)
    mandatory_spec_checks: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    reviewed_by_human: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    __table_args__ = (UniqueConstraint("procurement_item_id", "supplier_product_id", name="uq_product_match"),)


class HistoricalPrice(Base):
    __tablename__ = "historical_prices"
    id: Mapped[int] = mapped_column(primary_key=True)
    description: Mapped[str] = mapped_column(Text)
    catmat_catser_code: Mapped[str | None] = mapped_column(String(40), index=True)
    unit: Mapped[str | None] = mapped_column(String(80))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    reference_date: Mapped[date]
    source: Mapped[str] = mapped_column(String(60))
    source_url: Mapped[str | None] = mapped_column(Text)
    comparability_score: Mapped[int] = mapped_column(default=0)
    raw_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class OpportunityAnalysis(Base):
    __tablename__ = "opportunity_analyses"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), index=True)
    item_id: Mapped[int | None] = mapped_column(ForeignKey("procurement_items.id", ondelete="CASCADE"), index=True)
    eligibility_status: Mapped[str] = mapped_column(String(40), default="pending")
    blockers: Mapped[list[str]] = mapped_column(JSON, default=list)
    warnings: Mapped[list[str]] = mapped_column(JSON, default=list)
    data_completeness: Mapped[int] = mapped_column(default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class FinancialScenario(Base):
    __tablename__ = "financial_scenarios"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_item_id: Mapped[int] = mapped_column(ForeignKey("procurement_items.id", ondelete="CASCADE"), index=True)
    supplier_quote_id: Mapped[int | None] = mapped_column(ForeignKey("supplier_quotes.id", ondelete="SET NULL"), index=True)
    name: Mapped[str] = mapped_column(String(40), default="base")
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    sale_unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    acquisition_unit_cost: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    freight: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    packaging: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    insurance: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    tax_rate: Mapped[Decimal] = mapped_column(Numeric(8, 5), default=0)
    financial_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    installation_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    contingency: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=0)
    payment_days: Mapped[int] = mapped_column(default=30)
    available_capital: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    target_margin_rate: Mapped[Decimal] = mapped_column(Numeric(8, 5), default=Decimal("0.15"))
    revenue: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    total_cost: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    estimated_result: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    margin_rate: Mapped[Decimal] = mapped_column(Numeric(8, 5))
    required_capital: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    break_even_unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    target_margin_unit_price: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    sensitivity: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    assumptions_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    supplier_quote_unit_price_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    supplier_quote_freight_snapshot: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    supplier_quote_valid_until_snapshot: Mapped[date | None] = mapped_column(Date, nullable=True)
    capital_source: Mapped[str | None] = mapped_column(String(40), nullable=True)
    freight_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    taxes_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    item: Mapped[ProcurementItem] = relationship(back_populates="financial_scenarios")
    supplier_quote: Mapped[SupplierQuote | None] = relationship()


class OpportunityScore(Base):
    __tablename__ = "opportunity_scores"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), index=True)
    item_id: Mapped[int | None] = mapped_column(ForeignKey("procurement_items.id", ondelete="CASCADE"), index=True)
    score: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), index=True)
    provisional: Mapped[bool] = mapped_column(Boolean, default=True)
    eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    eligibility: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    components: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    blockers: Mapped[list[str]] = mapped_column(JSON, default=list)
    explanation: Mapped[str] = mapped_column(Text)
    pending_checks: Mapped[list[str]] = mapped_column(JSON, default=list)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    procurement: Mapped[Procurement] = relationship(back_populates="scores")


class OpportunityStatusHistory(Base):
    __tablename__ = "opportunity_status_history"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_id: Mapped[int] = mapped_column(ForeignKey("procurements.id", ondelete="CASCADE"), index=True)
    from_status: Mapped[str | None] = mapped_column(String(40))
    to_status: Mapped[str] = mapped_column(String(40))
    note: Mapped[str | None] = mapped_column(Text)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"
    id: Mapped[int] = mapped_column(primary_key=True)
    job_type: Mapped[str] = mapped_column(String(50), index=True)
    status: Mapped[str] = mapped_column(String(30), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    records_seen: Mapped[int] = mapped_column(default=0)
    records_created: Mapped[int] = mapped_column(default=0)
    records_updated: Mapped[int] = mapped_column(default=0)
    error_count: Mapped[int] = mapped_column(default=0)
    last_error: Mapped[str | None] = mapped_column(Text)
    retry_of_id: Mapped[int | None] = mapped_column(ForeignKey("ingestion_jobs.id"))


class PartnerSearchRun(Base):
    __tablename__ = "partner_search_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    procurement_item_id: Mapped[int] = mapped_column(ForeignKey("procurement_items.id", ondelete="CASCADE"), index=True)
    query: Mapped[str | None] = mapped_column(String(300))
    uf: Mapped[str | None] = mapped_column(String(2))
    published_from: Mapped[date | None] = mapped_column(Date)
    published_to: Mapped[date | None] = mapped_column(Date)
    max_results: Mapped[int] = mapped_column(default=10)
    status: Mapped[str] = mapped_column(String(30), default="success")
    complete: Mapped[bool] = mapped_column(Boolean, default=False)
    request_count: Mapped[int] = mapped_column(default=0)
    consulted_window: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    queried_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    source: Mapped[str] = mapped_column(String(80), default="pncp")
    source_version: Mapped[str | None] = mapped_column(String(80))
    limitations: Mapped[list[str]] = mapped_column(JSON, default=list)
    parameters_hash: Mapped[str] = mapped_column(String(64), index=True)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    __table_args__ = (UniqueConstraint("parameters_hash", name="uq_partner_search_parameters_hash"),)


class PartnerCandidate(Base):
    __tablename__ = "partner_candidates"
    id: Mapped[int] = mapped_column(primary_key=True)
    legal_name: Mapped[str] = mapped_column(String(300), index=True)
    tax_id: Mapped[str] = mapped_column(String(20), index=True)
    discovery_status: Mapped[str] = mapped_column(String(30), default="discovered")
    comparability: Mapped[str] = mapped_column(String(30), default="pending")
    reasons: Mapped[list[str]] = mapped_column(JSON, default=list)
    matched_terms: Mapped[list[str]] = mapped_column(JSON, default=list)
    supplier_id: Mapped[int | None] = mapped_column(ForeignKey("suppliers.id", ondelete="SET NULL"), nullable=True, index=True)
    evidence_count: Mapped[int] = mapped_column(default=0)
    observations: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    last_observed_at: Mapped[date | None] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    __table_args__ = (UniqueConstraint("tax_id", name="uq_partner_candidate_tax_id"),)


class PartnerObservation(Base):
    __tablename__ = "partner_observations"
    id: Mapped[int] = mapped_column(primary_key=True)
    candidate_id: Mapped[int] = mapped_column(ForeignKey("partner_candidates.id", ondelete="CASCADE"), index=True)
    search_run_id: Mapped[int | None] = mapped_column(ForeignKey("partner_search_runs.id", ondelete="SET NULL"), nullable=True, index=True)
    procurement_item_id: Mapped[int | None] = mapped_column(ForeignKey("procurement_items.id", ondelete="SET NULL"), nullable=True, index=True)
    official_control_number: Mapped[str] = mapped_column(String(100))
    item_number: Mapped[int]
    result_sequence: Mapped[int]
    event_key: Mapped[str] = mapped_column(String(200), index=True)
    source_url: Mapped[str] = mapped_column(Text)
    source_payload_hash: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    description: Mapped[str | None] = mapped_column(Text)
    unit: Mapped[str | None] = mapped_column(String(80))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    result_date: Mapped[date | None] = mapped_column(Date)
    person_type: Mapped[str | None] = mapped_column(String(4))
    status: Mapped[str | None] = mapped_column(String(120))
    price_nature: Mapped[str | None] = mapped_column(String(40))
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    __table_args__ = (UniqueConstraint("event_key", "source_payload_hash", name="uq_partner_observation_event_version"),)


class SystemSetting(Base):
    __tablename__ = "system_settings"
    key: Mapped[str] = mapped_column(String(120), primary_key=True)
    value: Mapped[Any] = mapped_column(JSON)
    description: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

