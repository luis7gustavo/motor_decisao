from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl, model_validator

from app.models import AnalysisStatus, DeliveryClass, MatchClass


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class ProcurementItemOut(ORMModel):
    id: int
    item_number: int
    description: str
    complementary_description: str | None
    material_or_service: str | None
    quantity: Decimal | None
    unit: str | None
    unit_estimated_value: Decimal | None
    total_estimated_value: Decimal | None
    price_is_confidential: bool
    judgment_criterion: str | None
    status: str | None


class EvidenceOut(ORMModel):
    id: int
    field_name: str
    normalized_value: str | None
    page_number: int | None
    excerpt: str
    confidence: Decimal
    extraction_method: str


class DocumentOut(ORMModel):
    id: int
    sequence: int
    title: str | None
    document_type: str | None
    source_url: str
    content_type: str | None
    size_bytes: int | None
    processing_status: str
    processing_error: str | None
    extractions: list[EvidenceOut] = []


class DeliveryOut(ORMModel):
    remote_delivery_score: int
    classification: DeliveryClass
    confidence: Decimal
    direct_ship_score: int
    requires_installation: bool | None
    requires_assembly: bool | None
    requires_site_visit: bool | None
    delivery_locations_count: int | None
    delivery_deadline_days: int | None
    delivery_address: str | None
    justifications: list[str]
    pending_checks: list[str]
    evidence_ids: list[int]


class ScoreOut(ORMModel):
    item_id: int | None = None
    eligibility: Literal["viable", "pending", "blocked"] = "pending"
    score: Decimal | None
    provisional: bool
    eligible: bool
    components: dict[str, Any]
    blockers: list[str]
    pending_checks: list[str] = []
    explanation: str
    calculated_at: datetime


class OpportunityListItem(ORMModel):
    id: int
    official_control_number: str
    object_description: str
    organization_name: str
    municipality: str | None
    uf: str | None
    modality_name: str | None
    estimated_total_value: Decimal | None
    is_price_confidential: bool
    proposal_close_at: datetime | None
    pncp_url: str
    source_system_url: str | None
    analysis_status: AnalysisStatus
    is_open: bool
    score: Decimal | None = None
    score_provisional: bool = True
    delivery_classification: DeliveryClass | None = None
    remote_delivery_score: int | None = None
    lowest_quote: Decimal | None = None
    simulated_margin: Decimal | None = None
    required_capital: Decimal | None = None
    item_count: int = 0


class ProcurementDetail(ORMModel):
    id: int
    official_control_number: str
    organization_cnpj: str
    organization_name: str
    unit_name: str | None
    year: int
    sequence: int
    purchase_number: str | None
    process_number: str | None
    object_description: str
    additional_information: str | None
    modality_name: str | None
    dispute_mode_name: str | None
    judgment_criterion_name: str | None
    status_name: str | None
    is_price_confidential: bool
    estimated_total_value: Decimal | None
    proposal_open_at: datetime | None
    proposal_close_at: datetime | None
    published_at: datetime | None
    source_updated_at: datetime | None
    uf: str | None
    municipality: str | None
    source_system_url: str | None
    pncp_url: str
    analysis_status: AnalysisStatus
    is_open: bool
    items: list[ProcurementItemOut]
    documents: list[DocumentOut]
    delivery_requirement: DeliveryOut | None
    latest_score: ScoreOut | None = None
    item_analyses: list[ScoreOut] = []


class PaginatedOpportunities(BaseModel):
    data: list[OpportunityListItem]
    page: int
    page_size: int
    total: int
    total_pages: int


class DashboardSummary(BaseModel):
    open_opportunities: int
    new_opportunities: int
    analyzed_opportunities: int
    financially_viable: int
    remote_favorable: int
    closing_soon: int
    last_sync_at: datetime | None
    last_sync_status: str | None


class SupplierCreate(BaseModel):
    name: str = Field(min_length=2, max_length=250)
    tax_id: str | None = Field(default=None, max_length=20)
    origin_uf: str | None = Field(default=None, min_length=2, max_length=2)
    contact_name: str | None = None
    email: str | None = None
    phone: str | None = None


class SupplierProductCreate(BaseModel):
    name: str
    brand: str | None = None
    model: str | None = None
    manufacturer_code: str | None = None
    sku: str | None = None
    technical_description: str | None = None
    specifications: dict[str, Any] = {}
    product_url: HttpUrl | None = None
    warranty: str | None = None
    direct_shipping: bool | None = None


class SupplierQuoteCreate(BaseModel):
    procurement_item_id: int | None = None
    unit_price: Decimal = Field(gt=0)
    minimum_quantity: Decimal | None = Field(default=None, gt=0)
    reported_stock: Decimal | None = Field(default=None, ge=0)
    quoted_at: date = Field(default_factory=date.today)
    valid_until: date | None = None
    dispatch_days: int | None = Field(default=None, ge=0)
    estimated_freight: Decimal | None = Field(default=None, ge=0)
    freight_known: bool = False
    direct_shipping: bool | None = None
    quote_document_url: HttpUrl | None = None


class QuoteOut(ORMModel):
    id: int
    procurement_item_id: int | None = None
    unit_price: Decimal
    valid_until: date | None
    dispatch_days: int | None
    estimated_freight: Decimal | None
    freight_known: bool
    direct_shipping: bool | None
    source: str
    freight_scope: dict[str, Any] | None = None


class SupplierProductOut(ORMModel):
    id: int
    name: str
    brand: str | None
    model: str | None
    sku: str | None
    technical_description: str | None
    specifications: dict[str, Any]
    direct_shipping: bool | None
    quotes: list[QuoteOut] = []


class SupplierOut(ORMModel):
    id: int
    name: str
    tax_id: str | None
    origin_uf: str | None
    contact_name: str | None
    email: str | None
    phone: str | None
    active: bool
    products: list[SupplierProductOut] = []


class ProductMatchOut(ORMModel):
    id: int
    procurement_item_id: int
    supplier_product_id: int
    classification: MatchClass
    score: int
    reasons: list[str]
    mandatory_spec_checks: dict[str, Any]
    reviewed_by_human: bool


class FinancialScenarioCreate(BaseModel):
    name: Literal["optimistic", "base", "conservative"] = "base"
    quantity: Decimal = Field(gt=0)
    sale_unit_price: Decimal = Field(gt=0)
    acquisition_unit_cost: Decimal = Field(gt=0)
    freight: Decimal = Field(default=Decimal(0), ge=0)
    packaging: Decimal = Field(default=Decimal(0), ge=0)
    insurance: Decimal = Field(default=Decimal(0), ge=0)
    tax_rate: Decimal = Field(default=Decimal(0), ge=0, lt=1)
    financial_cost: Decimal = Field(default=Decimal(0), ge=0)
    installation_cost: Decimal = Field(default=Decimal(0), ge=0)
    contingency: Decimal = Field(default=Decimal(0), ge=0)
    payment_days: int = Field(default=30, ge=0)
    available_capital: Decimal | None = Field(default=None, ge=0)
    target_margin_rate: Decimal = Field(default=Decimal("0.15"), ge=0, lt=1)
    freight_known: bool = True
    taxes_confirmed: bool = False
    supplier_quote_id: int | None = Field(default=None, gt=0)


class FinancialScenarioOut(ORMModel):
    id: int
    name: str
    quantity: Decimal
    sale_unit_price: Decimal
    acquisition_unit_cost: Decimal
    revenue: Decimal
    total_cost: Decimal
    estimated_result: Decimal
    margin_rate: Decimal
    required_capital: Decimal
    break_even_unit_price: Decimal
    target_margin_unit_price: Decimal
    sensitivity: dict[str, Any]
    assumptions_complete: bool
    supplier_quote_id: int | None = None
    supplier_quote_unit_price_snapshot: Decimal | None = None
    supplier_quote_freight_snapshot: Decimal | None = None
    supplier_quote_valid_until_snapshot: date | None = None
    capital_source: str | None = None
    freight_confirmed: bool = False
    taxes_confirmed: bool = False


class PartnerSearchCreate(BaseModel):
    query: str | None = Field(default=None, max_length=300)
    uf: str | None = Field(default=None, min_length=2, max_length=2)
    published_from: date | None = None
    published_to: date | None = None
    max_results: int = Field(default=10, ge=1, le=10)


class PartnerObservationOut(ORMModel):
    id: int
    official_control_number: str
    item_number: int
    result_sequence: int
    source_url: str
    observed_at: datetime
    description: str | None
    unit: str | None
    quantity: Decimal | None
    result_date: date | None
    person_type: str | None
    status: str | None
    price_nature: str | None
    unit_price: Decimal | None


class PartnerCandidateOut(ORMModel):
    id: int
    legal_name: str
    tax_id: str
    discovery_status: str
    comparability: str
    reasons: list[str]
    matched_terms: list[str]
    supplier_id: int | None
    evidence_count: int
    last_observed_at: date | None


class PartnerSearchOut(ORMModel):
    id: int
    procurement_item_id: int
    status: str
    complete: bool
    request_count: int
    consulted_window: dict[str, Any]
    queried_at: datetime
    limitations: list[str]
    candidates: list[PartnerCandidateOut] = []


class StatusUpdate(BaseModel):
    status: AnalysisStatus
    note: str | None = Field(default=None, max_length=1000)


class SyncRequest(BaseModel):
    mode: Literal["open", "publication", "updates"] = "open"
    start_date: date | None = None
    end_date: date | None = None
    modality_ids: list[int] = [4, 6, 8, 12, 16, 18]
    max_pages: int | None = Field(default=2, ge=1, le=200)
    hydrate_details: bool = True

    @model_validator(mode="after")
    def validate_period(self):
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be before end_date")
        return self


class SyncResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    job_id: int = Field(validation_alias=AliasChoices("job_id", "id"))
    status: str
    records_seen: int
    records_created: int
    records_updated: int
    error_count: int


class SettingsUpdate(BaseModel):
    origin_uf: str | None = None
    preferred_categories: list[str] | None = None
    available_capital: Decimal | None = Field(default=None, ge=0)
    minimum_margin: Decimal | None = Field(default=None, ge=0, lt=1)
    maximum_delivery_days: int | None = Field(default=None, ge=1)
    minimum_preparation_days: int | None = Field(default=None, ge=0)
    score_weights: dict[str, Decimal] | None = None
    sync_interval_minutes: int | None = Field(default=None, ge=5)
    tax_rate: Decimal | None = Field(default=None, ge=0, lt=1)
    prefer_remote_delivery: bool | None = None

    @model_validator(mode="after")
    def validate_score_weights(self):
        if self.score_weights is None:
            return self
        expected = {"margin", "remote_delivery", "capital", "supplier", "operations", "information"}
        if set(self.score_weights) != expected:
            raise ValueError(f"score_weights must contain exactly: {', '.join(sorted(expected))}")
        if any(value < 0 for value in self.score_weights.values()):
            raise ValueError("score weights cannot be negative")
        if abs(sum(self.score_weights.values()) - Decimal(1)) > Decimal("0.001"):
            raise ValueError("score weights must sum to 1")
        return self

