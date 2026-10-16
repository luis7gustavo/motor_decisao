import csv
import io
import math
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from app.core.time import local_today
from app.db import get_db
from app.documents.service import process_pending_documents
from app.finance.service import calculate_financial_scenario
from app.ingestion.service import run_sync
from app.models import (
    AnalysisStatus,
    DeliveryClass,
    DeliveryRequirement,
    FinancialScenario,
    IngestionJob,
    OpportunityScore,
    OpportunityStatusHistory,
    Procurement,
    ProcurementDocument,
    ProcurementItem,
    Supplier,
    SupplierProduct,
    SupplierQuote,
    SystemSetting,
)
from app.ranking.service import calculate_score, evaluate_item
from app.schemas import (
    DashboardSummary,
    FinancialScenarioCreate,
    FinancialScenarioOut,
    OpportunityListItem,
    PaginatedOpportunities,
    ProcurementDetail,
    ProductMatchOut,
    SettingsUpdate,
    StatusUpdate,
    SupplierCreate,
    SupplierOut,
    SupplierProductCreate,
    SupplierProductOut,
    SupplierQuoteCreate,
    SyncRequest,
    SyncResult,
)
from app.suppliers.service import match_product

router = APIRouter(prefix="/api/v1")
DB = Annotated[Session, Depends(get_db)]


def not_found(resource: str = "Resource") -> HTTPException:
    return HTTPException(status_code=404, detail=f"{resource} not found")


@router.get("/health")
def health(db: DB):
    db.execute(select(1))
    last_job = db.scalar(select(IngestionJob).order_by(IngestionJob.started_at.desc()))
    return {
        "status": "ok",
        "database": "ok",
        "pncp_last_job": last_job.status if last_job else "never_run",
        "checked_at": datetime.now(UTC),
    }


@router.get("/dashboard/summary", response_model=DashboardSummary)
def dashboard_summary(db: DB):
    now = datetime.now(UTC)
    today_start = now - timedelta(days=1)
    open_count = db.scalar(select(func.count(Procurement.id)).where(Procurement.is_open.is_(True))) or 0
    new_count = db.scalar(select(func.count(Procurement.id)).where(Procurement.collected_at >= today_start)) or 0
    analyzed = db.scalar(select(func.count(Procurement.id)).where(Procurement.analysis_status != AnalysisStatus.NEW)) or 0
    latest_score_ids = (
        select(func.max(OpportunityScore.id).label("id"))
        .where(OpportunityScore.item_id.is_not(None))
        .group_by(OpportunityScore.item_id)
        .subquery()
    )
    viable = db.scalar(
        select(func.count(func.distinct(OpportunityScore.procurement_id)))
        .join(latest_score_ids, latest_score_ids.c.id == OpportunityScore.id)
        .join(Procurement, Procurement.id == OpportunityScore.procurement_id)
        .where(OpportunityScore.eligible.is_(True), Procurement.is_open.is_(True), Procurement.proposal_close_at >= now)
    ) or 0
    remote = db.scalar(select(func.count(DeliveryRequirement.id)).where(DeliveryRequirement.classification == DeliveryClass.FAVORABLE)) or 0
    closing = db.scalar(
        select(func.count(Procurement.id)).where(
            Procurement.is_open.is_(True),
            Procurement.proposal_close_at >= now,
            Procurement.proposal_close_at <= now + timedelta(days=3),
        )
    ) or 0
    last_job = db.scalar(select(IngestionJob).order_by(IngestionJob.started_at.desc()))
    return DashboardSummary(
        open_opportunities=open_count,
        new_opportunities=new_count,
        analyzed_opportunities=analyzed,
        financially_viable=viable,
        remote_favorable=remote,
        closing_soon=closing,
        last_sync_at=last_job.finished_at if last_job else None,
        last_sync_status=last_job.status if last_job else None,
    )


@router.get("/opportunities", response_model=PaginatedOpportunities)
def list_opportunities(
    db: DB,
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=10, le=100),
    q: str | None = None,
    uf: str | None = None,
    municipality: str | None = None,
    modality: str | None = None,
    min_value: Annotated[Decimal | None, Query(ge=0)] = None,
    max_value: Annotated[Decimal | None, Query(ge=0)] = None,
    min_margin: Decimal | None = None,
    max_capital: Annotated[Decimal | None, Query(ge=0)] = None,
    delivery: DeliveryClass | None = None,
    status: AnalysisStatus | None = None,
    supplier_available: bool | None = None,
    days_remaining: int | None = Query(None, ge=0),
    open_only: bool = True,
):
    query = select(Procurement)
    if open_only:
        query = query.where(Procurement.is_open.is_(True))
    if q:
        term = f"%{q.strip()}%"
        query = query.where(or_(Procurement.object_description.ilike(term), Procurement.organization_name.ilike(term)))
    if uf:
        query = query.where(Procurement.uf == uf.upper())
    if municipality:
        query = query.where(Procurement.municipality.ilike(f"%{municipality}%"))
    if modality:
        query = query.where(Procurement.modality_name.ilike(f"%{modality}%"))
    if min_value is not None:
        query = query.where(Procurement.estimated_total_value >= min_value)
    if max_value is not None:
        query = query.where(Procurement.estimated_total_value <= max_value)
    if status:
        query = query.where(Procurement.analysis_status == status)
    if days_remaining is not None:
        query = query.where(Procurement.proposal_close_at <= datetime.now(UTC) + timedelta(days=days_remaining))
    if delivery:
        query = query.join(DeliveryRequirement).where(DeliveryRequirement.classification == delivery)
    if min_margin is not None or max_capital is not None:
        query = query.join(ProcurementItem).join(FinancialScenario)
        if min_margin is not None:
            query = query.where(FinancialScenario.margin_rate >= min_margin)
        if max_capital is not None:
            query = query.where(FinancialScenario.required_capital <= max_capital)
    if supplier_available is not None:
        quote_exists = select(SupplierQuote.id).join(ProcurementItem).where(ProcurementItem.procurement_id == Procurement.id).exists()
        query = query.where(quote_exists if supplier_available else ~quote_exists)

    count_query = select(func.count()).select_from(query.order_by(None).subquery())
    total = db.scalar(count_query) or 0
    procurements = list(
        db.scalars(
            query.options(selectinload(Procurement.items), selectinload(Procurement.delivery_requirement))
            .distinct()
            .order_by(Procurement.proposal_close_at.asc().nullslast())
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).unique()
    )
    rows = []
    for procurement in procurements:
        score = db.scalar(select(OpportunityScore).where(OpportunityScore.procurement_id == procurement.id).order_by(OpportunityScore.id.desc()))
        quotes = list(
            db.scalars(
                select(SupplierQuote)
                .join(ProcurementItem)
                .where(ProcurementItem.procurement_id == procurement.id)
                .order_by(SupplierQuote.unit_price)
            )
        )
        scenarios = list(
            db.scalars(
                select(FinancialScenario)
                .join(ProcurementItem)
                .where(ProcurementItem.procurement_id == procurement.id, FinancialScenario.name == "base")
                .order_by(FinancialScenario.created_at.desc())
            )
        )
        rows.append(
            OpportunityListItem(
                **{key: getattr(procurement, key) for key in OpportunityListItem.model_fields if hasattr(procurement, key)},
                score=score.score if score else None,
                score_provisional=score.provisional if score else True,
                delivery_classification=procurement.delivery_requirement.classification if procurement.delivery_requirement else None,
                remote_delivery_score=procurement.delivery_requirement.remote_delivery_score if procurement.delivery_requirement else None,
                lowest_quote=quotes[0].unit_price if quotes else None,
                simulated_margin=scenarios[0].margin_rate if scenarios else None,
                required_capital=scenarios[0].required_capital if scenarios else None,
                item_count=len(procurement.items),
            )
        )
    return PaginatedOpportunities(data=rows, page=page, page_size=page_size, total=total, total_pages=math.ceil(total / page_size))


@router.get("/opportunities/{procurement_id}", response_model=ProcurementDetail)
def get_opportunity(procurement_id: int, db: DB):
    procurement = db.scalar(
        select(Procurement)
        .where(Procurement.id == procurement_id)
        .options(
            selectinload(Procurement.items),
            selectinload(Procurement.documents).selectinload(ProcurementDocument.extractions),
            selectinload(Procurement.delivery_requirement),
        )
    )
    if not procurement:
        raise not_found("Opportunity")
    result = ProcurementDetail.model_validate(procurement)
    scores = list(db.scalars(select(OpportunityScore).where(OpportunityScore.procurement_id == procurement.id).order_by(OpportunityScore.id.desc())))
    result.latest_score = scores[0] if scores else None
    latest_by_item: dict[int | None, OpportunityScore] = {}
    for score in scores:
        latest_by_item.setdefault(score.item_id, score)
    result.item_analyses = [score for score in latest_by_item.values() if score.item_id is not None]
    return result


@router.patch("/opportunities/{procurement_id}/status")
def update_opportunity_status(procurement_id: int, payload: StatusUpdate, db: DB):
    procurement = db.get(Procurement, procurement_id)
    if not procurement:
        raise not_found("Opportunity")
    previous = procurement.analysis_status
    procurement.analysis_status = payload.status
    db.add(
        OpportunityStatusHistory(
            procurement_id=procurement.id,
            from_status=previous.value if previous else None,
            to_status=payload.status.value,
            note=payload.note,
        )
    )
    db.commit()
    return {"id": procurement.id, "status": procurement.analysis_status}


@router.post("/opportunities/{procurement_id}/documents/process")
async def process_opportunity_documents(procurement_id: int, db: DB):
    if not db.get(Procurement, procurement_id):
        raise not_found("Opportunity")
    return await process_pending_documents(db, procurement_id=procurement_id, limit=50)


@router.post("/sync/pncp", response_model=SyncResult)
async def sync_pncp(payload: SyncRequest, db: DB):
    end_date = payload.end_date or local_today()
    start_date = payload.start_date or end_date - timedelta(days=3)
    job = await run_sync(
        db,
        mode=payload.mode,
        start_date=start_date,
        end_date=end_date,
        modality_ids=payload.modality_ids,
        max_pages=payload.max_pages,
        hydrate_details=payload.hydrate_details,
    )
    return SyncResult(job_id=job.id, status=job.status, records_seen=job.records_seen, records_created=job.records_created, records_updated=job.records_updated, error_count=job.error_count)


@router.get("/sync/jobs")
def list_jobs(db: DB, limit: int = Query(20, ge=1, le=100)):
    return list(db.scalars(select(IngestionJob).order_by(IngestionJob.started_at.desc()).limit(limit)))


@router.get("/suppliers", response_model=list[SupplierOut])
def list_suppliers(db: DB):
    return list(db.scalars(select(Supplier).options(selectinload(Supplier.products).selectinload(SupplierProduct.quotes)).order_by(Supplier.name)))


@router.post("/suppliers", response_model=SupplierOut, status_code=201)
def create_supplier(payload: SupplierCreate, db: DB):
    supplier = Supplier(**payload.model_dump())
    db.add(supplier)
    db.commit()
    db.refresh(supplier)
    return supplier


@router.post("/suppliers/{supplier_id}/products", response_model=SupplierProductOut, status_code=201)
def create_supplier_product(supplier_id: int, payload: SupplierProductCreate, db: DB):
    if not db.get(Supplier, supplier_id):
        raise not_found("Supplier")
    values = payload.model_dump()
    if values.get("product_url"):
        values["product_url"] = str(values["product_url"])
    product = SupplierProduct(supplier_id=supplier_id, **values)
    db.add(product)
    db.commit()
    db.refresh(product)
    return product


@router.post("/supplier-products/{product_id}/quotes", status_code=201)
def create_quote(product_id: int, payload: SupplierQuoteCreate, db: DB):
    product = db.get(SupplierProduct, product_id)
    if not product:
        raise not_found("Supplier product")
    if payload.procurement_item_id and not db.get(ProcurementItem, payload.procurement_item_id):
        raise not_found("Procurement item")
    values = payload.model_dump()
    if values.get("quote_document_url"):
        values["quote_document_url"] = str(values["quote_document_url"])
    quote = SupplierQuote(supplier_product_id=product_id, source="manual", **values)
    db.add(quote)
    if payload.procurement_item_id:
        item = db.get(ProcurementItem, payload.procurement_item_id)
        match_product(db, item, product)
        db.flush()
        procurement = db.scalar(
            select(Procurement)
            .where(Procurement.id == item.procurement_id)
            .options(selectinload(Procurement.delivery_requirement))
        )
        calculate_score(db, procurement, item)
    db.commit()
    db.refresh(quote)
    return quote


@router.put("/suppliers/{supplier_id}", response_model=SupplierOut)
def update_supplier(supplier_id: int, payload: SupplierCreate, db: DB):
    supplier = db.get(Supplier, supplier_id)
    if not supplier:
        raise not_found("Supplier")
    for key, value in payload.model_dump().items():
        setattr(supplier, key, value)
    db.commit()
    db.refresh(supplier)
    return supplier


@router.put("/supplier-products/{product_id}", response_model=SupplierProductOut)
def update_supplier_product(product_id: int, payload: SupplierProductCreate, db: DB):
    product = db.get(SupplierProduct, product_id)
    if not product:
        raise not_found("Supplier product")
    values = payload.model_dump()
    if values.get("product_url"):
        values["product_url"] = str(values["product_url"])
    for key, value in values.items():
        setattr(product, key, value)
    db.commit()
    db.refresh(product)
    return product


@router.put("/quotes/{quote_id}")
def update_quote(quote_id: int, payload: SupplierQuoteCreate, db: DB):
    quote = db.get(SupplierQuote, quote_id)
    if not quote:
        raise not_found("Quote")
    values = payload.model_dump()
    if values.get("quote_document_url"):
        values["quote_document_url"] = str(values["quote_document_url"])
    for key, value in values.items():
        setattr(quote, key, value)
    if quote.procurement_item_id:
        item = db.get(ProcurementItem, quote.procurement_item_id)
        product = db.get(SupplierProduct, quote.supplier_product_id)
        match_product(db, item, product)
        procurement = db.scalar(
            select(Procurement)
            .where(Procurement.id == item.procurement_id)
            .options(selectinload(Procurement.delivery_requirement))
        )
        calculate_score(db, procurement, item)
    db.commit()
    db.refresh(quote)
    return quote


@router.post("/supplier-products/{product_id}/match/{item_id}", response_model=ProductMatchOut)
def calculate_product_match(product_id: int, item_id: int, db: DB):
    product = db.get(SupplierProduct, product_id)
    item = db.get(ProcurementItem, item_id)
    if not product or not item:
        raise not_found("Product or procurement item")
    result = match_product(db, item, product)
    db.commit()
    db.refresh(result)
    return result


@router.post("/suppliers/import-csv")
async def import_supplier_csv(db: DB, file: Annotated[UploadFile, File()]):
    content = await file.read()
    if len(content) > 5_000_000:
        raise HTTPException(413, "CSV exceeds 5 MB")
    try:
        text = content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
    except UnicodeDecodeError as exc:
        raise HTTPException(422, "CSV must be UTF-8") from exc
    required = {"fornecedor", "produto", "preco_unitario"}
    if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
        raise HTTPException(422, f"Required columns: {', '.join(sorted(required))}")
    created = 0
    errors: list[dict] = []
    for number, row in enumerate(reader, start=2):
        try:
            supplier = db.scalar(select(Supplier).where(Supplier.name == row["fornecedor"].strip()))
            if supplier is None:
                supplier = Supplier(name=row["fornecedor"].strip(), origin_uf=(row.get("uf_origem") or None))
                db.add(supplier)
                db.flush()
            sku = (row.get("sku") or "").strip() or None
            product = db.scalar(select(SupplierProduct).where(SupplierProduct.supplier_id == supplier.id, SupplierProduct.sku == sku)) if sku else None
            if product is None:
                product = SupplierProduct(
                    supplier_id=supplier.id,
                    name=row["produto"].strip(),
                    brand=(row.get("marca") or None),
                    model=(row.get("modelo") or None),
                    manufacturer_code=(row.get("codigo_fabricante") or None),
                    sku=sku,
                    technical_description=(row.get("descricao_tecnica") or None),
                    direct_shipping=(row.get("envio_direto", "").lower() in {"sim", "true", "1"}),
                )
                db.add(product)
                db.flush()
            quote = SupplierQuote(
                supplier_product_id=product.id,
                unit_price=Decimal(row["preco_unitario"].replace(",", ".")),
                quoted_at=date.fromisoformat(row["data_cotacao"]) if row.get("data_cotacao") else local_today(),
                valid_until=date.fromisoformat(row["validade"]) if row.get("validade") else None,
                estimated_freight=Decimal(row["frete"].replace(",", ".")) if row.get("frete") else None,
                freight_known=bool(row.get("frete")),
                source="csv",
            )
            db.add(quote)
            created += 1
        except Exception as exc:  # noqa: BLE001 - one invalid row must not abort the import
            errors.append({"line": number, "error": str(exc)})
    db.commit()
    return {"created_quotes": created, "errors": errors}


@router.post("/items/{item_id}/financial-scenarios", response_model=FinancialScenarioOut, status_code=201)
def create_financial_scenario(item_id: int, payload: FinancialScenarioCreate, db: DB):
    item = db.get(ProcurementItem, item_id)
    if not item:
        raise not_found("Procurement item")
    quote = db.get(SupplierQuote, payload.supplier_quote_id) if payload.supplier_quote_id else None
    if payload.supplier_quote_id and not quote:
        raise not_found("Supplier quote")
    if quote and quote.procurement_item_id not in (None, item_id):
        raise HTTPException(422, "Selected quote belongs to another procurement item")
    if quote and quote.procurement_item_id is None:
        quote.procurement_item_id = item_id
    effective_payload = payload
    if quote:
        effective_payload = payload.model_copy(update={"acquisition_unit_cost": quote.unit_price, "freight": quote.estimated_freight or payload.freight, "freight_known": quote.freight_known and quote.estimated_freight is not None})
    calculated = calculate_financial_scenario(effective_payload)
    values = effective_payload.model_dump(exclude={"freight_known", "taxes_confirmed", "supplier_quote_id"})
    scenario = FinancialScenario(
        procurement_item_id=item_id,
        supplier_quote_id=quote.id if quote else None,
        supplier_quote_unit_price_snapshot=quote.unit_price if quote else None,
        supplier_quote_freight_snapshot=quote.estimated_freight if quote else None,
        supplier_quote_valid_until_snapshot=quote.valid_until if quote else None,
        capital_source="request" if effective_payload.available_capital is not None else "settings_or_unknown",
        freight_confirmed=effective_payload.freight_known,
        taxes_confirmed=effective_payload.taxes_confirmed,
        **values,
        **calculated,
    )
    db.add(scenario)
    db.flush()
    procurement = db.get(Procurement, item.procurement_id)
    settings_rows = {row.key: row.value for row in db.scalars(select(SystemSetting))}
    available = settings_rows.get("available_capital")
    minimum = settings_rows.get("minimum_margin", "0.10")
    configured_weights = settings_rows.get("score_weights")
    weights = {key: Decimal(str(value)) for key, value in configured_weights.items()} if configured_weights else None
    calculate_score(
        db,
        procurement,
        item,
        minimum_margin=Decimal(str(minimum)),
        available_capital=payload.available_capital if payload.available_capital is not None else Decimal(str(available)) if available is not None else None,
        weights=weights,
    )
    db.commit()
    db.refresh(scenario)
    return scenario


@router.post("/opportunities/{procurement_id}/rank")
def rank_opportunity(procurement_id: int, db: DB):
    procurement = db.scalar(select(Procurement).where(Procurement.id == procurement_id).options(selectinload(Procurement.items), selectinload(Procurement.delivery_requirement)))
    if not procurement:
        raise not_found("Opportunity")
    settings_rows = {row.key: row.value for row in db.scalars(select(SystemSetting))}
    configured_weights = settings_rows.get("score_weights")
    weights = {key: Decimal(str(value)) for key, value in configured_weights.items()} if configured_weights else None
    minimum_margin = Decimal(str(settings_rows.get("minimum_margin", "0.10")))
    results = [
        calculate_score(db, procurement, item, minimum_margin=minimum_margin, weights=weights)
        for item in procurement.items
    ] or [calculate_score(db, procurement, minimum_margin=minimum_margin, weights=weights)]
    db.commit()
    return results


@router.get("/settings")
def get_settings(db: DB):
    return {row.key: row.value for row in db.scalars(select(SystemSetting))}


@router.put("/settings")
def update_settings(payload: SettingsUpdate, db: DB):
    values = payload.model_dump(exclude_none=True, mode="json")
    for key, value in values.items():
        row = db.get(SystemSetting, key)
        if row is None:
            row = SystemSetting(key=key, value=value)
            db.add(row)
        else:
            row.value = value
    db.commit()
    return values

