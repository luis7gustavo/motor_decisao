import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.ingestion.pncp_client import PNCPClient
from app.models import (
    IngestionJob,
    Procurement,
    ProcurementDocument,
    ProcurementItem,
    ProcurementVersion,
    SystemSetting,
    utcnow,
)

log = structlog.get_logger()


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def positive_money(value: Any, confidential: bool = False) -> Decimal | None:
    if confidential or value in (None, ""):
        return None
    try:
        amount = Decimal(str(value))
        return amount if amount > 0 else None
    except (InvalidOperation, TypeError):
        return None


def _canonical_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


def upsert_procurement(db: Session, payload: dict[str, Any], *, force_open: bool | None = None) -> tuple[Procurement, bool, bool]:
    control = payload["numeroControlePNCP"]
    procurement = db.scalar(select(Procurement).where(Procurement.official_control_number == control))
    created = procurement is None
    if procurement is None:
        procurement = Procurement(official_control_number=control, raw_json=payload)
        db.add(procurement)

    org = payload.get("orgaoEntidade") or {}
    unit = payload.get("unidadeOrgao") or {}
    confidential = bool(payload.get("orcamentoSigiloso")) or positive_money(payload.get("valorTotalEstimado")) is None
    close_at = parse_datetime(payload.get("dataEncerramentoProposta"))
    now = datetime.now(UTC)
    values = {
        "organization_cnpj": org.get("cnpj", ""),
        "organization_name": org.get("razaoSocial") or "Órgão não informado",
        "unit_code": unit.get("codigoUnidade"),
        "unit_name": unit.get("nomeUnidade"),
        "year": int(payload.get("anoCompra") or 0),
        "sequence": int(payload.get("sequencialCompra") or 0),
        "purchase_number": payload.get("numeroCompra"),
        "process_number": payload.get("processo"),
        "object_description": payload.get("objetoCompra") or "Objeto não informado",
        "additional_information": payload.get("informacaoComplementar"),
        "modality_id": payload.get("modalidadeId"),
        "modality_name": payload.get("modalidadeNome"),
        "dispute_mode_name": payload.get("modoDisputaNome"),
        "judgment_criterion_name": payload.get("criterioJulgamentoNome"),
        "status_id": payload.get("situacaoCompraId"),
        "status_name": payload.get("situacaoCompraNome"),
        "is_price_confidential": confidential,
        "estimated_total_value": positive_money(payload.get("valorTotalEstimado"), confidential),
        "proposal_open_at": parse_datetime(payload.get("dataAberturaProposta")),
        "proposal_close_at": close_at,
        "published_at": parse_datetime(payload.get("dataPublicacaoPncp")),
        "source_updated_at": parse_datetime(payload.get("dataAtualizacaoGlobal") or payload.get("dataAtualizacao")),
        "uf": unit.get("ufSigla"),
        "municipality": unit.get("municipioNome"),
        "municipality_ibge": unit.get("codigoIbge"),
        "source_system_url": payload.get("linkSistemaOrigem") or payload.get("linkProcessoEletronico"),
        "pncp_url": f"https://pncp.gov.br/app/editais/{org.get('cnpj')}/{payload.get('anoCompra')}/{payload.get('sequencialCompra')}",
        "raw_json": payload,
        "is_open": force_open if force_open is not None else bool(close_at and close_at >= now),
    }
    for key, value in values.items():
        setattr(procurement, key, value)

    db.flush()
    content_hash = _canonical_hash(payload)
    version_exists = db.scalar(
        select(ProcurementVersion.id).where(
            ProcurementVersion.procurement_id == procurement.id,
            ProcurementVersion.content_hash == content_hash,
        )
    )
    changed = version_exists is None
    if changed:
        db.add(
            ProcurementVersion(
                procurement_id=procurement.id,
                source_updated_at=procurement.source_updated_at,
                content_hash=content_hash,
                change_type="initial" if created else "update",
                raw_json=payload,
            )
        )
    return procurement, created, changed


def upsert_items(db: Session, procurement: Procurement, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        number = int(row.get("numeroItem") or 0)
        item = db.scalar(
            select(ProcurementItem).where(
                ProcurementItem.procurement_id == procurement.id,
                ProcurementItem.item_number == number,
            )
        )
        if item is None:
            item = ProcurementItem(procurement_id=procurement.id, item_number=number, description="", raw_json=row)
            db.add(item)
        confidential = bool(row.get("orcamentoSigiloso"))
        item.description = row.get("descricao") or "Item sem descrição"
        item.complementary_description = row.get("informacaoComplementar")
        item.material_or_service = row.get("materialOuServico")
        item.catmat_catser_code = row.get("catalogoCodigoItem") or row.get("codigoItemCatalogo")
        item.quantity = positive_money(row.get("quantidade"))
        item.unit = row.get("unidadeMedida")
        item.unit_estimated_value = positive_money(row.get("valorUnitarioEstimado"), confidential)
        item.total_estimated_value = positive_money(row.get("valorTotal"), confidential)
        item.price_is_confidential = confidential or item.unit_estimated_value is None
        item.judgment_criterion = row.get("criterioJulgamentoNome")
        item.status = row.get("situacaoCompraItemNome")
        item.raw_json = row


def upsert_documents(db: Session, procurement: Procurement, rows: list[dict[str, Any]]) -> list[ProcurementDocument]:
    changed_docs: list[ProcurementDocument] = []
    for row in rows:
        sequence = int(row.get("sequencialDocumento") or 0)
        doc = db.scalar(
            select(ProcurementDocument).where(
                ProcurementDocument.procurement_id == procurement.id,
                ProcurementDocument.sequence == sequence,
            )
        )
        if doc is None:
            doc = ProcurementDocument(
                procurement_id=procurement.id,
                sequence=sequence,
                source_url=row.get("url") or row.get("uri"),
                raw_json=row,
            )
            db.add(doc)
            changed_docs.append(doc)
        elif doc.raw_json != row:
            doc.processing_status = "pending"
            changed_docs.append(doc)
        doc.title = row.get("titulo")
        doc.document_type = row.get("tipoDocumentoNome") or row.get("tipoDocumentoDescricao")
        doc.source_url = row.get("url") or row.get("uri")
        doc.file_name = row.get("titulo")
        doc.source_updated_at = parse_datetime(row.get("dataPublicacaoPncp"))
        doc.raw_json = row
    return changed_docs


async def hydrate_procurement(db: Session, client: PNCPClient, procurement: Procurement) -> None:
    items = await client.get_items(procurement.organization_cnpj, procurement.year, procurement.sequence)
    docs = await client.get_documents(procurement.organization_cnpj, procurement.year, procurement.sequence)
    upsert_items(db, procurement, items)
    changed_docs = upsert_documents(db, procurement, docs)
    if changed_docs and procurement.delivery_requirement:
        from app.models import DeliveryClass

        procurement.delivery_requirement.classification = DeliveryClass.UNDETERMINED
        procurement.delivery_requirement.remote_delivery_score = 0
        procurement.delivery_requirement.pending_checks = ["Documentos alterados; reprocessamento necessário"]


async def run_sync(
    db: Session,
    *,
    mode: str,
    start_date: date,
    end_date: date,
    modality_ids: list[int],
    max_pages: int | None,
    hydrate_details: bool,
) -> IngestionJob:
    job = IngestionJob(
        job_type=f"pncp_{mode}",
        status="running",
        parameters={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "modality_ids": modality_ids,
            "max_pages": max_pages,
            "hydrate_details": hydrate_details,
        },
    )
    db.add(job)
    db.commit()
    try:
        async with PNCPClient() as client:
            if mode == "open":
                iterator = client.iter_open_proposals(end_date, max_pages=max_pages)
                async for payload in iterator:
                    job.records_seen += 1
                    procurement, created, changed = upsert_procurement(db, payload, force_open=True)
                    job.records_created += int(created)
                    job.records_updated += int(changed and not created)
                    if hydrate_details:
                        try:
                            await hydrate_procurement(db, client, procurement)
                        except Exception as exc:  # noqa: BLE001 - one record must not abort collection
                            job.error_count += 1
                            job.last_error = str(exc)[:2000]
                            log.warning("pncp_hydration_failed", control=procurement.official_control_number, error=str(exc))
                    db.commit()
            else:
                for modality_id in modality_ids:
                    async for payload in client.iter_by_period(mode, start_date, end_date, modality_id, max_pages):
                        job.records_seen += 1
                        procurement, created, changed = upsert_procurement(db, payload)
                        job.records_created += int(created)
                        job.records_updated += int(changed and not created)
                        if hydrate_details and changed:
                            try:
                                await hydrate_procurement(db, client, procurement)
                            except Exception as exc:  # noqa: BLE001 - one record must not abort collection
                                job.error_count += 1
                                job.last_error = str(exc)[:2000]
                        db.commit()
        job.status = "completed_with_errors" if job.error_count else "completed"
    except Exception as exc:
        db.rollback()
        job = db.get(IngestionJob, job.id)
        job.status = "failed"
        job.error_count += 1
        job.last_error = str(exc)[:2000]
        log.exception("pncp_sync_failed", job_id=job.id)
    job.finished_at = utcnow()
    db.merge(SystemSetting(key="last_sync", value={"at": job.finished_at.isoformat(), "status": job.status}, description="Latest PNCP synchronization"))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        current = db.get(SystemSetting, "last_sync")
        if current:
            current.value = {"at": job.finished_at.isoformat(), "status": job.status}
        db.commit()
    return job

