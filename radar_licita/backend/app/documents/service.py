import hashlib
import re
from pathlib import Path

import pymupdf
import structlog
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.ingestion.pncp_client import PNCPClient
from app.logistics.service import classify_logistics
from app.models import DocumentExtraction, ProcurementDocument

log = structlog.get_logger()

FIELD_PATTERNS: dict[str, list[tuple[re.Pattern[str], str, float]]] = {
    "installation": [
        (re.compile(r"(?i)\b(?:dever[aá]|ser[aá]\s+necess[aá]ri[oa])[^.]{0,80}\binstala(?:ç|c)[aã]o\b"), "required", 0.9),
        (re.compile(r"(?i)\b(?:n[aã]o\s+(?:ser[aá]\s+)?(?:exigid[oa]|necess[aá]ri[oa])|dispensad[oa])[^.]{0,60}\binstala(?:ç|c)[aã]o\b"), "not_required", 0.9),
        (re.compile(r"(?i)\binstala(?:ç|c)[aã]o\b"), "mentioned", 0.55),
    ],
    "assembly": [
        (re.compile(r"(?i)\b(?:com|inclui(?:ndo)?|exige)[^.]{0,50}\bmontagem\b"), "required", 0.85),
        (re.compile(r"(?i)\bsem\s+(?:necessidade\s+de\s+)?montagem\b"), "not_required", 0.9),
        (re.compile(r"(?i)\bmontagem\b"), "mentioned", 0.55),
    ],
    "site_visit": [
        (re.compile(r"(?i)\bvisita\s+t[eé]cnica\s+(?:obrigat[oó]ria|dever[aá])"), "required", 0.95),
        (re.compile(r"(?i)\bvisita\s+t[eé]cnica\s+(?:facultativa|opcional|dispensada)"), "not_required", 0.95),
        (re.compile(r"(?i)\bvisita\s+t[eé]cnica\b"), "mentioned", 0.6),
    ],
    "in_person": [
        (re.compile(r"(?i)\b(?:execu(?:ç|c)[aã]o|presta(?:ç|c)[aã]o|atendimento)\s+presencial\b"), "required", 0.95),
        (re.compile(r"(?i)\bpresencialmente\b"), "required", 0.8),
    ],
    "delivery_deadline": [
        (re.compile(r"(?i)(?:prazo\s+(?:m[aá]ximo\s+)?(?:de\s+)?entrega|entrega\s+em)\D{0,40}(\d{1,3})\s*dias"), "days", 0.9),
    ],
    "delivery_address": [
        (re.compile(r"(?i)(?:local|endere(?:ç|c)o)\s+(?:de\s+)?entrega\s*[:\-]\s*([^\n]{10,220})"), "address", 0.82),
        (re.compile(r"(?i)entreg(?:a|ues)\s+(?:no|na|em)\s+([^\n.]{10,180})"), "address", 0.62),
    ],
    "multiple_locations": [
        (re.compile(r"(?i)\b(?:diversos|m[uú]ltiplos|v[aá]rios)\s+(?:endere(?:ç|c)os|locais)\b"), "true", 0.9),
        (re.compile(r"(?i)\bunidades\s+(?:requisitantes|escolares|de\s+sa[uú]de)\b"), "possible", 0.65),
    ],
    "split_delivery": [
        (re.compile(r"(?i)\bentrega(?:s)?\s+(?:parcelada|fracionada|sob\s+demanda)\b"), "required", 0.9),
    ],
    "warranty": [(re.compile(r"(?i)\bgarantia\s+(?:m[ií]nima\s+)?(?:de\s+)?(\d{1,3})\s*(meses|anos|dias)"), "term", 0.85)],
    "technical_assistance": [(re.compile(r"(?i)\bassist[eê]ncia\s+t[eé]cnica\b"), "mentioned", 0.75)],
    "payment": [(re.compile(r"(?i)(?:pagamento)[^.]{0,100}(\d{1,3})\s*dias"), "days", 0.82)],
    "qualification": [(re.compile(r"(?i)\b(?:qualifica(?:ç|c)[aã]o\s+t[eé]cnica|atestado\s+de\s+capacidade\s+t[eé]cnica|habilita(?:ç|c)[aã]o)\b"), "mentioned", 0.7)],
    "judgment_scope": [
        (re.compile(r"(?i)\bjulgamento\s+(?:ser[aá]\s+)?por\s+item\b"), "item", 0.9),
        (re.compile(r"(?i)\bjulgamento\s+(?:ser[aá]\s+)?por\s+(?:lote|grupo)\b"), "lot", 0.9),
    ],
}


def _context(text: str, start: int, end: int, radius: int = 180) -> str:
    excerpt = re.sub(r"\s+", " ", text[max(0, start - radius) : min(len(text), end + radius)]).strip()
    return excerpt[:600]


def extract_page_evidence(text: str, page_number: int) -> list[dict]:
    evidence: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for field, rules in FIELD_PATTERNS.items():
        for pattern, normalized, confidence in rules:
            for match in pattern.finditer(text):
                value = normalized
                if field in {"delivery_deadline", "payment"} and match.groups():
                    value = match.group(1)
                elif field == "delivery_address" and match.groups():
                    value = match.group(1).strip()[:300]
                elif field == "warranty" and match.groups():
                    value = " ".join(group for group in match.groups() if group)
                excerpt = _context(text, match.start(), match.end())
                key = (field, value, excerpt)
                if key not in seen:
                    evidence.append(
                        {
                            "field_name": field,
                            "normalized_value": value,
                            "page_number": page_number,
                            "excerpt": excerpt,
                            "confidence": confidence,
                            "extraction_method": "regex",
                        }
                    )
                    seen.add(key)
    return evidence


async def process_document(db: Session, document: ProcurementDocument, client: PNCPClient | None = None) -> ProcurementDocument:
    owned_client = client is None
    client = client or PNCPClient()
    try:
        content = await client.download_document(document.source_url)
        digest = hashlib.sha256(content).hexdigest()
        suffix = ".pdf" if content.startswith(b"%PDF") else Path(document.file_name or "").suffix.lower()
        storage = settings.document_storage_path.resolve()
        storage.mkdir(parents=True, exist_ok=True)
        target = (storage / f"{document.id}-{digest[:16]}{suffix or '.bin'}").resolve()
        if storage not in target.parents:
            raise ValueError("Unsafe document path")
        target.write_bytes(content)
        document.local_path = str(target)
        document.sha256 = digest
        document.size_bytes = len(content)
        document.content_type = "application/pdf" if content.startswith(b"%PDF") else "application/octet-stream"
        db.execute(delete(DocumentExtraction).where(DocumentExtraction.document_id == document.id))
        if not content.startswith(b"%PDF"):
            document.processing_status = "manual_review"
            document.processing_error = "Formato não PDF: preservado para análise manual"
            db.commit()
            return document

        pdf = pymupdf.open(stream=content, filetype="pdf")
        if pdf.page_count > 500:
            raise ValueError("PDF exceeds the 500-page processing limit")
        extracted_count = 0
        for index, page in enumerate(pdf):
            text = page.get_text("text")
            for row in extract_page_evidence(text, index + 1):
                db.add(DocumentExtraction(document_id=document.id, **row))
                extracted_count += 1
        document.processing_status = "processed" if extracted_count else "manual_review"
        document.processing_error = None if extracted_count else "PDF legível, mas nenhuma cláusula-alvo foi identificada"
        db.flush()
        classify_logistics(db, document.procurement_id)
        db.commit()
    except Exception as exc:  # noqa: BLE001 - failures are persisted for manual review
        db.rollback()
        document = db.get(ProcurementDocument, document.id)
        document.processing_status = "failed"
        document.processing_error = str(exc)[:2000]
        db.commit()
        log.warning("document_processing_failed", document_id=document.id, error=str(exc))
    finally:
        if owned_client:
            await client.__aexit__(None, None, None)
    return document


async def process_pending_documents(db: Session, procurement_id: int | None = None, limit: int = 20) -> dict[str, int]:
    query = select(ProcurementDocument).where(ProcurementDocument.processing_status == "pending")
    if procurement_id is not None:
        query = query.where(ProcurementDocument.procurement_id == procurement_id)
    documents = list(db.scalars(query.limit(limit)))
    stats = {"processed": 0, "failed": 0, "manual_review": 0}
    async with PNCPClient() as client:
        for document in documents:
            result = await process_document(db, document, client)
            stats[result.processing_status] = stats.get(result.processing_status, 0) + 1
    return stats

