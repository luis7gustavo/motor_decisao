from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import DeliveryClass, DeliveryRequirement, DocumentExtraction, ProcurementDocument


def classify_logistics(db: Session, procurement_id: int) -> DeliveryRequirement:
    evidence = list(
        db.scalars(
            select(DocumentExtraction)
            .join(ProcurementDocument)
            .where(ProcurementDocument.procurement_id == procurement_id)
        )
    )
    requirement = db.scalar(select(DeliveryRequirement).where(DeliveryRequirement.procurement_id == procurement_id))
    if requirement is None:
        requirement = DeliveryRequirement(procurement_id=procurement_id)
        db.add(requirement)

    by_field: dict[str, list[DocumentExtraction]] = {}
    for item in evidence:
        by_field.setdefault(item.field_name, []).append(item)

    positives = []
    negatives = []
    conditions = []
    pending = []
    required_installation = any(e.normalized_value == "required" for e in by_field.get("installation", []))
    denied_installation = any(e.normalized_value == "not_required" for e in by_field.get("installation", []))
    required_assembly = any(e.normalized_value == "required" for e in by_field.get("assembly", []))
    required_visit = any(e.normalized_value == "required" for e in by_field.get("site_visit", []))
    in_person = bool(by_field.get("in_person"))
    contradictory = required_installation and denied_installation

    if by_field.get("delivery_address"):
        positives.append("Há indicação de local/endereço de entrega no documento")
    else:
        pending.append("Confirmar endereço completo de entrega")
    if by_field.get("delivery_deadline"):
        positives.append("Prazo de entrega identificado")
    else:
        pending.append("Confirmar prazo contratual de entrega")
    if required_assembly:
        conditions.append("Montagem exigida ou incluída")
    if required_installation:
        conditions.append("Instalação exigida")
    if by_field.get("multiple_locations"):
        conditions.append("Possibilidade de múltiplos locais de entrega")
    if by_field.get("split_delivery"):
        conditions.append("Entrega parcelada ou sob demanda")

    if contradictory:
        classification, score, confidence = DeliveryClass.UNDETERMINED, 35, Decimal("0.45")
        pending.append("Cláusulas contraditórias sobre instalação exigem revisão humana")
    elif in_person or required_visit or required_installation:
        classification, score, confidence = DeliveryClass.LOCAL_REQUIRED, 15, Decimal("0.88")
        negatives.append("Foi identificada obrigação presencial, visita técnica ou instalação")
        pending.append("Avaliar terceirização local sem presumir conformidade contratual")
    elif conditions:
        classification, score, confidence = DeliveryClass.CONDITIONAL, 58, Decimal("0.72")
    elif len(positives) >= 2:
        classification, score, confidence = DeliveryClass.FAVORABLE, 84, Decimal("0.78")
        positives.append("Nenhuma obrigação presencial explícita foi localizada")
    else:
        classification, score, confidence = DeliveryClass.UNDETERMINED, 25, Decimal("0.40")
        pending.append("Documentação insuficiente para confirmar entrega remota")

    deadlines = [int(e.normalized_value) for e in by_field.get("delivery_deadline", []) if (e.normalized_value or "").isdigit()]
    addresses = [e.normalized_value for e in by_field.get("delivery_address", []) if e.normalized_value]
    direct_ship_score = 0
    if classification == DeliveryClass.FAVORABLE:
        direct_ship_score += 45
    elif classification == DeliveryClass.CONDITIONAL:
        direct_ship_score += 25
    if addresses:
        direct_ship_score += 15
    if deadlines:
        direct_ship_score += 10
    pending.extend(["Confirmar emissão fiscal direta ao órgão", "Confirmar capacidade e prazo do fornecedor"])

    requirement.remote_delivery_score = score
    requirement.classification = classification
    requirement.confidence = confidence
    requirement.direct_ship_score = min(direct_ship_score, 70)
    requirement.requires_installation = required_installation if by_field.get("installation") else None
    requirement.requires_assembly = required_assembly if by_field.get("assembly") else None
    requirement.requires_site_visit = required_visit if by_field.get("site_visit") else None
    requirement.delivery_locations_count = 2 if by_field.get("multiple_locations") else (1 if addresses else None)
    requirement.delivery_deadline_days = min(deadlines) if deadlines else None
    requirement.delivery_address = addresses[0] if addresses else None
    requirement.justifications = positives + conditions + negatives
    requirement.pending_checks = list(dict.fromkeys(pending))
    requirement.evidence_ids = [e.id for e in evidence]
    db.flush()
    return requirement

