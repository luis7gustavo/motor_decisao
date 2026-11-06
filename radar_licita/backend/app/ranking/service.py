from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.time import local_today
from app.models import DeliveryClass, FinancialScenario, MatchClass, OpportunityScore, Procurement, ProcurementItem, ProductMatch, SupplierQuote

DEFAULT_WEIGHTS = {"margin": Decimal("0.30"), "remote_delivery": Decimal("0.25"), "capital": Decimal("0.15"), "supplier": Decimal("0.15"), "operations": Decimal("0.10"), "information": Decimal("0.05")}


@dataclass(frozen=True)
class ItemEvaluation:
    item_id: int | None
    eligibility: str
    score: Decimal | None
    provisional: bool
    eligible: bool
    components: dict[str, float]
    blockers: list[str]
    pending_checks: list[str]
    explanation: str
    scenario: FinancialScenario | None
    quote: SupplierQuote | None


def normalize_instant(value: datetime | None) -> datetime | None:
    """Normalize legacy SQLite naive instants as UTC before comparison."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _latest_scenario(db: Session, item: ProcurementItem | None) -> FinancialScenario | None:
    if item is None:
        return None
    return db.scalar(select(FinancialScenario).where(FinancialScenario.procurement_item_id == item.id, FinancialScenario.name == "base").order_by(FinancialScenario.created_at.desc(), FinancialScenario.id.desc()))


def evaluate_item(db: Session, procurement: Procurement, item: ProcurementItem | None = None, *, minimum_margin: Decimal = Decimal("0.10"), available_capital: Decimal | None = None, use_scenario_capital: bool = True, weights: dict[str, Decimal] | None = None) -> ItemEvaluation:
    """Evaluate an item without adding or updating any database row."""
    weights = weights or DEFAULT_WEIGHTS
    now = datetime.now(UTC)
    blockers: list[str] = []
    pending: list[str] = []
    components: dict[str, float] = {}
    scenario = _latest_scenario(db, item)
    if available_capital is None and use_scenario_capital and scenario is not None:
        available_capital = scenario.available_capital
    matches = list(db.scalars(select(ProductMatch).where(ProductMatch.procurement_item_id == item.id))) if item else []
    compatible_ids = [m.supplier_product_id for m in matches if m.classification == MatchClass.COMPATIBLE]
    quote = db.get(SupplierQuote, scenario.supplier_quote_id) if scenario and scenario.supplier_quote_id else None
    if scenario and quote and item and quote.procurement_item_id != item.id:
        blockers.append("Cotação selecionada vinculada a outro item")
    if not procurement.proposal_close_at or normalize_instant(procurement.proposal_close_at) < now:
        blockers.append("Prazo de propostas encerrado ou não identificado")
    if item:
        criterion = (item.judgment_criterion or procurement.judgment_criterion_name or "").lower()
        if "lote" in criterion or "grupo" in criterion:
            blockers.append("Julgamento por lote/grupo exige análise financeira do conjunto completo")
        if matches and all(match.classification == MatchClass.INCOMPATIBLE for match in matches):
            blockers.append("Produto comprovadamente incompatível")
        if not matches:
            pending.append("Compatibilidade do produto não avaliada")
        elif not compatible_ids:
            pending.append("Produto compatível ainda não confirmado")
    delivery = procurement.delivery_requirement
    if delivery and delivery.classification == DeliveryClass.LOCAL_REQUIRED:
        blockers.append("Operação presencial/local indicada")
    if delivery is None:
        pending.append("Logística não avaliada")
    elif delivery.classification == DeliveryClass.UNDETERMINED:
        pending.append("Logística de entrega não confirmada")
    if scenario is None:
        pending.append("Cenário financeiro não calculado")
    else:
        if scenario.margin_rate < minimum_margin:
            blockers.append("Margem abaixo do mínimo configurado")
        if available_capital is not None and scenario.required_capital > available_capital:
            blockers.append("Capital disponível insuficiente")
        if not scenario.assumptions_complete or not scenario.freight_confirmed or not scenario.taxes_confirmed:
            pending.append("Premissas de frete/tributos não confirmadas")
        if scenario.supplier_quote_id is None:
            pending.append("Cotação atual não selecionada")
        elif quote is None:
            blockers.append("Cotação selecionada não encontrada")
        else:
            if quote.valid_until is None:
                pending.append("Validade da cotação desconhecida")
            elif quote.valid_until < local_today():
                blockers.append("Cotação atual expirada")
            if not quote.freight_known or quote.estimated_freight is None:
                pending.append("Frete da cotação não confirmado")
            if scenario.supplier_quote_unit_price_snapshot is not None and scenario.acquisition_unit_cost != scenario.supplier_quote_unit_price_snapshot:
                pending.append("Custo de aquisição diverge do snapshot da cotação")
            if compatible_ids and quote.supplier_product_id not in compatible_ids:
                pending.append("Cotação não pertence a produto compatível confirmado")
    if scenario:
        components["margin"] = max(0, min(100, float(scenario.margin_rate) * 400))
        components["capital"] = 100 if available_capital is None else max(0, min(100, float(available_capital / max(scenario.required_capital, 1)) * 100))
    else:
        components["margin"], components["capital"] = 0, 30
    components["remote_delivery"] = delivery.remote_delivery_score if delivery else 0
    components["supplier"] = max([m.score for m in matches if m.classification == MatchClass.COMPATIBLE], default=0)
    close_at = normalize_instant(procurement.proposal_close_at)
    days_left = (close_at - now).days if close_at else 0
    components["operations"] = 85 if days_left >= 7 else 55 if days_left >= 3 else 20
    completeness = [procurement.estimated_total_value, close_at, item.quantity if item else None, item.unit if item else None, delivery]
    components["information"] = sum(value is not None for value in completeness) / len(completeness) * 100
    raw_score = sum(Decimal(str(components[name])) * weights.get(name, Decimal(0)) for name in components)
    if blockers:
        eligibility, score = "blocked", None
        explanation = "Oportunidade bloqueada: " + "; ".join(dict.fromkeys(blockers)) + "."
    elif pending:
        eligibility, score = "pending", None
        explanation = "Análise pendente: " + "; ".join(dict.fromkeys(pending)) + "."
    else:
        eligibility, score = "viable", raw_score.quantize(Decimal("0.01"))
        explanation = f"Análise viável sob as premissas registradas; margem de {float(scenario.margin_rate) * 100:.1f}%."
    return ItemEvaluation(item.id if item else None, eligibility, score, eligibility != "viable", eligibility == "viable", components, list(dict.fromkeys(blockers)), list(dict.fromkeys(pending)), explanation, scenario, quote)


def calculate_score(db: Session, procurement: Procurement, item: ProcurementItem | None = None, *, minimum_margin: Decimal = Decimal("0.10"), available_capital: Decimal | None = None, weights: dict[str, Decimal] | None = None) -> OpportunityScore:
    evaluation = evaluate_item(db, procurement, item, minimum_margin=minimum_margin, available_capital=available_capital, weights=weights)
    result = OpportunityScore(procurement_id=procurement.id, item_id=evaluation.item_id, score=evaluation.score, eligibility=evaluation.eligibility, provisional=evaluation.provisional, eligible=evaluation.eligible, components=evaluation.components, blockers=evaluation.blockers, pending_checks=evaluation.pending_checks, explanation=evaluation.explanation)
    db.add(result)
    db.flush()
    return result
