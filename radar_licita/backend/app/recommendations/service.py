from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from app.models import Procurement, ProcurementItem, SystemSetting
from app.ranking.service import DEFAULT_WEIGHTS, evaluate_item, normalize_instant

RULES_VERSION = "s04-budget-v1"


def _settings(db: Session) -> tuple[Decimal, dict[str, Decimal]]:
    values = {row.key: row.value for row in db.scalars(select(SystemSetting))}
    minimum = Decimal(str(values.get("minimum_margin", "0.10")))
    raw_weights = values.get("score_weights")
    weights = {key: Decimal(str(value)) for key, value in raw_weights.items()} if isinstance(raw_weights, dict) else DEFAULT_WEIGHTS
    return minimum, weights


def _page(items: list[dict], page: int, page_size: int) -> list[dict]:
    start = (page - 1) * page_size
    return items[start : start + page_size]


def _sort_recommendations(item: dict) -> tuple:
    return (-item["score"], -item["estimated_result"], item["proposal_close_at"] or datetime.max.replace(tzinfo=UTC), item["procurement_id"], item["item_id"])


def _sort_other(item: dict) -> tuple:
    return (item["proposal_close_at"] or datetime.max.replace(tzinfo=UTC), item["procurement_id"], item["item_id"])


def search_recommendations(db: Session, payload) -> dict:
    evaluated_at = datetime.now(UTC)
    minimum_margin, weights = _settings(db)
    query = (
        select(Procurement)
        .options(selectinload(Procurement.items), selectinload(Procurement.delivery_requirement))
        .where(Procurement.is_open.is_(True), Procurement.proposal_close_at.is_not(None))
    )
    if payload.uf:
        query = query.where(Procurement.uf == payload.uf.upper())
    if payload.q and payload.q.strip():
        term = f"%{payload.q.strip()}%"
        query = query.where(or_(Procurement.object_description.ilike(term), Procurement.items.any(ProcurementItem.description.ilike(term))))
    procurements = list(db.scalars(query).unique())
    results = {"recommendations": [], "needs_analysis": [], "outside_budget": [], "blocked": []}

    for procurement in procurements:
        close_at = normalize_instant(procurement.proposal_close_at)
        if close_at and close_at < evaluated_at:
            continue
        for item in sorted(procurement.items, key=lambda value: value.id):
            evaluation = evaluate_item(db, procurement, item, minimum_margin=minimum_margin, available_capital=None, use_scenario_capital=False, weights=weights)
            scenario = evaluation.scenario
            required = scenario.required_capital if scenario else None
            remaining = payload.available_capital - required if payload.available_capital is not None and required is not None else None
            non_financial_blockers = list(evaluation.blockers)
            budget_exceeded = required is not None and payload.available_capital is not None and required > payload.available_capital
            if budget_exceeded:
                non_financial_blockers = [reason for reason in non_financial_blockers if reason != "Capital disponível insuficiente"]

            pending = list(evaluation.pending_checks)
            if payload.available_capital is None:
                pending.append("Orçamento disponível não informado")
            if non_financial_blockers:
                group = "blocked"
                eligibility = "blocked"
                score = None
            elif pending:
                group = "needs_analysis"
                eligibility = "pending"
                score = None
            elif budget_exceeded:
                group = "outside_budget"
                eligibility = "viable"
                score = evaluation.score
            else:
                group = "recommendations"
                eligibility = "viable"
                score = evaluation.score

            quote = evaluation.quote
            if scenario is None:
                cost_basis = "unknown"
            elif quote is not None:
                cost_basis = "quoted"
            else:
                cost_basis = "manual_estimate"
            explanation = evaluation.explanation
            if budget_exceeded and not non_financial_blockers:
                explanation = f"Análise viável sob as premissas registradas, mas exige R$ {required:.2f} de capital."
            elif payload.available_capital is None and not non_financial_blockers:
                explanation = "Informe o orçamento para confirmar se o item cabe no capital disponível."
            record = {
                "procurement_id": procurement.id,
                "item_id": item.id,
                "scenario_id": scenario.id if scenario else None,
                "supplier_quote_id": scenario.supplier_quote_id if scenario else None,
                "eligibility": eligibility,
                "score": score,
                "required_capital": required,
                "budget_remaining": remaining,
                "estimated_result": scenario.estimated_result if scenario else None,
                "margin_rate": scenario.margin_rate if scenario else None,
                "proposal_close_at": close_at,
                "pending_checks": list(dict.fromkeys(pending)),
                "blockers": list(dict.fromkeys(non_financial_blockers)),
                "explanation": explanation,
                "cost_basis": cost_basis,
                "origin": {"rules_version": RULES_VERSION, "source": "local_database", "scenario_id": scenario.id if scenario else None, "quote_id": quote.id if quote else None},
            }
            results[group].append(record)

    results["recommendations"].sort(key=_sort_recommendations)
    for key in ("needs_analysis", "outside_budget", "blocked"):
        results[key].sort(key=_sort_other)
    totals = {key: len(value) for key, value in results.items()}
    return {
        "available_capital": payload.available_capital,
        "evaluated_at": evaluated_at,
        "rules_version": RULES_VERSION,
        **{key: _page(value, payload.page, payload.page_size) for key, value in results.items()},
        "totals": totals,
        "page": payload.page,
        "page_size": payload.page_size,
    }

