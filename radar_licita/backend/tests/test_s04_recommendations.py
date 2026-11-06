from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.db import get_db
from app.finance.service import calculate_financial_scenario
from app.models import DeliveryClass, DeliveryRequirement, FinancialScenario, MatchClass, ProcurementItem, Supplier, SupplierProduct, SupplierQuote
from app.recommendations.router import router
from app.recommendations.schemas import RecommendationSearchRequest
from app.recommendations.service import search_recommendations
from app.schemas import FinancialScenarioCreate
from app.suppliers.service import match_product


def _scenario(db, procurement, capital: str, item_number: int, acquisition_unit_cost: str = "100", sale_unit_price: str = "200"):
    item = ProcurementItem(procurement_id=procurement.id, item_number=item_number, description=f"Notebook {item_number}", quantity=10, unit="un", raw_json={})
    db.add(item)
    db.flush()
    supplier = Supplier(name=f"Fornecedor {item_number}", tax_id=f"12345678000{item_number:03d}")
    db.add(supplier)
    db.flush()
    product = SupplierProduct(supplier_id=supplier.id, name=item.description, technical_description=item.description, specifications={})
    db.add(product)
    db.flush()
    match = match_product(db, item, product)
    match.classification = MatchClass.COMPATIBLE
    quote = SupplierQuote(supplier_product_id=product.id, procurement_item_id=item.id, unit_price=Decimal(acquisition_unit_cost), valid_until=date.today() + timedelta(days=10), estimated_freight=Decimal("100"), freight_known=True)
    db.add(quote)
    db.flush()
    payload = FinancialScenarioCreate(
        quantity=10, sale_unit_price=Decimal(sale_unit_price), acquisition_unit_cost=Decimal(acquisition_unit_cost), freight=100, packaging=50,
        insurance=20, tax_rate=Decimal("0.10"), financial_cost=30, installation_cost=0,
        contingency=100, available_capital=Decimal(capital), freight_known=True, taxes_confirmed=True,
        supplier_quote_id=quote.id,
    )
    values = payload.model_dump(exclude={"freight_known", "taxes_confirmed", "supplier_quote_id"})
    calculated = calculate_financial_scenario(payload)
    db.add(FinancialScenario(
        procurement_item_id=item.id, supplier_quote_id=quote.id,
        supplier_quote_unit_price_snapshot=quote.unit_price,
        supplier_quote_freight_snapshot=quote.estimated_freight,
        supplier_quote_valid_until_snapshot=quote.valid_until,
        capital_source="request", freight_confirmed=True, taxes_confirmed=True,
        **values, **calculated,
    ))
    return item


def test_budget_groups_and_revalidates_saved_capital(db, procurement):
    procurement.proposal_close_at = datetime.now(UTC) + timedelta(days=10)
    procurement.is_open = True
    db.add(DeliveryRequirement(procurement_id=procurement.id, classification=DeliveryClass.FAVORABLE, remote_delivery_score=90, confidence=Decimal("0.9"), direct_ship_score=90, justifications=[], pending_checks=[], evidence_ids=[]))
    _scenario(db, procurement, "5000", 1)
    _scenario(db, procurement, "5000", 2, "700", "1000")
    db.commit()

    result = search_recommendations(db, RecommendationSearchRequest(available_capital=Decimal("5000")))
    assert result["totals"] == {"recommendations": 1, "needs_analysis": 0, "outside_budget": 1, "blocked": 0}
    assert result["recommendations"][0]["budget_remaining"] == Decimal("3500")
    assert result["outside_budget"][0]["budget_remaining"] == Decimal("-3300")
    assert db.query(__import__("app.models", fromlist=["OpportunityScore"]).OpportunityScore).count() == 0


def test_exact_and_one_cent_below_are_distinct(db, procurement):
    procurement.proposal_close_at = datetime.now(UTC) + timedelta(days=10)
    procurement.is_open = True
    db.add(DeliveryRequirement(procurement_id=procurement.id, classification=DeliveryClass.FAVORABLE, remote_delivery_score=90, confidence=Decimal("0.9"), direct_ship_score=90, justifications=[], pending_checks=[], evidence_ids=[]))
    _scenario(db, procurement, "5000", 1)
    db.commit()
    required = Decimal("1500")
    exact = search_recommendations(db, RecommendationSearchRequest(available_capital=required))
    below = search_recommendations(db, RecommendationSearchRequest(available_capital=required - Decimal("0.01")))
    assert exact["totals"]["recommendations"] == 1
    assert below["totals"]["outside_budget"] == 1
    assert below["outside_budget"][0]["budget_remaining"] == Decimal("-0.01")


def test_null_budget_never_confirms_fit(db, procurement):
    procurement.proposal_close_at = datetime.now(UTC) + timedelta(days=10)
    procurement.is_open = True
    db.add(DeliveryRequirement(procurement_id=procurement.id, classification=DeliveryClass.FAVORABLE, remote_delivery_score=90, confidence=Decimal("0.9"), direct_ship_score=90, justifications=[], pending_checks=[], evidence_ids=[]))
    _scenario(db, procurement, "5000", 1)
    db.commit()
    result = search_recommendations(db, RecommendationSearchRequest())
    assert result["totals"]["recommendations"] == 0
    assert result["totals"]["needs_analysis"] == 1
    assert "Orçamento disponível não informado" in result["needs_analysis"][0]["pending_checks"]


def test_router_is_executable_in_isolated_app(db):
    isolated = FastAPI()
    isolated.include_router(router)
    isolated.dependency_overrides[get_db] = lambda: db
    response = TestClient(isolated).post("/api/v1/recommendations/search", json={"available_capital": "0"})
    assert response.status_code == 200
    assert set(response.json()) >= {"recommendations", "needs_analysis", "outside_budget", "blocked", "rules_version"}

