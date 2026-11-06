from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from app.finance.service import calculate_financial_scenario
from app.models import DeliveryClass, DeliveryRequirement, FinancialScenario, ProductMatch, Supplier, SupplierProduct, SupplierQuote
from app.ranking.service import evaluate_item, normalize_instant
from app.schemas import FinancialScenarioCreate
from app.suppliers.service import match_product


def complete_payload(**overrides):
    values = {
        "quantity": Decimal("10"), "sale_unit_price": Decimal("200"), "acquisition_unit_cost": Decimal("100"),
        "freight": Decimal("100"), "packaging": Decimal("50"), "insurance": Decimal("20"), "tax_rate": Decimal("0.10"),
        "financial_cost": Decimal("30"), "installation_cost": Decimal("0"), "contingency": Decimal("100"),
        "available_capital": Decimal("5000"), "freight_known": True, "taxes_confirmed": True,
    }
    values.update(overrides)
    return FinancialScenarioCreate(**values)


def test_break_even_includes_taxes_on_revenue():
    result = calculate_financial_scenario(complete_payload())
    assert result["break_even_unit_price"] == Decimal("144.4444")


def test_evaluate_item_does_not_persist_score(db, procurement):
    item = procurement.items[0] if procurement.items else None
    if item is None:
        from app.models import ProcurementItem
        item = ProcurementItem(procurement_id=procurement.id, item_number=1, description="Notebook", quantity=10, unit="un", raw_json={})
        db.add(item)
        db.flush()
    before = db.query(__import__("app.models", fromlist=["OpportunityScore"]).OpportunityScore).count()
    result = evaluate_item(db, procurement, item)
    after = db.query(__import__("app.models", fromlist=["OpportunityScore"]).OpportunityScore).count()
    assert result.eligibility == "pending"
    assert before == after


def test_selected_current_quote_and_compatible_match_can_be_viable(db, procurement):
    from app.models import ProcurementItem
    item = ProcurementItem(procurement_id=procurement.id, item_number=1, description="Notebook profissional", complementary_description="RAM: 16 GB; SSD: 512 GB", quantity=10, unit="un", raw_json={})
    db.add(item)
    db.flush()
    procurement.proposal_close_at = datetime.now(UTC) + timedelta(days=10)
    procurement.estimated_total_value = Decimal("100000")
    db.add(DeliveryRequirement(procurement_id=procurement.id, classification=DeliveryClass.FAVORABLE, remote_delivery_score=90, confidence=Decimal("0.9"), direct_ship_score=90, justifications=[], pending_checks=[], evidence_ids=[]))
    supplier = Supplier(name="Fornecedor compatível", tax_id="12345678000199")
    db.add(supplier)
    db.flush()
    product = SupplierProduct(supplier_id=supplier.id, name="Notebook profissional", technical_description="16 GB RAM SSD 512 GB", specifications={"RAM": "16 GB", "SSD": "512 GB"})
    db.add(product)
    db.flush()
    match = match_product(db, item, product)
    assert match.classification.value == "compatible"
    quote = SupplierQuote(supplier_product_id=product.id, procurement_item_id=item.id, unit_price=Decimal("100"), valid_until=date.today() + timedelta(days=10), estimated_freight=Decimal("100"), freight_known=True, source="manual")
    db.add(quote)
    db.flush()
    payload = complete_payload(supplier_quote_id=quote.id)
    values = payload.model_dump(exclude={"freight_known", "taxes_confirmed", "supplier_quote_id"})
    calculated = calculate_financial_scenario(payload)
    db.add(FinancialScenario(procurement_item_id=item.id, supplier_quote_id=quote.id, supplier_quote_unit_price_snapshot=quote.unit_price, supplier_quote_freight_snapshot=quote.estimated_freight, supplier_quote_valid_until_snapshot=quote.valid_until, capital_source="request", freight_confirmed=True, taxes_confirmed=True, **values, **calculated))
    db.flush()
    result = evaluate_item(db, procurement, item)
    assert result.eligibility == "viable"
    assert result.quote.id == quote.id
    scenario = result.scenario
    scenario.available_capital = Decimal("500")
    db.flush()
    assert evaluate_item(db, procurement, item).eligibility == "blocked"
    assert evaluate_item(db, procurement, item, use_scenario_capital=False).eligibility == "viable"
    assert evaluate_item(db, procurement, item, available_capital=Decimal("5000")).eligibility == "viable"


def test_expired_quote_is_blocked_and_capital_is_contextual(db, procurement):
    from app.models import ProcurementItem
    item = ProcurementItem(procurement_id=procurement.id, item_number=1, description="Notebook", quantity=10, unit="un", raw_json={})
    db.add(item)
    db.flush()
    supplier = Supplier(name="Fornecedor expirado", tax_id="22345678000199")
    db.add(supplier)
    db.flush()
    product = SupplierProduct(supplier_id=supplier.id, name="Notebook", technical_description="Notebook", specifications={})
    db.add(product)
    db.flush()
    match_product(db, item, product)
    quote = SupplierQuote(supplier_product_id=product.id, procurement_item_id=item.id, unit_price=Decimal("100"), valid_until=date.today() - timedelta(days=1), estimated_freight=Decimal("100"), freight_known=True)
    db.add(quote)
    db.flush()
    payload = complete_payload(supplier_quote_id=quote.id)
    values = payload.model_dump(exclude={"freight_known", "taxes_confirmed", "supplier_quote_id"})
    db.add(FinancialScenario(procurement_item_id=item.id, supplier_quote_id=quote.id, supplier_quote_unit_price_snapshot=quote.unit_price, capital_source="request", freight_confirmed=True, taxes_confirmed=True, **values, **calculate_financial_scenario(payload)))
    db.flush()
    result = evaluate_item(db, procurement, item, available_capital=Decimal("500"))
    assert result.eligibility == "blocked"
    assert any("expirada" in text for text in result.blockers)
    assert any("insuficiente" in text for text in result.blockers)


def test_naive_sqlite_datetime_is_normalized():
    value = normalize_instant(datetime(2026, 10, 15, 12, 0))
    assert value.tzinfo == UTC
