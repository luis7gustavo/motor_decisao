from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.finance.service import calculate_financial_scenario
from app.models import FinancialScenario, ProcurementItem
from app.ranking.service import calculate_score
from app.schemas import FinancialScenarioCreate


def scenario(**overrides):
    data = {
        "quantity": Decimal(10),
        "sale_unit_price": Decimal(200),
        "acquisition_unit_cost": Decimal(100),
        "freight": Decimal(100),
        "packaging": Decimal(50),
        "insurance": Decimal(20),
        "tax_rate": Decimal("0.10"),
        "financial_cost": Decimal(30),
        "installation_cost": Decimal(0),
        "contingency": Decimal(100),
        "available_capital": Decimal(5000),
        "freight_known": True,
        "taxes_confirmed": True,
    }
    data.update(overrides)
    return FinancialScenarioCreate(**data)


def test_financial_calculation_includes_all_additional_costs():
    result = calculate_financial_scenario(scenario())
    assert result["revenue"] == Decimal("2000.00")
    assert result["total_cost"] == Decimal("1500.00")
    assert result["estimated_result"] == Decimal("500.00")
    assert result["margin_rate"] == Decimal("0.2500")


def test_capital_below_required_is_flagged():
    result = calculate_financial_scenario(scenario(available_capital=Decimal(500)))
    assert result["required_capital"] == Decimal("1500.00")
    assert result["sensitivity"]["capital_sufficient"] is False


def test_ended_proposal_is_blocked(db, procurement):
    procurement.proposal_close_at = datetime.now(UTC) - timedelta(hours=1)
    result = calculate_score(db, procurement)
    assert not result.eligible
    assert any("encerrado" in blocker for blocker in result.blockers)


def test_lot_judgment_cannot_be_scored_as_isolated_item(db, procurement):
    item = ProcurementItem(procurement_id=procurement.id, item_number=1, description="Notebook", judgment_criterion="Menor preço por lote", raw_json={})
    db.add(item)
    db.flush()
    result = calculate_score(db, procurement, item)
    assert any("lote/grupo" in blocker for blocker in result.blockers)


def test_insufficient_capital_blocks_ranking(db, procurement):
    item = ProcurementItem(procurement_id=procurement.id, item_number=1, description="Notebook", judgment_criterion="Por item", raw_json={})
    db.add(item)
    db.flush()
    calc = calculate_financial_scenario(scenario())
    values = scenario().model_dump(exclude={"freight_known", "taxes_confirmed"})
    db.add(FinancialScenario(procurement_item_id=item.id, **values, **calc))
    db.flush()
    result = calculate_score(db, procurement, item, available_capital=Decimal(500))
    assert any("Capital" in blocker for blocker in result.blockers)

