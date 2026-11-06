from decimal import ROUND_HALF_UP, Decimal

from app.schemas import FinancialScenarioCreate

CENT = Decimal("0.01")
FOUR = Decimal("0.0001")


def money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def calculate_financial_scenario(data: FinancialScenarioCreate) -> dict:
    revenue = data.sale_unit_price * data.quantity
    acquisition = data.acquisition_unit_cost * data.quantity
    taxes = revenue * data.tax_rate
    operating = data.freight + data.packaging + data.insurance + data.financial_cost + data.installation_cost + data.contingency
    total_cost = acquisition + taxes + operating
    result = revenue - total_cost
    margin_rate = result / revenue if revenue else Decimal(0)
    required_capital = acquisition + operating + taxes
    # Taxes are charged on revenue, so the break-even price must solve
    # quantity * price * (1 - tax_rate) = acquisition + fixed operating costs.
    break_even_denominator = Decimal(1) - data.tax_rate
    break_even = (acquisition + operating) / data.quantity / break_even_denominator if break_even_denominator > 0 else Decimal(0)
    denominator = Decimal(1) - data.tax_rate - data.target_margin_rate
    target_price = (acquisition + operating) / data.quantity / denominator if denominator > 0 else Decimal(0)

    sensitivity = {}
    for discount in (Decimal("0.05"), Decimal("0.10"), Decimal("0.15")):
        adjusted_revenue = revenue * (Decimal(1) - discount)
        adjusted_cost = acquisition + operating + adjusted_revenue * data.tax_rate
        sensitivity[f"bid_discount_{int(discount * 100)}pct"] = {
            "revenue": str(money(adjusted_revenue)),
            "margin_rate": str(((adjusted_revenue - adjusted_cost) / adjusted_revenue).quantize(FOUR)) if adjusted_revenue else "0",
        }
    for increase in (Decimal("0.20"), Decimal("0.50")):
        adjusted_cost = total_cost + data.freight * increase
        sensitivity[f"freight_increase_{int(increase * 100)}pct"] = {
            "total_cost": str(money(adjusted_cost)),
            "margin_rate": str(((revenue - adjusted_cost) / revenue).quantize(FOUR)) if revenue else "0",
        }
    sensitivity["capital_sufficient"] = data.available_capital is None or data.available_capital >= required_capital
    sensitivity["payment_days"] = data.payment_days

    return {
        "revenue": money(revenue),
        "total_cost": money(total_cost),
        "estimated_result": money(result),
        "margin_rate": margin_rate.quantize(FOUR),
        "required_capital": money(required_capital),
        "break_even_unit_price": break_even.quantize(FOUR),
        "target_margin_unit_price": target_price.quantize(FOUR),
        "sensitivity": sensitivity,
        "assumptions_complete": data.freight_known and data.taxes_confirmed,
    }

