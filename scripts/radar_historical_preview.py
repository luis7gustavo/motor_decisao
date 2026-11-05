"""Print the synthetic historical comparison as reviewable JSON."""

import json

from app.historical_preview import compare, load_fixture


def main() -> None:
    target, rows = load_fixture()
    result = compare(target, rows)
    print(json.dumps({
        "data_kind": "synthetic_historical_observations",
        "warning": "Historical prices are not current quotations, stock, delivery, or supplier capacity.",
        "target": vars(target),
        "sample": {"usable": len(result.usable), "excluded": len(result.excluded), "sufficient": result.sample_sufficient},
        "historical_unit_price": {
            "currency": target.currency,
            "min": str(result.price_min) if result.price_min is not None else None,
            "median": str(result.price_median) if result.price_median is not None else None,
            "max": str(result.price_max) if result.price_max is not None else None,
        },
        "observations": [{
            "result_key": row.result_key,
            "version": row.version,
            "source_item": row.source_item,
            "result_date": row.result_date.isoformat(),
            "quantity": str(row.quantity),
            "unit_price": str(row.unit_price),
            "supplier_id": row.supplier_id,
            "source_ref": row.source_ref,
        } for row in result.usable],
        "excluded": [{"result_key": key, "reason": reason} for key, reason in result.excluded],
        "candidates_for_manual_investigation": [{
            "supplier_id": supplier_id, "supplier_name": name, "historical_events": count
        } for supplier_id, name, count in result.candidates],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
