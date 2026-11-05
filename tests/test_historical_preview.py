from dataclasses import replace
from decimal import Decimal
import json

import pytest

from app.historical_preview import compare, load_fixture


def test_latest_versions_and_distinct_establishments_drive_reference():
    target, rows = load_fixture()

    result = compare(target, rows)

    assert [row.result_key for row in result.usable] == ["SYN-R1", "SYN-R2", "SYN-R3"]
    assert result.usable[0].version == 2
    assert (result.price_min, result.price_median, result.price_max) == (
        Decimal("1.20"), Decimal("1.50"), Decimal("1.80")
    )
    assert result.sample_sufficient
    assert result.candidates == (
        ("SYN-EST-1", "Papelaria Exemplo", 1),
        ("SYN-EST-2", "Papelaria Exemplo", 2),
    )


def test_exclusions_keep_stage_unit_spec_identity_and_revocation_visible():
    target, rows = load_fixture()

    result = compare(target, rows)

    assert dict(result.excluded) == {
        "SYN-R4": "unit_conversion_unproven",
        "SYN-R5": "different_stage",
        "SYN-R6": "technical_mismatch_or_unproven",
        "SYN-R7": "possible_duplicate_pending",
        "SYN-R8": "supplier_identity_pending",
        "SYN-R9": "revoked_latest_version",
    }
    assert sum(count for _, _, count in result.candidates) == len(result.usable)


def test_insufficient_sample_and_no_comparable_price():
    target, rows = load_fixture()

    two = compare(target, tuple(row for row in rows if row.result_key != "SYN-R3"))
    none = compare(replace(target, unit="pacote"), rows)

    assert not two.sample_sufficient
    assert two.price_median == Decimal("1.35")
    assert not none.sample_sufficient
    assert none.price_min is None and none.candidates == ()


def test_conflicting_version_and_invalid_money_fail_closed():
    target, rows = load_fixture()

    with pytest.raises(ValueError, match="conflicting version"):
        compare(target, rows + (replace(rows[0], unit_price=Decimal("99")),))
    with pytest.raises(ValueError, match="unit_price"):
        compare(target, (replace(rows[0], unit_price=Decimal("NaN")),))


def test_non_synthetic_file_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="synthetic fixtures"):
        path = tmp_path / "real.json"
        path.write_text(json.dumps({"synthetic": False, "target": {}, "observations": []}), encoding="utf-8")
        load_fixture(path)


def test_latest_revocation_never_falls_back_to_old_price():
    target, rows = load_fixture()
    result = compare(target, rows)

    assert "SYN-R9" not in {row.result_key for row in result.usable}
    assert ("SYN-R9", "revoked_latest_version") in result.excluded


def test_result_identity_cannot_join_distinct_items_and_currency_is_not_mixed():
    target, rows = load_fixture()

    with pytest.raises(ValueError, match="spans source items"):
        compare(target, rows + (replace(rows[0], version=3, source_item="SYN-OTHER/I1"),))
    changed = tuple(replace(row, currency="USD") if row.result_key == "SYN-R2" else row for row in rows)
    result = compare(target, changed)
    assert ("SYN-R2", "currency_conversion_unproven") in result.excluded
    assert not result.sample_sufficient
