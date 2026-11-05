from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.commercial_preview import Case, Event, apply_event
from app.viability_preview import evaluate, load_synthetic_fixture
from scripts.radar_commercial_preview import replay


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo" / "viability_base.json"
BASE_TIME = datetime(2026, 11, 4, 15, tzinfo=timezone.utc)


def base():
    scenario, today = load_synthetic_fixture(FIXTURE)
    evaluation = evaluate(scenario, today=today)
    assert evaluation.status == "viável"
    case = Case(scenario.scenario_id, scenario.source_version,
                tuple((item.key, item.quantity) for item in scenario.items), evaluation.revenue)
    return case, evaluation


def event(kind, minute, *, version=2, **kwargs):
    return Event(f"evt-{minute}", kind, "synthetic-operator",
                 BASE_TIME + timedelta(minutes=minute), version, **kwargs)


def approved(case, evaluation):
    scenario, today = load_synthetic_fixture(FIXTURE)
    return apply_event(case, event("review_approved", 0, reason="human synthetic review",
                                   viability_status=evaluation.status,
                                   snapshot_hash=evaluation.snapshot_hash),
                       review_scenario=scenario)


def test_review_and_participation_require_current_viability_and_external_reference():
    case, evaluation = base()
    with pytest.raises(ValueError, match="viable snapshot"):
        scenario, today = load_synthetic_fixture(FIXTURE)
        apply_event(case, event("review_approved", 0, reason="insufficient",
                                viability_status="pendente", snapshot_hash="old"),
                    review_scenario=scenario)
    assert case.state == "E04" and case.events == ()

    case = approved(case, evaluation)
    assert case.state == "E07" and not case.review_pending
    with pytest.raises(ValueError, match="evidence reference"):
        apply_event(case, event("participation_recorded", 1))
    case = apply_event(case, event("participation_recorded", 1,
                                   reference="synthetic://participation/1"))
    assert case.state == "E08" and len(case.events) == 2


def test_retification_preserves_participation_and_requires_new_review():
    case, evaluation = base()
    case = approved(case, evaluation)
    case = apply_event(case, event("participation_recorded", 1,
                                   reference="synthetic://participation/1"))
    original_participation = case.events[-1]
    case = apply_event(case, event("source_changed", 2, version=3,
                                   reference="synthetic://source/version/3"))
    assert case.state == "E08" and case.review_pending
    assert original_participation in case.events
    with pytest.raises(ValueError, match="outdated source"):
        old_scenario, today = load_synthetic_fixture(FIXTURE)
        apply_event(case, event("review_approved", 3, reason="stale", viability_status="viável",
                                snapshot_hash=evaluation.snapshot_hash),
                    review_scenario=old_scenario)
    revised = replace(old_scenario, source_version=3,
                      items=tuple(replace(item, source_version=3) for item in old_scenario.items))
    new_evaluation = evaluate(revised, today=today)
    case = apply_event(case, event("review_approved", 3, version=3,
                                   reason="new human review", viability_status="viável",
                                   snapshot_hash=new_evaluation.snapshot_hash),
                       review_scenario=revised)
    assert case.state == "E08" and not case.review_pending
    assert case.events[0].snapshot_hash == evaluation.snapshot_hash
    assert case.approved_snapshot_hash == new_evaluation.snapshot_hash


def test_complete_journey_keeps_partial_delivery_and_receipt_separate():
    case, evaluation = base()
    case = approved(case, evaluation)
    case = apply_event(case, event("participation_recorded", 1, reference="synthetic://p"))
    case = apply_event(case, event("result_recorded", 2, reference="synthetic://r"))
    case = apply_event(case, event("winner_recorded", 3, reference="synthetic://w"))
    assert case.state == "E11"  # A win does not create a contract.
    case = apply_event(case, event("contract_started", 4, reference="synthetic://c",
                                   contract_items=case.required_items, amount=case.amount_due))
    case = apply_event(case, event("delivery_recorded", 5, item_key="A", quantity=Decimal("100"),
                                   reference="synthetic://d1"))
    case = apply_event(case, event("delivery_recorded", 6, item_key="B", quantity=Decimal("5"),
                                   reference="synthetic://d2"))
    with pytest.raises(ValueError, match="complete delivery"):
        apply_event(case, event("delivery_accepted", 7, reference="synthetic://a"))
    case = apply_event(case, event("delivery_recorded", 7, item_key="B", quantity=Decimal("5"),
                                   reference="synthetic://d3"))
    case = apply_event(case, event("delivery_accepted", 8, reference="synthetic://a"))
    case = apply_event(case, event("receipt_recorded", 9, amount=Decimal("5000"),
                                   reference="synthetic://cash1"))
    assert case.state == "E14" and case.received_amount == Decimal("5000")
    with pytest.raises(ValueError, match="full receipt"):
        apply_event(case, event("receipt_reconciled", 10, reference="synthetic://reconcile"))
    case = apply_event(case, event("receipt_recorded", 10, amount=Decimal("8300"),
                                   reference="synthetic://cash2"))
    case = apply_event(case, event("receipt_reconciled", 11, reference="synthetic://reconcile"))
    assert case.state == "E15" and case.received_amount == case.amount_due


def test_duplicate_event_id_is_idempotent_but_conflict_is_rejected():
    case, evaluation = base()
    scenario, today = load_synthetic_fixture(FIXTURE)
    first = event("review_approved", 0, reason="human review", viability_status="viável",
                  snapshot_hash=evaluation.snapshot_hash)
    approved_case = apply_event(case, first, review_scenario=scenario)
    assert apply_event(approved_case, first) is approved_case
    with pytest.raises(ValueError, match="conflicting event id"):
        apply_event(approved_case, replace(first, reason="different"))


def test_new_review_replaces_precontract_terms_and_rejects_forged_hash():
    case, evaluation = base()
    case = approved(case, evaluation)
    case = apply_event(case, event("participation_recorded", 1, reference="synthetic://p"))
    case = apply_event(case, event("source_changed", 2, version=3,
                                   reference="synthetic://source/version/3"))
    scenario, today = load_synthetic_fixture(FIXTURE)
    changed_items = tuple(replace(item, source_version=3,
                                  quantity=Decimal("90") if item.key == "A" else item.quantity)
                          for item in scenario.items)
    revised = replace(scenario, source_version=3, items=changed_items)
    current_evaluation = evaluate(revised, today=today)
    assert current_evaluation.status == "viável"
    with pytest.raises(ValueError, match="viable snapshot"):
        apply_event(case, event("review_approved", 3, version=3, reason="forged",
                                viability_status="viável", snapshot_hash="invented"),
                    review_scenario=revised)
    case = apply_event(case, event("review_approved", 3, version=3, reason="revised terms",
                                   viability_status="viável", snapshot_hash=current_evaluation.snapshot_hash),
                       review_scenario=revised)
    assert dict(case.required_items)["A"] == Decimal("90")
    assert case.amount_due == current_evaluation.revenue != evaluation.revenue
    case = apply_event(case, event("result_recorded", 4, reference="synthetic://r"))
    case = apply_event(case, event("winner_recorded", 5, reference="synthetic://w"))
    with pytest.raises(ValueError, match="contract terms"):
        apply_event(case, event("contract_started", 6, reference="synthetic://c",
                                contract_items=(("A", Decimal("100")), ("B", Decimal("10"))),
                                amount=evaluation.revenue))


def test_evidenced_contract_terms_remain_locked_after_source_change():
    case, evaluation = base()
    case = approved(case, evaluation)
    case = apply_event(case, event("participation_recorded", 1, reference="synthetic://p"))
    case = apply_event(case, event("result_recorded", 2, reference="synthetic://r"))
    case = apply_event(case, event("winner_recorded", 3, reference="synthetic://w"))
    case = apply_event(case, event("contract_started", 4, reference="synthetic://c",
                                   contract_items=case.required_items, amount=case.amount_due))
    assert case.contract_locked
    case = apply_event(case, event("source_changed", 5, version=3,
                                   reference="synthetic://source/version/3"))
    scenario, today = load_synthetic_fixture(FIXTURE)
    revised = replace(scenario, source_version=3,
                      items=tuple(replace(item, source_version=3,
                                          quantity=Decimal("90") if item.key == "A" else item.quantity)
                                  for item in scenario.items))
    recalculated = evaluate(revised, today=today)
    with pytest.raises(ValueError, match="conflicts with evidenced contract"):
        apply_event(case, event("review_approved", 6, version=3, reason="changed scope",
                                viability_status=recalculated.status,
                                snapshot_hash=recalculated.snapshot_hash),
                    review_scenario=revised)
    assert case.state == "E13" and case.review_pending
    assert dict(case.required_items)["A"] == Decimal("100")
    assert case.amount_due == Decimal("13300.00")


def test_fixture_replays_audit_timeline_without_external_action():
    case = replay()
    assert case.state == "E16"
    assert case.source_version == 3 and not case.review_pending
    assert dict(case.delivered_items) == dict(case.required_items)
    assert case.received_amount == case.amount_due == Decimal("13300.00")
    assert len(case.events) == 14
    assert case.events[1].kind == "participation_recorded"
    assert case.events[2].kind == "source_changed"
    assert case.events[1].source_version == 2  # Prior fact retained after retification.


def test_approval_uses_event_day_so_expired_quote_cannot_pass():
    case, evaluation = base()
    scenario, _ = load_synthetic_fixture(FIXTURE)
    late = Event("late-review", "review_approved", "synthetic-operator",
                 datetime(2026, 11, 11, 15, tzinfo=timezone.utc), 2,
                 reason="stale quote", viability_status="viável",
                 snapshot_hash=evaluation.snapshot_hash)
    with pytest.raises(ValueError, match="viable snapshot"):
        apply_event(case, late, review_scenario=scenario)
    assert case.state == "E04" and case.events == ()


def test_duplicate_contract_item_does_not_disappear_in_dict_conversion():
    case, evaluation = base()
    case = approved(case, evaluation)
    case = apply_event(case, event("participation_recorded", 1, reference="synthetic://p"))
    case = apply_event(case, event("result_recorded", 2, reference="synthetic://r"))
    case = apply_event(case, event("winner_recorded", 3, reference="synthetic://w"))
    duplicated = case.required_items + (("A", Decimal("100")),)
    with pytest.raises(ValueError, match="contract terms"):
        apply_event(case, event("contract_started", 4, reference="synthetic://c",
                                contract_items=duplicated, amount=case.amount_due))
