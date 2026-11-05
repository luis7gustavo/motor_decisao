from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.commercial_preview import Case
from app.review_queue_preview import ReviewAction, create_queue, pending_entries, submit
from app.viability_preview import evaluate, load_synthetic_fixture
from scripts.radar_review_queue_preview import replay_queue


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo" / "viability_base.json"
NOW = datetime(2026, 11, 4, 15, tzinfo=timezone.utc)


def base_queue():
    scenario, today = load_synthetic_fixture(FIXTURE)
    evaluation = evaluate(scenario, today=today)
    assert evaluation.status == "viável"
    items = tuple((item.key, item.quantity) for item in scenario.items)
    queue = create_queue((
        Case("LOTE-SINTETICO-01", scenario.source_version, items, evaluation.revenue),
        Case("LOTE-SINTETICO-02", scenario.source_version, items, evaluation.revenue),
    ))
    return queue, scenario, evaluation


def action(kind, action_id, opportunity_id="LOTE-SINTETICO-01", *, revision=0, version=2, at=NOW, **kwargs):
    return ReviewAction(action_id, opportunity_id, kind, "synthetic-operator", at,
                        revision, version, **kwargs)


def test_fixture_queue_count_matches_pending_list_and_preserves_decisions():
    queue = replay_queue()

    assert len(pending_entries(queue)) == 0
    assert len(queue.audit) == 4
    assert [(entry.case.opportunity_id, entry.case.state, entry.revision) for entry in queue.entries] == [
        ("LOTE-SINTETICO-01", "E07", 3),
        ("LOTE-SINTETICO-02", "E06", 1),
    ]
    assert queue.entries[0].complement.status == "resolved"
    assert queue.audit[0].action.gap and queue.audit[0].action.assignee
    assert queue.audit[1].action.reason
    assert queue.audit[-1].previous_state == "E04" and queue.audit[-1].next_state == "E07"
    assert queue.entries[0].case.events[-1].snapshot_hash == queue.audit[-1].action.snapshot_hash


def test_complement_requires_owner_and_evidence_then_blocks_approval_until_resolved():
    queue, scenario, evaluation = base_queue()
    with pytest.raises(ValueError, match="gap and assignee"):
        submit(queue, action("request_complement", "q1", gap="missing proof"))
    request = action("request_complement", "q1", gap="missing proof", assignee="reviewer")
    queue = submit(queue, request)
    assert len(pending_entries(queue)) == 2
    assert pending_entries(queue)[0].case.opportunity_id == "LOTE-SINTETICO-01"
    approve = action("approve", "q2", revision=1, reason="human review",
                     viability_status=evaluation.status, snapshot_hash=evaluation.snapshot_hash)
    with pytest.raises(ValueError, match="open or stale complement blocks approval"):
        submit(queue, approve, scenario=scenario)
    with pytest.raises(ValueError, match="evidence and reason"):
        submit(queue, action("resolve_complement", "q3", revision=1, reason="done"))
    queue = submit(queue, action("resolve_complement", "q3", revision=1,
                                 reason="evidence checked", evidence_ref="synthetic://proof"))
    queue = submit(queue, replace(approve, expected_revision=2), scenario=scenario)
    assert [entry.case.opportunity_id for entry in pending_entries(queue)] == ["LOTE-SINTETICO-02"]


def test_double_submission_is_idempotent_and_stale_revision_fails():
    queue, _, _ = base_queue()
    discard = action("discard", "same-id", opportunity_id="LOTE-SINTETICO-02",
                     reason="human synthetic decision")
    decided = submit(queue, discard)
    assert submit(decided, discard) is decided
    with pytest.raises(ValueError, match="conflicting action id"):
        submit(decided, replace(discard, reason="another reason"))
    with pytest.raises(ValueError, match="stale queue revision"):
        submit(decided, action("request_complement", "q-new", "LOTE-SINTETICO-02",
                               gap="proof", assignee="reviewer"))
    assert len(decided.audit) == 1 and decided.entries[1].case.state == "E06"


def test_favorable_queue_action_cannot_use_expired_quote_or_forged_snapshot():
    queue, scenario, evaluation = base_queue()
    late = action("approve", "late", at=datetime(2026, 11, 11, 15, tzinfo=timezone.utc),
                  reason="stale review", viability_status="viável",
                  snapshot_hash=evaluation.snapshot_hash)
    with pytest.raises(ValueError, match="viable snapshot"):
        submit(queue, late, scenario=scenario)
    forged = action("approve", "forged", reason="forged review", viability_status="viável",
                    snapshot_hash="not-the-D-hash")
    with pytest.raises(ValueError, match="viable snapshot"):
        submit(queue, forged, scenario=scenario)
    assert queue.audit == () and all(entry.case.review_pending for entry in queue.entries)


def test_duplicate_case_and_discard_without_reason_fail_closed():
    queue, _, _ = base_queue()
    with pytest.raises(ValueError, match="distinct opportunity"):
        create_queue((queue.entries[0].case, queue.entries[0].case))
    with pytest.raises(ValueError, match="discard requires"):
        submit(queue, action("discard", "q1"))


def test_source_change_invalidates_old_decision_and_resolved_complement():
    queue, scenario, _ = base_queue()
    queue = submit(queue, action("request_complement", "q1", gap="old evidence", assignee="reviewer"))
    queue = submit(queue, action("resolve_complement", "q2", revision=1,
                                 reason="checked", evidence_ref="synthetic://old-proof"))
    queue = submit(queue, action("source_changed", "q3", revision=2,
                                 next_source_version=3, evidence_ref="synthetic://version/3"))
    assert queue.entries[0].case.source_version == 3
    assert queue.entries[0].complement.status == "stale"
    assert len(pending_entries(queue)) == 2

    with pytest.raises(ValueError, match="stale source version"):
        submit(queue, action("discard", "old-decision", revision=3, version=2,
                             reason="prepared before change"))
    revised = replace(scenario, source_version=3,
                      items=tuple(replace(item, source_version=3) for item in scenario.items))
    evaluation = evaluate(revised, today=NOW.date())
    with pytest.raises(ValueError, match="stale complement blocks approval"):
        submit(queue, action("approve", "too-early", revision=3, version=3,
                             reason="review", viability_status=evaluation.status,
                             snapshot_hash=evaluation.snapshot_hash), scenario=revised)

    queue = submit(queue, action("request_complement", "q4", revision=3, version=3,
                                 gap="new version", assignee="reviewer"))
    queue = submit(queue, action("resolve_complement", "q5", revision=4, version=3,
                                 reason="new proof checked", evidence_ref="synthetic://new-proof"))
    queue = submit(queue, action("approve", "q6", revision=5, version=3,
                                 reason="reviewed current source", viability_status=evaluation.status,
                                 snapshot_hash=evaluation.snapshot_hash), scenario=revised)
    assert queue.entries[0].case.state == "E07"
    assert queue.entries[0].case.source_version == 3
    assert queue.audit[2].source_version == 2 and queue.audit[2].next_source_version == 3
    assert queue.audit[1].action.evidence_ref == "synthetic://old-proof"
