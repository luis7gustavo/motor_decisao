"""Replay a synthetic human review queue without persistence or external actions."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.commercial_preview import Case
from app.review_queue_preview import ReviewAction, create_queue, pending_entries, submit
from app.viability_preview import evaluate, load_synthetic_fixture


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "radar_demo"


def replay_queue():
    payload = json.loads((FIXTURES / "review_queue.json").read_text(encoding="utf-8"))
    if payload.get("synthetic") is not True:
        raise ValueError("review queue accepts only synthetic fixtures")
    scenario, today = load_synthetic_fixture(FIXTURES / "viability_base.json")
    evaluation = evaluate(scenario, today=today)
    if evaluation.status != "viável" or evaluation.revenue is None:
        raise ValueError("base viability fixture is not viable")
    cases = tuple(Case(opportunity_id=key, source_version=scenario.source_version,
                       required_items=tuple((item.key, item.quantity) for item in scenario.items),
                       amount_due=evaluation.revenue)
                  for key in payload["opportunities"])
    queue = create_queue(cases)
    origin = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)
    for row in payload["actions"]:
        action = ReviewAction(
            action_id=row["action_id"], opportunity_id=row["opportunity_id"],
            kind=row["kind"], actor="synthetic-operator",
            occurred_at=origin + timedelta(minutes=row["minute"]),
            expected_revision=row["expected_revision"],
            expected_source_version=row["expected_source_version"],
            next_source_version=row.get("next_source_version"), reason=row.get("reason", ""),
            gap=row.get("gap", ""), assignee=row.get("assignee", ""),
            evidence_ref=row.get("evidence_ref", ""),
            viability_status=evaluation.status if row["kind"] == "approve" else "",
            snapshot_hash=evaluation.snapshot_hash if row["kind"] == "approve" else "",
        )
        queue = submit(queue, action, scenario=scenario if row["kind"] == "approve" else None)
    return queue


def main() -> None:
    queue = replay_queue()
    pending = pending_entries(queue)
    print(json.dumps({
        "data_kind": "synthetic_review_queue",
        "pending_count": len(pending),
        "pending_ids": [entry.case.opportunity_id for entry in pending],
        "entries": [{"opportunity_id": entry.case.opportunity_id, "state": entry.case.state,
                     "review_pending": entry.case.review_pending, "revision": entry.revision,
                     "source_version": entry.case.source_version,
                     "approved_snapshot_hash": entry.case.approved_snapshot_hash or None,
                     "complement": ({"status": entry.complement.status, "gap": entry.complement.gap,
                                     "assignee": entry.complement.assignee,
                                     "evidence_ref": entry.complement.evidence_ref or None}
                                    if entry.complement else None)}
                    for entry in queue.entries],
        "audit": [{"action_id": row.action.action_id, "kind": row.action.kind,
                   "opportunity_id": row.action.opportunity_id, "actor": row.action.actor,
                   "occurred_at": row.action.occurred_at.isoformat(),
                   "source_version": row.source_version,
                   "next_source_version": row.next_source_version,
                   "previous_state": row.previous_state, "next_state": row.next_state,
                   "previous_review_pending": row.previous_review_pending,
                   "next_review_pending": row.next_review_pending,
                   "reason": row.action.reason or None, "gap": row.action.gap or None,
                   "assignee": row.action.assignee or None,
                   "evidence_ref": row.action.evidence_ref or None,
                   "snapshot_hash": row.action.snapshot_hash or None,
                   "revision": row.revision}
                  for row in queue.audit],
        "warning": "Synthetic in-memory replay only; no proposal, contact, purchase or payment was sent.",
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
