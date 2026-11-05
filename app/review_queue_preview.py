"""In-memory review queue for synthetic Radar Licita cases.

Queue actions are immutable and auditable within one process. Persistence,
authentication and multi-process locking belong to a later gate.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from app.commercial_preview import Case, Event, apply_event
from app.viability_preview import Scenario


@dataclass(frozen=True)
class Complement:
    gap: str
    assignee: str
    opened_at: datetime
    source_version: int
    status: str = "open"
    evidence_ref: str = ""


@dataclass(frozen=True)
class QueueEntry:
    case: Case
    revision: int = 0
    complement: Complement | None = None


@dataclass(frozen=True)
class ReviewAction:
    action_id: str
    opportunity_id: str
    kind: str
    actor: str
    occurred_at: datetime
    expected_revision: int
    expected_source_version: int
    reason: str = ""
    gap: str = ""
    assignee: str = ""
    evidence_ref: str = ""
    viability_status: str = ""
    snapshot_hash: str = ""
    next_source_version: int | None = None


@dataclass(frozen=True)
class QueueAudit:
    action: ReviewAction
    previous_state: str
    next_state: str
    previous_review_pending: bool
    next_review_pending: bool
    source_version: int
    next_source_version: int
    revision: int


@dataclass(frozen=True)
class ReviewQueue:
    entries: tuple[QueueEntry, ...]
    audit: tuple[QueueAudit, ...] = ()


def create_queue(cases: tuple[Case, ...]) -> ReviewQueue:
    keys = [case.opportunity_id for case in cases]
    if not keys or any(not key for key in keys) or len(set(keys)) != len(keys):
        raise ValueError("queue requires distinct opportunity identities")
    return ReviewQueue(tuple(QueueEntry(case) for case in cases))


def pending_entries(queue: ReviewQueue) -> tuple[QueueEntry, ...]:
    """Return the exact source list for the pending counter and review screen."""
    return tuple(sorted(
        (entry for entry in queue.entries if entry.case.review_pending
         and entry.case.state not in {"E06", "E16"}),
        key=lambda entry: (entry.complement is None or entry.complement.status not in {"open", "stale"},
                           entry.case.opportunity_id),
    ))


def submit(queue: ReviewQueue, action: ReviewAction, *, scenario: Scenario | None = None) -> ReviewQueue:
    """Apply one human queue action with idempotency and optimistic revision."""
    if not action.action_id or not action.actor or action.occurred_at.tzinfo is None:
        raise ValueError("action requires id, actor and timezone")
    for prior in queue.audit:
        if prior.action.action_id == action.action_id:
            if prior.action == action:
                return queue
            raise ValueError("conflicting action id")
    entries = {entry.case.opportunity_id: entry for entry in queue.entries}
    if len(entries) != len(queue.entries):
        raise ValueError("duplicate opportunity in queue")
    if action.opportunity_id not in entries:
        raise KeyError(action.opportunity_id)
    entry = entries[action.opportunity_id]
    case = entry.case
    if action.expected_revision != entry.revision:
        raise ValueError("stale queue revision")
    if action.expected_source_version != case.source_version:
        raise ValueError("stale source version")
    if (case.events and action.occurred_at < case.events[-1].occurred_at) or (
            queue.audit and action.occurred_at < queue.audit[-1].action.occurred_at):
        raise ValueError("action time goes backwards")

    complement = entry.complement
    updated = case
    if action.kind == "request_complement":
        if not case.review_pending or case.state in {"E06", "E16"}:
            raise ValueError("case is not awaiting review")
        if complement and complement.status == "open":
            raise ValueError("open complement already exists")
        if not action.gap or not action.assignee:
            raise ValueError("complement requires gap and assignee")
        complement = Complement(action.gap, action.assignee, action.occurred_at, case.source_version)
    elif action.kind == "resolve_complement":
        if not complement or complement.status != "open":
            raise ValueError("no open complement")
        if complement.source_version != case.source_version:
            raise ValueError("complement belongs to outdated source version")
        if not action.evidence_ref or not action.reason:
            raise ValueError("resolution requires evidence and reason")
        complement = replace(complement, status="resolved", evidence_ref=action.evidence_ref)
    elif action.kind == "approve":
        if complement and complement.status in {"open", "stale"}:
            raise ValueError("open or stale complement blocks approval")
        event = Event(action.action_id, "review_approved", action.actor, action.occurred_at,
                      case.source_version, reason=action.reason, snapshot_hash=action.snapshot_hash,
                      viability_status=action.viability_status)
        updated = apply_event(case, event, review_scenario=scenario)
    elif action.kind == "discard":
        event = Event(action.action_id, "discarded", action.actor, action.occurred_at,
                      case.source_version, reason=action.reason)
        updated = apply_event(case, event)
        if complement and complement.status == "open":
            complement = replace(complement, status="cancelled")
    elif action.kind == "source_changed":
        if action.next_source_version is None:
            raise ValueError("source change requires next version")
        event = Event(action.action_id, "source_changed", action.actor, action.occurred_at,
                      action.next_source_version, reference=action.evidence_ref)
        updated = apply_event(case, event)
        if complement and complement.status in {"open", "resolved"}:
            complement = replace(complement, status="stale")
    else:
        raise ValueError(f"unsupported queue action: {action.kind}")

    new_entry = QueueEntry(updated, entry.revision + 1, complement)
    next_entries = tuple(new_entry if old.case.opportunity_id == action.opportunity_id else old
                         for old in queue.entries)
    audit = QueueAudit(action, case.state, updated.state, case.review_pending,
                       updated.review_pending, case.source_version,
                       updated.source_version, new_entry.revision)
    return ReviewQueue(next_entries, queue.audit + (audit,))
