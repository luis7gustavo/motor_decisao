"""Synthetic failure cases for the P02 offline page ledger."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from pncp_offline_ledger import audit  # noqa: E402


def _manifest(tmp_path, attempts):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"kind": "pncp-offline-ledger-v1", "captured_at": "2026-11-19T12:00:00Z",
                                "evidence_class": "E2", "attempts": attempts}), encoding="utf-8")
    return path


def _row(tmp_path, page, attempt=1, status=200, total=2, source_time=None):
    row = {"window": "synthetic-window", "resource": "items", "partition": "part-1",
           "version": "v1", "page": page, "attempt": attempt, "http_status": status,
           "received_at": "2026-11-19T11:00:00Z", "source_updated_at": source_time,
           "total_pages_reported": total}
    if status == 200:
        name = f"page-{page}-{attempt}.json"
        body = f'{{"page":{page}}}'.encode()
        (tmp_path / name).write_bytes(body)
        row.update(file=name, sha256=hashlib.sha256(body).hexdigest())
    return row


def test_retry_and_reported_page_gap_remain_partial(tmp_path):
    attempts = [_row(tmp_path, 1, 1, 500), _row(tmp_path, 1, 2, 200,
                    source_time="2026-11-19T10:00:00Z")]
    result = audit(_manifest(tmp_path, attempts))
    group = result["groups"][0]
    assert group["attempt_count"] == 2
    assert group["failed_http_attempt_count"] == 1
    assert group["recovered_failed_http_attempt_count"] == 1
    failed, accepted = group["page_attempts"]
    assert (failed["page"], failed["attempt"], failed["http_status"]) == (1, 1, 500)
    assert failed["received_at"] == "2026-11-19T11:00:00+00:00"
    assert failed["source_updated_at"] is None
    assert failed["file"] is None and failed["sha256"] is None
    assert failed["actual_sha256"] is None and failed["hash_valid"] is None
    assert failed["is_latest"] is False and failed["accepted"] is False
    assert (accepted["page"], accepted["attempt"], accepted["http_status"]) == (1, 2, 200)
    assert accepted["received_at"] == "2026-11-19T11:00:00+00:00"
    assert accepted["source_updated_at"] == "2026-11-19T10:00:00+00:00"
    assert accepted["file"] == "page-1-2.json"
    assert accepted["sha256"] == accepted["actual_sha256"]
    assert accepted["hash_valid"] is True
    assert accepted["is_latest"] is True and accepted["accepted"] is True
    assert group["successful_pages"] == [1]
    assert group["missing_pages_within_reported_total"] == [2]
    assert group["max_received_age_seconds"] == 3600
    assert group["max_source_updated_age_seconds"] == 7200
    assert group["coverage_state"] == "unknown_without_independent_reference"
    assert group["freshness_state"] == "unknown_without_independent_reference_and_sla"


def test_hash_mismatch_does_not_count_as_observed_page(tmp_path):
    row = _row(tmp_path, 1, total=1)
    (tmp_path / row["file"]).write_bytes(b"changed")
    result = audit(_manifest(tmp_path, [row]))
    assert result["issues"] == ["hash_mismatch:synthetic-window/items/part-1/v1:1:1"]
    assert result["groups"][0]["successful_pages"] == []
    assert result["groups"][0]["missing_pages_within_reported_total"] == [1]
    attempt = result["groups"][0]["page_attempts"][0]
    assert attempt["sha256"] != attempt["actual_sha256"]
    assert attempt["hash_valid"] is False and attempt["accepted"] is False


def test_successful_reported_pages_still_do_not_prove_coverage(tmp_path):
    result = audit(_manifest(tmp_path, [_row(tmp_path, 1), _row(tmp_path, 2)]))
    assert result["issues"] == []
    assert result["groups"][0]["observed_page_state"] == "all_reported_pages_observed"
    assert result["groups"][0]["source_updated_timestamps_missing"] == 2
    assert result["groups"][0]["coverage_state"] == "unknown_without_independent_reference"


def test_rejects_path_escape_and_conflicting_totals(tmp_path):
    escaping = _row(tmp_path, 1)
    escaping["file"] = "../outside.json"
    with pytest.raises(ValueError, match="path must stay"):
        audit(_manifest(tmp_path, [escaping]))
    result = audit(_manifest(tmp_path, [_row(tmp_path, 1, total=2), _row(tmp_path, 2, total=3)]))
    assert "reported_total_conflict:synthetic-window/items/part-1/v1" in result["issues"]
    assert result["groups"][0]["total_pages_reported"] is None


def test_last_failed_attempt_is_not_hidden_by_earlier_success(tmp_path):
    result = audit(_manifest(tmp_path, [_row(tmp_path, 1, 1, 200, total=1),
                                        _row(tmp_path, 1, 2, 503, total=1)]))
    assert result["groups"][0]["failed_latest_pages"] == [1]
    assert result["groups"][0]["recovered_failed_http_attempt_count"] == 0
    assert [row["accepted"] for row in result["groups"][0]["page_attempts"]] == [False, False]
    assert result["groups"][0]["observed_page_state"] == "partial_or_unknown"


def test_cli_exit_codes_and_json_output(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts" / "pncp_offline_ledger.py"
    row = _row(tmp_path, 1, total=1)
    manifest = _manifest(tmp_path, [row])
    good = subprocess.run([sys.executable, str(script), str(manifest)], capture_output=True, text=True)
    assert good.returncode == 0
    assert json.loads(good.stdout)["groups"][0]["successful_pages"] == [1]
    (tmp_path / row["file"]).write_bytes(b"changed")
    bad = subprocess.run([sys.executable, str(script), str(manifest)], capture_output=True, text=True)
    assert bad.returncode == 1
    assert json.loads(bad.stdout)["issues"]
