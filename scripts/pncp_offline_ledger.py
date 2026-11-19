"""Audit an offline PNCP page export without claiming source coverage.

Input is a JSON manifest of page attempts. A successful attempt must point to
an existing file whose raw bytes match its SHA-256. No network or database is
used. See plans/radar-licita-v4/LEDGER_OFFLINE_P02.md for the contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _instant(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field}: timestamp with timezone required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field}: invalid timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field}: timezone required")
    return parsed.astimezone(timezone.utc)


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field}: nonempty string required")
    return value


def _positive(value: Any, field: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field}: positive integer required")
    return value


def _file_hash(root: Path, name: Any) -> str:
    file_name = _text(name, "file")
    relative = Path(file_name)
    if relative.is_absolute() or any(part == ".." for part in relative.parts):
        raise ValueError("file: path must stay inside manifest directory")
    target = (root / relative).resolve()
    if not target.is_relative_to(root.resolve()) or not target.is_file():
        raise ValueError("file: missing or outside manifest directory")
    return hashlib.sha256(target.read_bytes()).hexdigest()


def audit(manifest_path: Path) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("kind") != "pncp-offline-ledger-v1":
        raise ValueError("kind: pncp-offline-ledger-v1 required")
    captured = _instant(manifest.get("captured_at"), "captured_at")
    attempts = manifest.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise ValueError("attempts: nonempty list required")

    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    seen_attempts: set[tuple[str, str, str, str, int, int]] = set()
    issues: list[str] = []
    for index, raw in enumerate(attempts):
        if not isinstance(raw, dict):
            raise ValueError(f"attempts[{index}]: object required")
        key = tuple(_text(raw.get(field), field) for field in ("window", "resource", "partition", "version"))
        page = _positive(raw.get("page"), "page")
        attempt = _positive(raw.get("attempt"), "attempt")
        status = raw.get("http_status")
        if type(status) is not int or not 100 <= status <= 599:
            raise ValueError("http_status: integer 100..599 required")
        received = _instant(raw.get("received_at"), "received_at")
        source_raw = raw.get("source_updated_at")
        source = _instant(source_raw, "source_updated_at") if source_raw is not None else None
        reported = raw.get("total_pages_reported")
        if reported is not None:
            reported = _positive(reported, "total_pages_reported")
            if reported > 10000:
                raise ValueError("total_pages_reported: exceeds offline safety limit of 10000")
        if (key + (page, attempt)) in seen_attempts:
            issues.append(f"duplicate_attempt:{'/'.join(key)}:{page}:{attempt}")
        seen_attempts.add(key + (page, attempt))
        row = {"page": page, "attempt": attempt, "http_status": status,
               "received_at": received, "source_updated_at": source,
               "total_pages_reported": reported, "file": raw.get("file"),
               "sha256": raw.get("sha256"), "actual_sha256": None, "hash_valid": None}
        if 200 <= status < 300:
            digest = _text(raw.get("sha256"), "sha256").lower()
            if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("sha256: 64 hexadecimal characters required")
            actual = _file_hash(manifest_path.parent, raw.get("file"))
            row["actual_sha256"] = actual
            row["hash_valid"] = actual == digest
            if actual != digest:
                issues.append(f"hash_mismatch:{'/'.join(key)}:{page}:{attempt}")
            row["sha256"] = digest
            if reported is not None and page > reported:
                issues.append(f"page_above_reported_total:{'/'.join(key)}:{page}")
        elif raw.get("file") is not None or raw.get("sha256") is not None:
            raise ValueError("failed attempt must not claim a successful body/hash")
        groups[key].append(row)

    result_groups = []
    for key, rows in sorted(groups.items()):
        by_page: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_page[row["page"]].append(row)
        latest = [max(page_rows, key=lambda row: row["attempt"]) for _, page_rows in sorted(by_page.items())]
        latest_by_page = {row["page"]: row for row in latest}
        totals = {row["total_pages_reported"] for row in rows if row["total_pages_reported"] is not None and 200 <= row["http_status"] < 300}
        if len(totals) > 1:
            issues.append(f"reported_total_conflict:{'/'.join(key)}")
        total = next(iter(totals)) if len(totals) == 1 else None
        successful = [row for row in latest if 200 <= row["http_status"] < 300 and row["hash_valid"]]
        failed = [row["page"] for row in latest if not (200 <= row["http_status"] < 300 and row["hash_valid"])]
        missing = sorted(set(range(1, total + 1)) - {row["page"] for row in successful}) if total is not None else []
        received_ages = [int((captured - row["received_at"]).total_seconds()) for row in successful]
        source_ages = [int((captured - row["source_updated_at"]).total_seconds()) for row in successful if row["source_updated_at"]]
        if any(age < 0 for age in received_ages + source_ages):
            issues.append(f"future_timestamp:{'/'.join(key)}")
        result_groups.append({
            "window": key[0], "resource": key[1], "partition": key[2], "version": key[3],
            "attempt_count": len(rows),
            "page_attempts": [
                {"page": row["page"], "attempt": row["attempt"],
                 "http_status": row["http_status"],
                 "received_at": row["received_at"].isoformat(),
                 "source_updated_at": row["source_updated_at"].isoformat()
                 if row["source_updated_at"] else None,
                 "total_pages_reported": row["total_pages_reported"],
                 "file": row["file"], "sha256": row["sha256"],
                 "actual_sha256": row["actual_sha256"],
                 "hash_valid": row["hash_valid"],
                 "is_latest": row is latest_by_page[row["page"]],
                 "accepted": row is latest_by_page[row["page"]]
                 and 200 <= row["http_status"] < 300 and row["hash_valid"] is True}
                for row in sorted(rows, key=lambda item: (item["page"], item["attempt"]))
            ],
            "failed_http_attempt_count": sum(not 200 <= row["http_status"] < 300 for row in rows),
            "recovered_failed_http_attempt_count": sum(
                not 200 <= row["http_status"] < 300
                and latest_by_page[row["page"]]["attempt"] > row["attempt"]
                and latest_by_page[row["page"]] in successful
                for row in rows
            ),
            "successful_pages": [row["page"] for row in successful],
            "failed_latest_pages": failed, "total_pages_reported": total,
            "missing_pages_within_reported_total": missing,
            "observed_page_state": "all_reported_pages_observed" if total is not None and not missing and not failed else "partial_or_unknown",
            "max_received_age_seconds": max(received_ages, default=None),
            "max_source_updated_age_seconds": max(source_ages, default=None),
            "source_updated_timestamps_missing": sum(row["source_updated_at"] is None for row in successful),
            "coverage_state": "unknown_without_independent_reference",
            "freshness_state": "unknown_without_independent_reference_and_sla",
        })
    return {"kind": "pncp-offline-ledger-result-v1", "captured_at": manifest["captured_at"],
            "evidence_class": manifest.get("evidence_class", "unspecified"),
            "groups": result_groups, "issues": sorted(set(issues))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    try:
        result = audit(args.manifest)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(2, f"manifest error: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["issues"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
