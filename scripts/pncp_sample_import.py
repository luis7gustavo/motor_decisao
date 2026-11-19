"""Offline sample inventory and independent-label comparison for P03.

This imports only an explicit, already authorized manifest. It does not infer
commercial labels, fetch PNCP data, or expose a blind partition automatically.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

CATEGORIES = ("mouse", "keyboard", "webcam", "headset")


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name}: nonempty string required")
    return value


def _timestamp(value: Any, name: str) -> str:
    value = _required_text(value, name)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name}: invalid timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name}: timezone required")
    return value


def _raw_file(root: Path, name: Any, digest: Any) -> tuple[str, str]:
    name = _required_text(name, "file")
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("file: path escapes manifest directory")
    target = (root / relative).resolve()
    if not target.is_relative_to(root.resolve()) or not target.is_file():
        raise ValueError("file: missing or outside manifest directory")
    digest = _required_text(digest, "sha256").lower()
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("sha256: 64 hexadecimal characters required")
    if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
        raise ValueError(f"sha256 mismatch: {name}")
    try:
        json.loads(target.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"file: invalid JSON: {name}") from exc
    return name, digest


def import_sample(manifest_path: Path) -> dict[str, Any]:
    raw = manifest_path.read_bytes()
    manifest = json.loads(raw)
    if not isinstance(manifest, dict) or manifest.get("kind") != "pncp-sample-manifest-v1":
        raise ValueError("kind: pncp-sample-manifest-v1 required")
    partition = manifest.get("partition")
    if partition not in ("development", "validation", "blind"):
        raise ValueError("partition: development, validation or blind required")
    captured_at = _timestamp(manifest.get("captured_at"), "captured_at")
    evidence = manifest.get("evidence_class")
    if evidence not in ("E0", "E1", "E2", "E3", "E4"):
        raise ValueError("evidence_class: E0..E4 required")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("cases: nonempty list required")
    seen_cases: set[tuple[str, str]] = set()
    normalized = []
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("case: object required")
        source_id = _required_text(case.get("source_id"), "source_id")
        version = _required_text(case.get("version"), "version")
        if (source_id, version) in seen_cases:
            raise ValueError(f"duplicate case: {source_id}/{version}")
        seen_cases.add((source_id, version))
        resources = case.get("resources")
        if not isinstance(resources, list) or not resources:
            raise ValueError(f"resources: nonempty list required: {source_id}/{version}")
        seen_resources: set[tuple[str, str]] = set()
        normalized_resources = []
        for resource in resources:
            if not isinstance(resource, dict):
                raise ValueError("resource: object required")
            kind = _required_text(resource.get("kind"), "resource.kind")
            resource_id = _required_text(resource.get("resource_id"), "resource.resource_id")
            if (kind, resource_id) in seen_resources:
                raise ValueError(f"duplicate resource: {source_id}/{version}/{kind}/{resource_id}")
            seen_resources.add((kind, resource_id))
            file_name, digest = _raw_file(manifest_path.parent, resource.get("file"), resource.get("sha256"))
            source_updated = resource.get("source_updated_at")
            normalized_resources.append({"kind": kind, "resource_id": resource_id,
                                         "file": file_name, "sha256": digest,
                                         "source_url": _required_text(resource.get("source_url"), "source_url"),
                                         "received_at": _timestamp(resource.get("received_at"), "received_at"),
                                         "source_updated_at": _timestamp(source_updated, "source_updated_at") if source_updated is not None else None})
        normalized.append({"source_id": source_id, "version": version,
                           "resources": sorted(normalized_resources, key=lambda item: (item["kind"], item["resource_id"]))})
    return {"kind": "pncp-sample-inventory-v1", "partition": partition,
            "evidence_class": evidence, "captured_at": captured_at,
            "manifest_sha256": hashlib.sha256(raw).hexdigest(),
            "cases": sorted(normalized, key=lambda item: (item["source_id"], item["version"]))}


def label_template(inventory: dict[str, Any], reviewer_id: str) -> dict[str, Any]:
    if inventory.get("kind") != "pncp-sample-inventory-v1":
        raise ValueError("inventory kind mismatch")
    reviewer_id = _required_text(reviewer_id, "reviewer_id")
    return {"kind": "pncp-labels-v1", "reviewer_id": reviewer_id,
            "manifest_sha256": inventory["manifest_sha256"], "partition": inventory["partition"],
            "labels": [{"source_id": case["source_id"], "version": case["version"],
                        "category": category, "decision": None, "reason": None,
                        "evidence_refs": []}
                       for case in inventory["cases"] for category in CATEGORIES]}


def compare_labels(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    if left.get("kind") != "pncp-labels-v1" or right.get("kind") != "pncp-labels-v1":
        raise ValueError("label kind mismatch")
    _required_text(left.get("reviewer_id"), "reviewer_id")
    _required_text(right.get("reviewer_id"), "reviewer_id")
    _required_text(left.get("manifest_sha256"), "manifest_sha256")
    if left.get("partition") not in ("development", "validation", "blind"):
        raise ValueError("partition invalid")
    if left.get("reviewer_id") == right.get("reviewer_id"):
        raise ValueError("two distinct reviewers required")
    if (left.get("manifest_sha256"), left.get("partition")) != (right.get("manifest_sha256"), right.get("partition")):
        raise ValueError("labels refer to different sample snapshots")

    def index(document: dict[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
        labels = document.get("labels")
        if not isinstance(labels, list):
            raise ValueError("labels: list required")
        indexed = {}
        for label in labels:
            if not isinstance(label, dict):
                raise ValueError("label: object required")
            key = (_required_text(label.get("source_id"), "source_id"),
                   _required_text(label.get("version"), "version"),
                   _required_text(label.get("category"), "category"))
            if key[2] not in CATEGORIES:
                raise ValueError(f"category invalid: {key}")
            if key in indexed:
                raise ValueError(f"duplicate label: {key}")
            decision = label.get("decision")
            if decision not in ("positive", "negative", "uncertain"):
                raise ValueError(f"decision missing or invalid: {key}")
            _required_text(label.get("reason"), "reason")
            refs = label.get("evidence_refs")
            if not isinstance(refs, list) or not refs or any(not isinstance(ref, str) or not ref.strip() for ref in refs):
                raise ValueError(f"evidence_refs missing or invalid: {key}")
            indexed[key] = label
        return indexed

    a, b = index(left), index(right)
    if a.keys() != b.keys():
        raise ValueError("reviewers labeled different cases")
    rows = []
    for key in sorted(a):
        a_decision, b_decision = a[key]["decision"], b[key]["decision"]
        rows.append({"source_id": key[0], "version": key[1], "category": key[2],
                     "left_decision": a_decision, "right_decision": b_decision,
                     "state": "agreement" if a_decision == b_decision and a_decision != "uncertain" else "needs_adjudication"})
    return {"kind": "pncp-label-comparison-v1", "manifest_sha256": left["manifest_sha256"],
            "partition": left["partition"], "reviewers": [left["reviewer_id"], right["reviewer_id"]],
            "rows": rows, "ready_for_adjudicated_metrics": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    importing = sub.add_parser("import")
    importing.add_argument("manifest", type=Path)
    template = sub.add_parser("template")
    template.add_argument("inventory", type=Path)
    template.add_argument("reviewer_id")
    comparing = sub.add_parser("compare")
    comparing.add_argument("left", type=Path)
    comparing.add_argument("right", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "import":
            result = import_sample(args.manifest)
        elif args.command == "template":
            result = label_template(json.loads(args.inventory.read_text(encoding="utf-8")), args.reviewer_id)
        else:
            result = compare_labels(json.loads(args.left.read_text(encoding="utf-8")),
                                    json.loads(args.right.read_text(encoding="utf-8")))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(2, f"input error: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
