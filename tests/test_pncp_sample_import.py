"""Synthetic P03 import and two-reviewer labeling tests."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from pncp_sample_import import compare_labels, import_sample, label_template  # noqa: E402


def _fixture(tmp_path):
    body = b'{"descricao":"mouse sintetico","numeroItem":1}'
    (tmp_path / "item.json").write_bytes(body)
    manifest = {"kind": "pncp-sample-manifest-v1", "partition": "development",
                "evidence_class": "E2", "captured_at": "2026-11-19T12:00:00Z",
                "cases": [{"source_id": "SINTETICO-001", "version": "v1", "resources": [{
                    "kind": "items", "resource_id": "1", "file": "item.json",
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "source_url": "https://example.invalid/sintetico/001/items",
                    "received_at": "2026-11-19T11:00:00Z"}]}]}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path, manifest


def _filled(template, decision):
    for label in template["labels"]:
        label.update(decision=decision, reason="revisao sintetica", evidence_refs=["items:1"])
    return template


def test_import_preserves_manifest_and_resource_hashes(tmp_path):
    path, manifest = _fixture(tmp_path)
    inventory = import_sample(path)
    assert inventory["manifest_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert inventory["cases"][0]["resources"][0]["sha256"] == manifest["cases"][0]["resources"][0]["sha256"]
    assert inventory["partition"] == "development"
    assert inventory["cases"][0]["resources"][0]["source_updated_at"] is None
    assert len(label_template(inventory, "reviewer-A")["labels"]) == 4


def test_hash_change_and_escape_are_rejected(tmp_path):
    path, manifest = _fixture(tmp_path)
    (tmp_path / "item.json").write_bytes(b"changed")
    with pytest.raises(ValueError, match="sha256 mismatch"):
        import_sample(path)
    manifest["cases"][0]["resources"][0]["file"] = "../outside.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="path escapes"):
        import_sample(path)


def test_duplicate_case_and_resource_rejected(tmp_path):
    path, manifest = _fixture(tmp_path)
    manifest["cases"].append(manifest["cases"][0])
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate case"):
        import_sample(path)
    manifest["cases"].pop()
    manifest["cases"][0]["resources"].append(manifest["cases"][0]["resources"][0])
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate resource"):
        import_sample(path)


def test_independent_review_disagreement_stays_unresolved(tmp_path):
    inventory = import_sample(_fixture(tmp_path)[0])
    left = _filled(label_template(inventory, "reviewer-A"), "positive")
    right = _filled(label_template(inventory, "reviewer-B"), "negative")
    comparison = compare_labels(left, right)
    assert comparison["rows"][0]["state"] == "needs_adjudication"
    assert comparison["ready_for_adjudicated_metrics"] is False


def test_uncertain_and_incomplete_labels_never_count_as_agreement(tmp_path):
    inventory = import_sample(_fixture(tmp_path)[0])
    left = _filled(label_template(inventory, "reviewer-A"), "uncertain")
    right = _filled(label_template(inventory, "reviewer-B"), "uncertain")
    assert compare_labels(left, right)["rows"][0]["state"] == "needs_adjudication"
    right["labels"][0]["reason"] = None
    with pytest.raises(ValueError, match="reason"):
        compare_labels(left, right)


def test_same_reviewer_or_snapshot_is_rejected(tmp_path):
    inventory = import_sample(_fixture(tmp_path)[0])
    left = _filled(label_template(inventory, "reviewer-A"), "positive")
    right = _filled(label_template(inventory, "reviewer-A"), "positive")
    with pytest.raises(ValueError, match="distinct reviewers"):
        compare_labels(left, right)
    right["reviewer_id"] = "reviewer-B"
    right["manifest_sha256"] = "different"
    with pytest.raises(ValueError, match="different sample snapshots"):
        compare_labels(left, right)


def test_cli_import_and_template(tmp_path):
    path, _ = _fixture(tmp_path)
    script = Path(__file__).resolve().parents[1] / "scripts" / "pncp_sample_import.py"
    imported = subprocess.run([sys.executable, str(script), "import", str(path)],
                              capture_output=True, text=True)
    assert imported.returncode == 0
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(imported.stdout, encoding="utf-8")
    templated = subprocess.run([sys.executable, str(script), "template", str(inventory_path), "reviewer-A"],
                               capture_output=True, text=True)
    assert templated.returncode == 0
    assert len(json.loads(templated.stdout)["labels"]) == 4
