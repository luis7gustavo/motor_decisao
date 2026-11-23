import hashlib
import json
import sqlite3

import pytest

from scripts.export_complete_pncp_fixture import export_complete_fixture


def _payload(identity: str, complete: bool) -> dict:
    return {
        "identity": identity,
        "items": [{"id": "1"}],
        "results": [],
        "documents": [{"id": "1"}],
        "coverage": {
            "observed_resources_complete": complete,
            "source_snapshot_verified": False,
            "commercial_eligibility_verified": False,
        },
    }


def _database(path):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE observations (
            source TEXT NOT NULL,
            identity TEXT NOT NULL,
            version TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY(source,identity,version)
        );
        CREATE TABLE latest_acquisition (
            source TEXT NOT NULL,
            identity TEXT NOT NULL,
            version TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            PRIMARY KEY(source,identity)
        );
        """
    )
    for index, complete in enumerate((True, False, True), start=1):
        identity = f"case-{index}"
        payload = json.dumps(
            _payload(identity, complete),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        version = hashlib.sha256(payload.encode()).hexdigest()
        acquired_at = f"2026-11-2{index}T00:00:00+00:00"
        connection.execute(
            "INSERT INTO observations VALUES (?,?,?,?,?)",
            ("pncp_api", identity, version, acquired_at, payload),
        )
        connection.execute(
            "INSERT INTO latest_acquisition VALUES (?,?,?,?)",
            ("pncp_api", identity, version, acquired_at),
        )
    connection.commit()
    connection.close()


def test_exports_only_latest_complete_observations(tmp_path):
    database = tmp_path / "read-model.sqlite"
    output = tmp_path / "fixture"
    _database(database)

    result = export_complete_fixture(database, output, limit=2)
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

    assert result["exported"] == 2
    assert result["source_latest_identities"] == 3
    assert result["source_complete_identities"] == 2
    assert [entry["identity"] for entry in manifest["entries"]] == [
        "case-3",
        "case-1",
    ]
    assert all(
        entry["coverage"]["observed_resources_complete"]
        for entry in manifest["entries"]
    )
    assert manifest["limits"]["national_coverage_claimed"] is False


def test_refuses_limit_above_reviewed_complete_cohort(tmp_path):
    database = tmp_path / "read-model.sqlite"
    _database(database)

    with pytest.raises(ValueError, match="between 1 and 350"):
        export_complete_fixture(database, tmp_path / "fixture", limit=351)
