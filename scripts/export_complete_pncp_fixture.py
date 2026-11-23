"""Export a bounded public fixture from complete PNCP read-model observations."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3


SOURCE = "pncp_api"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def export_complete_fixture(database: Path, directory: Path, *, limit: int = 100) -> dict:
    if not 1 <= limit <= 350:
        raise ValueError("limit must be between 1 and 350")

    database = database.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """SELECT l.identity,l.version,l.acquired_at,o.payload
               FROM latest_acquisition l
               JOIN observations o
                 ON o.source=l.source
                AND o.identity=l.identity
                AND o.version=l.version
              WHERE l.source=?
              ORDER BY l.acquired_at DESC,l.identity
            """,
            (SOURCE,),
        ).fetchall()
    finally:
        connection.close()

    selected = []
    total_complete = 0
    for row in rows:
        payload = json.loads(row["payload"])
        coverage = payload.get("coverage", {})
        if not coverage.get("observed_resources_complete", False):
            continue
        total_complete += 1
        if len(selected) < limit:
            selected.append((row, payload, coverage))

    entries = []
    for row, payload, coverage in selected:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        digest = hashlib.sha256(body).hexdigest()
        if digest != row["version"]:
            raise ValueError(f"content hash mismatch for {row['identity']}")
        filename = digest + ".json"
        target = directory / filename
        if target.exists():
            if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                raise ValueError(f"existing fixture changed: {filename}")
        else:
            target.write_bytes(body)
        entries.append(
            {
                "identity": row["identity"],
                "version": row["version"],
                "acquired_at": row["acquired_at"],
                "file": filename,
                "sha256": digest,
                "items": len(payload.get("items", [])),
                "results": len(payload.get("results", [])),
                "documents": len(payload.get("documents", [])),
                "coverage": coverage,
            }
        )

    manifest = {
        "schema_version": "radar-pncp-public-complete-fixture-1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE,
        "source_database_sha256": _sha256(database),
        "source_latest_identities": len(rows),
        "source_complete_identities": total_complete,
        "selection": {
            "criterion": "latest acquisition with observed_resources_complete=true",
            "order": "acquired_at descending, identity ascending",
            "limit": limit,
        },
        "entries": entries,
        "limits": {
            "source_snapshot_verified": False,
            "commercial_eligibility_verified": False,
            "national_coverage_claimed": False,
            "freshness_sla_claimed": False,
        },
    }
    manifest_body = json.dumps(
        manifest, ensure_ascii=False, sort_keys=True, indent=2
    ).encode("utf-8")
    manifest_path = directory / "manifest.json"
    manifest_path.write_bytes(manifest_body)
    return {
        "manifest": str(manifest_path),
        "exported": len(entries),
        "source_latest_identities": len(rows),
        "source_complete_identities": total_complete,
        "source_database_sha256": manifest["source_database_sha256"],
        "network_calls": 0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    print(
        json.dumps(
            export_complete_fixture(args.database, args.directory, limit=args.limit),
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
