"""Read-only proof that the active demo database contains only captured PNCP data."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APPROVED = (ROOT / "backend" / "data" / "demo").resolve()
ORIGINAL = (ROOT / "backend" / "radar_licita.db").resolve()
FIXTURE = ROOT / "examples" / "demo" / "fixtures" / "official" / "PNCP_TECNOLOGIA_HISTORICA_EXPANDIDA_2026-10-15.json"
FIXTURE_SHA256 = "503ce560e9a0ef8ca755cec706ac0be2a3170a78878777e337fa661bda9f064d"


def check(path: Path) -> dict:
    resolved = path.expanduser().resolve(strict=True)
    if resolved == ORIGINAL or APPROVED not in resolved.parents or resolved.suffix.lower() != ".db":
        raise ValueError(f"Banco fora do diretório isolado: {resolved}")
    fixture_bytes = FIXTURE.read_bytes()
    if hashlib.sha256(fixture_bytes).hexdigest() != FIXTURE_SHA256:
        raise ValueError("Captura oficial difere do hash conferido")
    capture = json.loads(fixture_bytes)
    records = capture["records"]
    expected_controls = {record["purchase"]["numeroControlePNCP"] for record in records}
    expected_items = {(record["purchase"]["numeroControlePNCP"], int(entry["item"]["numeroItem"]))
                      for record in records for entry in record["items"]}
    expected_observations = {(record["purchase"]["numeroControlePNCP"], int(entry["item"]["numeroItem"]),
                              digest, str(result["niFornecedor"]))
                             for record in records for entry in record["items"]
                             for result, digest in zip(entry["results"], entry["result_hashes"], strict=True)}
    source_observations = {(record["purchase"]["numeroControlePNCP"], int(entry["item"]["numeroItem"]),
                            digest, str(result["niFornecedor"])): result
                           for record in records for entry in record["items"]
                           for result, digest in zip(entry["results"], entry["result_hashes"], strict=True)}
    expected_companies = {row[3] for row in expected_observations}
    with closing(sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True)) as db:
        db.execute("PRAGMA query_only=ON")
        integrity = db.execute("PRAGMA quick_check").fetchone()[0]
        if integrity != "ok" or db.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError(f"Integridade SQLite inválida: {integrity}")
        version = db.execute("SELECT version_num FROM alembic_version").fetchone()[0]
        if version != "0002":
            raise ValueError(f"Migração inesperada: {version}")
        purchases = db.execute("SELECT official_control_number, raw_json, is_open FROM procurements").fetchall()
        if {row[0] for row in purchases} != expected_controls or any(row[2] for row in purchases):
            raise ValueError("Compras fora do acervo histórico oficial")
        source_purchases = {record["purchase"]["numeroControlePNCP"]: record["purchase"] for record in records}
        if any(json.loads(raw) != source_purchases[control] for control, raw, _ in purchases):
            raise ValueError("Payload da contratação difere da fonte capturada")
        items = db.execute("SELECT p.official_control_number, i.item_number, i.raw_json "
                           "FROM procurement_items i JOIN procurements p ON p.id=i.procurement_id").fetchall()
        if {(control, number) for control, number, _ in items} != expected_items:
            raise ValueError("Itens fora do acervo oficial")
        source_items = {(record["purchase"]["numeroControlePNCP"], int(entry["item"]["numeroItem"])): entry["item"]
                        for record in records for entry in record["items"]}
        if any(json.loads(raw) != source_items[(control, number)] for control, number, raw in items):
            raise ValueError("Payload de item difere da fonte capturada")
        companies = {row[0] for row in db.execute("SELECT tax_id FROM partner_candidates")}
        if companies != expected_companies:
            raise ValueError("Empresas fora dos resultados oficiais")
        observations = db.execute(
            "SELECT o.official_control_number, o.item_number, o.source_payload_hash, c.tax_id, o.raw_json "
            "FROM partner_observations o JOIN partner_candidates c ON c.id=o.candidate_id").fetchall()
        if {row[:4] for row in observations} != expected_observations:
            raise ValueError("Cruzamento empresa/item difere dos resultados oficiais")
        if any(json.loads(raw) != source_observations[(control, number, digest, tax_id)]
               for control, number, digest, tax_id, raw in observations):
            raise ValueError("Payload de resultado difere da fonte capturada")
        counts = {table: db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("procurements", "procurement_items", "partner_candidates", "partner_observations",
                                "partner_search_runs", "suppliers", "supplier_products", "supplier_quotes",
                                "financial_scenarios", "opportunity_scores", "product_matches", "procurement_documents")}
        if any(counts[table] for table in ("suppliers", "supplier_products", "supplier_quotes", "financial_scenarios",
                                           "opportunity_scores", "product_matches", "procurement_documents")):
            raise ValueError("Base ativa contém dados comerciais ou cenários alheios ao acervo oficial")
        if (counts["procurements"] != len(expected_controls)
                or counts["procurement_items"] != len(expected_items)
                or counts["partner_candidates"] != len(expected_companies)
                or counts["partner_observations"] != len(expected_observations)
                or counts["partner_search_runs"] != len(expected_items)):
            raise ValueError("Contagens do banco não correspondem à captura oficial")
    return {"database": str(resolved), "quick_check": integrity, "alembic_version": version,
            "counts": counts, "fixture_sha256": FIXTURE_SHA256,
            "controls": sorted(expected_controls)}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Uso: check_demo_db.py <arquivo .db isolado>")
    try:
        print(json.dumps(check(Path(sys.argv[1])), ensure_ascii=False))
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError) as exc:
        raise SystemExit(f"Falha na validação da demo: {exc}") from exc
