"""Read-only discovery over an isolated, frozen C2 restore.

This module deliberately cannot point at the operational C2 or principal DB.
"""

import os
from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from app.radar_c2_contract import from_current_version


PAGE_SIZE = 20
MAX_PAGE = 500
SNAPSHOT_AT = ("05/11/2026 16:01:34 America/Sao_Paulo "
               "(mtime do dump; instante transacional não registrado)")
BACKUP_SHA256 = "1c0d1fdd7970eec7deb1640ca7df54cf95f3983db86894f1b7c7eefe7499f424"


class PreviewUnavailable(Exception):
    """The isolated copy is not configured or cannot be read."""


def _engine_url() -> str:
    value = os.environ.get("RADAR_PREVIEW_DATABASE_URL", "")
    if not value:
        raise PreviewUnavailable("Cópia isolada não configurada")
    url = make_url(value)
    allowed = {("127.0.0.1", 55434), ("localhost", 55434),
               ("host.docker.internal", 55434), ("radar-preview-pg-20261105", 5432)}
    if (url.database != "radar_preview_20261105" or url.username != "radar_reader"
            or (url.host, url.port) not in allowed):
        raise PreviewUnavailable("Destino da prévia fora da cópia isolada")
    return value


@lru_cache(maxsize=1)
def _engine():
    return create_engine(_engine_url(), pool_size=2, max_overflow=0,
                         pool_timeout=3, connect_args={"connect_timeout": 3})


def _pattern(value: str) -> str:
    return "%" + value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _record(row):
    metadata = from_current_version(source_id=row["source_id"],
                                    version=row["version_no"], snapshot=row["snapshot"])
    return {
        "source_id": metadata.source_id, "version": metadata.version,
        "object": metadata.object, "uf": metadata.uf,
        "modality": metadata.modality, "official_state_raw": metadata.official_state_raw,
        "deadline_text_unverified": metadata.deadline_text_unverified,
        "opening_state": metadata.opening_state, "freshness": metadata.freshness,
        "version_id": str(row["version_id"]), "raw_response_id": str(row["raw_response_id"]),
    }


def _read(sql, params):
    # The role is ALTER ROLE ... SET default_transaction_read_only = on as a second guard.
    with _engine().connect() as conn:
        conn.exec_driver_sql("SET TRANSACTION READ ONLY")
        conn.exec_driver_sql("SET LOCAL statement_timeout = '2500ms'")
        return list(conn.execute(text(sql), params).mappings())


def search(*, q="", uf="", modality="", state="", page=1):
    if not 1 <= page <= MAX_PAGE:
        raise ValueError("Página fora do limite da prévia")
    clauses = []
    params = {"limit": PAGE_SIZE + 1, "offset": (page - 1) * PAGE_SIZE}
    if q:
        clauses.append("d.object_text ILIKE :pattern ESCAPE '\\'")
        params["pattern"] = _pattern(q)
    if uf:
        clauses.append("d.uf = :uf")
        params["uf"] = uf.upper()
    if modality:
        clauses.append("d.modality = :modality")
        params["modality"] = modality
    if state:
        clauses.append("d.official_state_raw = :state")
        params["state"] = state
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    rows = _read("""
        WITH page AS MATERIALIZED (
          SELECT d.source_id, d.procurement_id, d.version_id, d.version_no,
                 d.raw_response_id
          FROM radar_preview.discovery_current d
        """ + where + " ORDER BY d.source_id LIMIT :limit OFFSET :offset) " + """
        SELECT page.source_id, page.version_id, page.version_no,
               page.raw_response_id, v.snapshot
        FROM page
        JOIN procurement.procurement_versions v ON v.id = page.version_id
        JOIN procurement.procurements p ON p.id = page.procurement_id
          AND p.current_version_id = page.version_id
        WHERE NOT EXISTS (
          SELECT 1 FROM procurement.procurement_versions newer
          WHERE newer.procurement_id = page.procurement_id
            AND newer.version_no > page.version_no
        )
        ORDER BY page.source_id
        """, params)
    return [_record(row) for row in rows[:PAGE_SIZE]], len(rows) > PAGE_SIZE


def detail(source_id: str):
    rows = _read("""
        SELECT d.source_id, d.version_id, d.version_no, d.raw_response_id, v.snapshot
        FROM radar_preview.discovery_current d
        JOIN procurement.procurement_versions v ON v.id = d.version_id
        JOIN procurement.procurements p ON p.id = d.procurement_id
          AND p.current_version_id = d.version_id
        WHERE d.source_id = :source_id
          AND NOT EXISTS (SELECT 1 FROM procurement.procurement_versions newer
                          WHERE newer.procurement_id = d.procurement_id
                            AND newer.version_no > d.version_no)
        LIMIT 1
    """, {"source_id": source_id})
    if not rows:
        return None, []
    row = _record(rows[0])
    versions = _read("""
        SELECT v.version_no, v.official_status, v.observed_at
        FROM procurement.procurement_versions v
        JOIN radar_preview.discovery_current d ON d.procurement_id = v.procurement_id
        WHERE d.source_id = :source_id AND v.version_no <= :current_version
        ORDER BY v.version_no DESC LIMIT 50
    """, {"source_id": source_id, "current_version": row["version"]})
    return row, [dict(v) for v in versions]
