"""Mostra o estado atual da coleta de dados.

Uso:
    python scripts/collect_status.py
    python scripts/collect_status.py --days 3   # últimos 3 dias (padrão: 7)
    python scripts/collect_status.py --json     # saída em JSON
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from app.core.database import engine  # noqa: E402


# ── queries ───────────────────────────────────────────────────────────────────

_SOURCE_RUNS_SUMMARY = text(
    """
    SELECT
        source_name,
        COUNT(*)                                            AS total_runs,
        SUM(records_loaded)                                 AS total_loaded,
        SUM(records_extracted)                              AS total_extracted,
        SUM(CASE WHEN status = 'success'  THEN 1 ELSE 0 END) AS runs_ok,
        SUM(CASE WHEN status = 'failed'   THEN 1 ELSE 0 END) AS runs_failed,
        SUM(CASE WHEN status = 'partial'  THEN 1 ELSE 0 END) AS runs_partial,
        MAX(finished_at)                                    AS last_run_at,
        MAX(CASE WHEN status = 'success' THEN finished_at END) AS last_success_at
    FROM control.source_runs
    WHERE started_at >= NOW() - :window
    GROUP BY source_name
    ORDER BY last_run_at DESC NULLS LAST
    """
)

_BRONZE_COUNTS = text(
    """
    SELECT
        'market_web_listings_raw'    AS table_name,
        source_name,
        fetched_date,
        COUNT(*)                     AS rows,
        SUM(CASE WHEN blocked THEN 1 ELSE 0 END) AS blocked
    FROM bronze.market_web_listings_raw
    WHERE fetched_date >= CURRENT_DATE - :days
    GROUP BY source_name, fetched_date

    UNION ALL

    SELECT
        'mercado_livre_items_raw',
        source_name,
        fetched_date,
        COUNT(*),
        0
    FROM bronze.mercado_livre_items_raw
    WHERE fetched_date >= CURRENT_DATE - :days
    GROUP BY source_name, fetched_date

    UNION ALL

    SELECT
        'mercado_livre_products_raw',
        source_name,
        fetched_date,
        COUNT(*),
        0
    FROM bronze.mercado_livre_products_raw
    WHERE fetched_date >= CURRENT_DATE - :days
    GROUP BY source_name, fetched_date

    ORDER BY table_name, source_name, fetched_date
    """
)

_RECENT_FAILURES = text(
    """
    SELECT
        sr.source_name,
        sr.status,
        sr.records_loaded,
        sr.error_message,
        sr.finished_at
    FROM control.source_runs sr
    WHERE sr.status IN ('failed', 'partial')
      AND sr.started_at >= NOW() - :window
    ORDER BY sr.finished_at DESC NULLS LAST
    LIMIT 10
    """
)

_QUALITY_FAILURES = text(
    """
    SELECT
        dq.schema_name || '.' || dq.table_name AS table_ref,
        dq.check_name,
        dq.status,
        dq.metric_value,
        dq.message,
        dq.created_at
    FROM control.data_quality_checks dq
    WHERE dq.status IN ('failed', 'warning')
      AND dq.created_at >= NOW() - :window
    ORDER BY dq.created_at DESC
    LIMIT 8
    """
)


# ── formatters ────────────────────────────────────────────────────────────────

def _ago(dt: datetime | None) -> str:
    if dt is None:
        return "nunca"
    now = datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}s atrás"
    if s < 3600:
        return f"{s // 60}min atrás"
    if s < 86400:
        return f"{s // 3600}h atrás"
    return f"{s // 86400}d atrás"


def _status_icon(status: str) -> str:
    return {"success": "✓", "partial": "~", "failed": "✗", "running": "⟳"}.get(status, "?")


def _col(value: object, width: int, align: str = "<") -> str:
    return f"{str(value):{align}{width}}"


def _sep(char: str = "─", width: int = 72) -> None:
    print(char * width)


# ── report ────────────────────────────────────────────────────────────────────

def print_report(days: int) -> dict:
    window = timedelta(days=days)
    window_interval = f"{days} days"

    now = datetime.now(tz=timezone.utc)
    report: dict = {"generated_at": now.isoformat(timespec="seconds"), "days": days}

    with engine.connect() as conn:
        source_runs = conn.execute(_SOURCE_RUNS_SUMMARY, {"window": window_interval}).fetchall()
        bronze_rows = conn.execute(_BRONZE_COUNTS, {"days": days}).fetchall()
        failures = conn.execute(_RECENT_FAILURES, {"window": window_interval}).fetchall()
        quality_failures = conn.execute(_QUALITY_FAILURES, {"window": window_interval}).fetchall()

    # ── section 1: source run health ─────────────────────────────────────────
    _sep("═")
    print(f"  STATUS DA COLETA — últimos {days} dias   ({now.strftime('%Y-%m-%d %H:%M')} UTC)")
    _sep("═")

    print()
    print("RUNS POR FONTE")
    _sep()
    hdr = (
        _col("fonte", 26)
        + _col("runs", 6, ">")
        + _col("ok", 5, ">")
        + _col("parcial", 8, ">")
        + _col("falha", 6, ">")
        + _col("carregados", 11, ">")
        + "  último run"
    )
    print(hdr)
    _sep()

    sr_data = []
    for row in source_runs:
        icon = _status_icon("success" if row.runs_failed == 0 else ("partial" if row.runs_ok > 0 else "failed"))
        line = (
            _col(f"{icon} {row.source_name}", 26)
            + _col(row.total_runs, 6, ">")
            + _col(row.runs_ok, 5, ">")
            + _col(row.runs_partial, 8, ">")
            + _col(row.runs_failed, 6, ">")
            + _col(row.total_loaded or 0, 11, ">")
            + f"  {_ago(row.last_run_at)}"
        )
        print(line)
        sr_data.append(dict(row._mapping))
    if not source_runs:
        print("  (nenhum run encontrado)")
    report["source_runs"] = sr_data

    # ── section 2: bronze rows per source per day ─────────────────────────────
    print()
    print("LINHAS BRONZE POR FONTE/DIA")
    _sep()

    # Group by table + source, then show one column per date
    from collections import defaultdict
    dates_seen: set = set()
    grid: dict[tuple, dict] = defaultdict(dict)
    blocked_grid: dict[tuple, dict] = defaultdict(dict)
    for row in bronze_rows:
        key = (row.table_name, row.source_name)
        d = str(row.fetched_date)
        dates_seen.add(d)
        grid[key][d] = int(row.rows)
        blocked_grid[key][d] = int(row.blocked)

    sorted_dates = sorted(dates_seen)
    date_headers = [d[5:] for d in sorted_dates]  # MM-DD

    if sorted_dates:
        head = _col("tabela / fonte", 42) + "".join(_col(h, 8, ">") for h in date_headers)
        print(head)
        _sep()
        prev_table = None
        for (table, source), day_counts in sorted(grid.items()):
            if table != prev_table:
                print(f"  {table}")
                prev_table = table
            total = sum(day_counts.values())
            row_line = _col(f"    {source}", 42) + "".join(
                _col(day_counts.get(d, ""), 8, ">") for d in sorted_dates
            )
            print(row_line)
    else:
        print("  (nenhum dado bronze nos últimos dias)")

    report["bronze_by_source_day"] = {
        f"{t}/{s}": days_d for (t, s), days_d in grid.items()
    }

    # ── section 3: recent failures ────────────────────────────────────────────
    print()
    print("RUNS COM PROBLEMA (últimos)")
    _sep()
    if failures:
        for row in failures:
            err = (row.error_message or "")[:80].replace("\n", " ")
            print(
                f"  {_status_icon(row.status)} {_col(row.source_name, 24)}"
                f" loaded={row.records_loaded or 0:<6}  {_ago(row.finished_at)}"
            )
            if err:
                print(f"      {err}")
    else:
        print("  Nenhum run com falha ou parcial.")
    report["recent_failures"] = [dict(r._mapping) for r in failures]

    # ── section 4: quality checks ─────────────────────────────────────────────
    print()
    print("QUALITY CHECKS COM FALHA (últimos)")
    _sep()
    if quality_failures:
        for row in quality_failures:
            msg = (row.message or "")[:70]
            print(
                f"  ✗ {_col(row.table_ref, 36)} {_col(row.check_name, 36)}"
                f"  {_ago(row.created_at)}"
            )
            if msg:
                print(f"      {msg}")
    else:
        print("  Nenhum quality check com falha.")
    report["quality_failures"] = [dict(r._mapping) for r in quality_failures]

    _sep("═")
    return report


# ── entrypoint ────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mostra o estado da coleta de dados.")
    parser.add_argument("--days", type=int, default=7, help="Janela de análise em dias (padrão: 7).")
    parser.add_argument("--json", action="store_true", help="Saída em JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = print_report(days=args.days)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
