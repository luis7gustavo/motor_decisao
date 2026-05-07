from __future__ import annotations

import traceback

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from threading import Lock, Thread

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import bindparam, text

from app.core.database import engine
from app.core.settings import get_settings
from pipelines.market_web.sources import SOURCE_CONFIGS
from pipelines.market_web.ingest import ingest_market_web
from pipelines.price_history.ingest import ingest_price_comparison_web_history
from pipelines.mercado_livre.ingest import ingest_mercado_livre_category
from scripts.collect_market_web import DEFAULT_QUERIES as MARKET_WEB_DEFAULT_QUERIES
from scripts.collect_price_history_web import DEFAULT_QUERIES as PRICE_HISTORY_DEFAULT_QUERIES
from scripts.collect_mercado_livre_perifericos import DEFAULT_QUERIES as MERCADO_LIVRE_DEFAULT_QUERIES
from scripts.notify_discord import main as notify_discord_main

router = APIRouter(prefix="/ops", tags=["operations"])

# 2026-05-02: push the safer Bronze sources harder now that scraper fan-out is
# coordinated per stage instead of relying on a fully serial HTTP cycle.
BRONZE_MARKET_WEB_MAX_RESULTS = 30
BRONZE_PRICE_HISTORY_MAX_RESULTS = 8
BRONZE_MERCADO_LIVRE_MAX_ITEMS = 40
BRONZE_MERCADO_LIVRE_PAGE_LIMIT = 20
BRONZE_MERCADO_LIVRE_CATALOG_PRODUCT_SCAN_LIMIT = 160
BRONZE_MERCADO_LIVRE_CATALOG_OFFER_SCAN_LIMIT = 30
_BRONZE_CYCLE_LOCK = Lock()
_BRONZE_CYCLE_THREAD: Thread | None = None
_BRONZE_CYCLE_STARTED_AT: datetime | None = None
_BRONZE_LOCK_STALE_AFTER = timedelta(minutes=20)
_BRONZE_PIPELINE_NAMES = (
    "market_web_ingestion",
    "price_comparison_web_history_ingestion",
    "mercado_livre_category_ingestion",
)
_BRONZE_TRIGGERED_BY = "ops_http_bronze_cycle"
_MARKET_WEB_SOURCE_PRIORITY = {
    "amazon": 10,
    "kabum": 20,
    "shopee": 30,
    "terabyte": 40,
    "aliexpress": 90,
    "magalu": 100,
    "pichau": 110,
}


def _serialize_db_row(row: dict[str, object]) -> dict[str, object]:
    serialized = dict(row)
    for key, value in list(serialized.items()):
        if hasattr(value, "isoformat"):
            serialized[key] = value.isoformat()
        elif value.__class__.__name__ == "UUID":
            serialized[key] = str(value)
    return serialized


def _repair_stale_runs(*, stale_after_hours: int = 8) -> dict[str, object]:
    cutoff_sql = text("NOW() - make_interval(hours => :hours)")
    pipeline_sql = text(
        """
        UPDATE control.pipeline_runs
        SET
            status = 'cancelled',
            error_message = COALESCE(
                error_message,
                :message
            ),
            finished_at = NOW(),
            updated_at = NOW()
        WHERE status = 'running'
          AND finished_at IS NULL
          AND created_at < (NOW() - make_interval(hours => :hours))
        RETURNING id, pipeline_name, created_at
        """
    )
    source_sql = text(
        """
        UPDATE control.source_runs
        SET
            status = 'cancelled',
            error_message = COALESCE(
                error_message,
                :message
            ),
            finished_at = NOW(),
            updated_at = NOW()
        WHERE status = 'running'
          AND finished_at IS NULL
          AND created_at < (NOW() - make_interval(hours => :hours))
        RETURNING id, source_name, created_at
        """
    )
    message = (
        f"Marked stale by /ops automation after {stale_after_hours}h without completion."
    )

    with engine.begin() as connection:
        # 2026-05-02: close abandoned running rows before a new Bronze cycle so
        # health dashboards and bad-streak logic reflect the real last outcome.
        pipeline_rows = connection.execute(
            pipeline_sql,
            {"hours": stale_after_hours, "message": message},
        ).mappings().all()
        source_rows = connection.execute(
            source_sql,
            {"hours": stale_after_hours, "message": message},
        ).mappings().all()

    return {
        "stale_after_hours": stale_after_hours,
        "cutoff_hint": str(timedelta(hours=stale_after_hours)),
        "pipelines_repaired": [_serialize_db_row(dict(row)) for row in pipeline_rows],
        "sources_repaired": [_serialize_db_row(dict(row)) for row in source_rows],
    }


def _find_active_bronze_runs() -> list[dict[str, object]]:
    sql = text(
        """
        SELECT
            id,
            pipeline_name,
            created_at,
            started_at,
            metadata
        FROM control.pipeline_runs
        WHERE status = 'running'
          AND finished_at IS NULL
          AND pipeline_name IN :pipeline_names
          AND COALESCE(metadata->>'triggered_by', '') = :triggered_by
        ORDER BY created_at DESC
        """
    ).bindparams(bindparam("pipeline_names", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(
            sql,
            {
                "pipeline_names": _BRONZE_PIPELINE_NAMES,
                "triggered_by": _BRONZE_TRIGGERED_BY,
            },
        ).mappings().all()
    serialized_rows = []
    for row in rows:
        item = _serialize_db_row(dict(row))
        item.pop("metadata", None)
        serialized_rows.append(item)
    return serialized_rows


def _cancel_pipeline_run(*, pipeline_run_id: str, reason: str) -> dict[str, object]:
    pipeline_sql = text(
        """
        UPDATE control.pipeline_runs
        SET
            status = 'cancelled',
            error_message = COALESCE(error_message, :reason),
            finished_at = NOW(),
            updated_at = NOW()
        WHERE id = CAST(:pipeline_run_id AS uuid)
        RETURNING id, pipeline_name, status, created_at, started_at, finished_at
        """
    )
    source_sql = text(
        """
        UPDATE control.source_runs
        SET
            status = 'cancelled',
            error_message = COALESCE(error_message, :reason),
            finished_at = NOW(),
            updated_at = NOW()
        WHERE pipeline_run_id = CAST(:pipeline_run_id AS uuid)
          AND status = 'running'
          AND finished_at IS NULL
        RETURNING id, source_name, status, created_at, started_at, finished_at
        """
    )
    with engine.begin() as connection:
        pipeline_row = connection.execute(
            pipeline_sql,
            {"pipeline_run_id": pipeline_run_id, "reason": reason},
        ).mappings().first()
        source_rows = connection.execute(
            source_sql,
            {"pipeline_run_id": pipeline_run_id, "reason": reason},
        ).mappings().all()

    if pipeline_row is None:
        raise HTTPException(status_code=404, detail="Pipeline run not found.")

    return {
        "pipeline": _serialize_db_row(dict(pipeline_row)),
        "sources_cancelled": [_serialize_db_row(dict(row)) for row in source_rows],
    }


def _ordered_enabled_market_sources(
    market_web_config: dict[str, object],
) -> list[str]:
    configured_sources = market_web_config.get("sources", {})
    if not isinstance(configured_sources, dict):
        configured_sources = {}

    candidate_names = list(dict.fromkeys([*configured_sources.keys(), *SOURCE_CONFIGS.keys()]))
    enabled_sources = [
        source_name
        for source_name in candidate_names
        if source_name in SOURCE_CONFIGS
        and configured_sources.get(source_name, {}).get("enabled", True)
    ]
    # 2026-05-02: run historically stable retailers first in the HTTP Bronze path
    # so a sticky anti-bot source does not block the whole cycle before good sources load.
    return sorted(
        enabled_sources,
        key=lambda source_name: (
            _MARKET_WEB_SOURCE_PRIORITY.get(source_name, 50),
            candidate_names.index(source_name),
        ),
    )


def _run_price_history_task(*, source_name: str, query: str) -> dict[str, object]:
    try:
        result = ingest_price_comparison_web_history(
            source_name=source_name,
            query=query,
            max_results=BRONZE_PRICE_HISTORY_MAX_RESULTS,
            product_detail_limit=BRONZE_PRICE_HISTORY_MAX_RESULTS,
            triggered_by=_BRONZE_TRIGGERED_BY,
        )
        return {
            "pipeline_run_id": str(result.pipeline_run_id),
            "source_run_id": str(result.source_run_id),
            "source_name": source_name,
            "query": query,
            "status": result.status,
            "extracted": result.extracted,
            "loaded": result.loaded,
            "skipped": result.skipped,
            "blocked": result.blocked,
        }
    except Exception as error:  # noqa: BLE001 - keep the rest of Bronze moving.
        return {
            "pipeline_run_id": None,
            "source_run_id": None,
            "source_name": source_name,
            "query": query,
            "status": "failed",
            "extracted": 0,
            "loaded": 0,
            "skipped": 0,
            "blocked": 0,
            "error": str(error),
        }


def _run_parallel_price_history(project_config: dict[str, object]) -> list[dict[str, object]]:
    price_history_config = project_config.get("market_sources", {}).get("price_history", {})
    web_config = price_history_config.get("web_history", {})
    bronze_sources = web_config.get("bronze_sources") or price_history_config.get("candidates") or [
        "buscape",
        "zoom",
    ]
    task_specs = [
        {"order": order, "source_name": source_name, "query": query}
        for order, (source_name, query) in enumerate(
            (source_name, query)
            for source_name in bronze_sources
            for query in PRICE_HISTORY_DEFAULT_QUERIES
        )
    ]
    if not task_specs:
        return []

    worker_count = min(
        max(1, int(web_config.get("parallel_workers", 1))),
        len(task_specs),
    )
    if worker_count <= 1:
        return [
            _run_price_history_task(
                source_name=spec["source_name"],
                query=spec["query"],
            )
            for spec in task_specs
        ]

    results_by_order: dict[int, dict[str, object]] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="price-history",
    ) as executor:
        future_to_spec = {
            executor.submit(
                _run_price_history_task,
                source_name=spec["source_name"],
                query=spec["query"],
            ): spec
            for spec in task_specs
        }
        for future in as_completed(future_to_spec):
            spec = future_to_spec[future]
            try:
                results_by_order[spec["order"]] = future.result()
            except Exception as error:  # noqa: BLE001 - reserve one failed slot and keep ordering stable.
                results_by_order[spec["order"]] = {
                    "pipeline_run_id": None,
                    "source_run_id": None,
                    "source_name": spec["source_name"],
                    "query": spec["query"],
                    "status": "failed",
                    "extracted": 0,
                    "loaded": 0,
                    "skipped": 0,
                    "blocked": 0,
                    "error": str(error),
                }

    return [results_by_order[index] for index in range(len(task_specs))]


def _run_mercado_livre_task(*, query: str) -> dict[str, object]:
    try:
        result = ingest_mercado_livre_category(
            query=query,
            max_items=BRONZE_MERCADO_LIVRE_MAX_ITEMS,
            page_limit=BRONZE_MERCADO_LIVRE_PAGE_LIMIT,
            catalog_product_scan_limit=BRONZE_MERCADO_LIVRE_CATALOG_PRODUCT_SCAN_LIMIT,
            catalog_offer_scan_limit=BRONZE_MERCADO_LIVRE_CATALOG_OFFER_SCAN_LIMIT,
            triggered_by=_BRONZE_TRIGGERED_BY,
        )
        return {
            "pipeline_run_id": str(result.pipeline_run_id),
            "source_run_id": str(result.source_run_id),
            "query": query,
            "status": result.status,
            "extracted": result.extracted,
            "loaded": result.loaded,
            "skipped": result.skipped,
            "pages": result.pages,
            "price_summaries": result.price_summaries,
            "catalog_products_loaded": result.catalog_products_loaded,
            "catalog_products_skipped": result.catalog_products_skipped,
        }
    except Exception as error:  # noqa: BLE001 - keep the batch moving even if one query degrades.
        return {
            "pipeline_run_id": None,
            "source_run_id": None,
            "query": query,
            "status": "failed",
            "extracted": 0,
            "loaded": 0,
            "skipped": 0,
            "pages": 0,
            "price_summaries": 0,
            "catalog_products_loaded": 0,
            "catalog_products_skipped": 0,
            "error": str(error),
        }


def _run_parallel_mercado_livre(project_config: dict[str, object]) -> list[dict[str, object]]:
    mercado_livre_config = project_config.get("market_sources", {}).get("mercado_livre", {})
    task_specs = [
        {"order": order, "query": query}
        for order, query in enumerate(MERCADO_LIVRE_DEFAULT_QUERIES)
    ]
    if not task_specs:
        return []

    worker_count = min(
        max(1, int(mercado_livre_config.get("parallel_workers", 1))),
        len(task_specs),
    )
    if worker_count <= 1:
        return [_run_mercado_livre_task(query=spec["query"]) for spec in task_specs]

    results_by_order: dict[int, dict[str, object]] = {}
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="mercado-livre",
    ) as executor:
        future_to_spec = {
            executor.submit(_run_mercado_livre_task, query=spec["query"]): spec
            for spec in task_specs
        }
        for future in as_completed(future_to_spec):
            spec = future_to_spec[future]
            try:
                results_by_order[spec["order"]] = future.result()
            except Exception as error:  # noqa: BLE001 - preserve task order for diagnostics.
                results_by_order[spec["order"]] = {
                    "pipeline_run_id": None,
                    "source_run_id": None,
                    "query": spec["query"],
                    "status": "failed",
                    "extracted": 0,
                    "loaded": 0,
                    "skipped": 0,
                    "pages": 0,
                    "price_summaries": 0,
                    "catalog_products_loaded": 0,
                    "catalog_products_skipped": 0,
                    "error": str(error),
                }

    return [results_by_order[index] for index in range(len(task_specs))]


def _run_bronze_cycle() -> dict[str, object]:
    repair_summary = _repair_stale_runs()
    project_config = get_settings().load_project_config()
    market_web_config = project_config.get("market_sources", {}).get("market_web", {})
    enabled_market_sources = _ordered_enabled_market_sources(market_web_config)

    market_web_result = ingest_market_web(
        sources=enabled_market_sources,
        queries=MARKET_WEB_DEFAULT_QUERIES,
        max_results=BRONZE_MARKET_WEB_MAX_RESULTS,
        triggered_by=_BRONZE_TRIGGERED_BY,
    )

    # 2026-05-02: fan out stable Buscape/Zoom tasks after market_web so the
    # stage order stays intact while idle browser time is converted into throughput.
    price_history_results = _run_parallel_price_history(project_config)

    # 2026-05-02: the ML API path is less anti-bot-sensitive, so query fan-out can
    # run wider than browser scrapers without duplicating the full Bronze cycle.
    mercado_livre_results = _run_parallel_mercado_livre(project_config)

    notify_exit_code = notify_discord_main()

    return {
        "repair_summary": repair_summary,
        "market_web": {
            "pipeline_run_id": str(market_web_result.pipeline_run_id),
            "source_run_ids": [str(source_run_id) for source_run_id in market_web_result.source_run_ids],
            "status": market_web_result.status,
            "extracted": market_web_result.extracted,
            "loaded": market_web_result.loaded,
            "skipped": market_web_result.skipped,
            "blocked": market_web_result.blocked,
            "results": market_web_result.results,
        },
        "price_history": {
            "runs": len(price_history_results),
            "success": sum(1 for item in price_history_results if item["status"] == "success"),
            "partial": sum(1 for item in price_history_results if item["status"] == "partial"),
            "failed": sum(1 for item in price_history_results if item["status"] == "failed"),
            "results": price_history_results,
        },
        "mercado_livre": {
            "runs": len(mercado_livre_results),
            "success": sum(1 for item in mercado_livre_results if item["status"] == "success"),
            "partial": sum(1 for item in mercado_livre_results if item["status"] == "partial"),
            "failed": sum(1 for item in mercado_livre_results if item["status"] == "failed"),
            "results": mercado_livre_results,
        },
        "notify_discord_exit_code": notify_exit_code,
    }


def _reconcile_async_bronze_lock(active_runs: list[dict[str, object]]) -> dict[str, object]:
    global _BRONZE_CYCLE_THREAD
    global _BRONZE_CYCLE_STARTED_AT

    if not _BRONZE_CYCLE_LOCK.locked():
        return {"released": False, "reason": "lock_not_held"}

    thread_alive = _BRONZE_CYCLE_THREAD is not None and _BRONZE_CYCLE_THREAD.is_alive()
    if active_runs:
        return {"released": False, "reason": "database_run_active", "thread_alive": thread_alive}

    now = datetime.now(timezone.utc)
    elapsed = None
    if _BRONZE_CYCLE_STARTED_AT is not None:
        elapsed = now - _BRONZE_CYCLE_STARTED_AT

    # 2026-05-02: self-heal orphaned in-memory Bronze locks when no Bronze
    # pipeline exists in control.pipeline_runs, which otherwise blocks the next
    # hourly trigger without any observable run to monitor or cancel.
    if thread_alive and (elapsed is None or elapsed < _BRONZE_LOCK_STALE_AFTER):
        return {
            "released": False,
            "reason": "thread_still_alive_without_db_run",
            "thread_alive": True,
            "elapsed_seconds": int(elapsed.total_seconds()) if elapsed else None,
        }

    _BRONZE_CYCLE_LOCK.release()
    _BRONZE_CYCLE_THREAD = None
    _BRONZE_CYCLE_STARTED_AT = None
    return {
        "released": True,
        "reason": "orphaned_async_lock_released",
        "thread_alive": thread_alive,
        "elapsed_seconds": int(elapsed.total_seconds()) if elapsed else None,
    }


@router.get("/recent-runs")
def recent_runs(
    hours: int = Query(default=24, ge=1, le=168),
    limit: int = Query(default=12, ge=1, le=100),
) -> dict[str, object]:
    # 2026-05-02: expose local run diagnostics over HTTP because Codex automation
    # cannot always reach the Docker daemon even when the API and Postgres are healthy.
    pipeline_sql = text(
        """
        SELECT
            id,
            pipeline_name,
            status,
            created_at,
            started_at,
            finished_at,
            metadata,
            error_message
        FROM control.pipeline_runs
        WHERE created_at >= now() - make_interval(hours => :hours)
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    source_sql = text(
        """
        SELECT
            pipeline_run_id,
            source_name,
            status,
            records_extracted,
            records_loaded,
            records_skipped,
            error_message
        FROM control.source_runs
        WHERE pipeline_run_id IN :pipeline_run_ids
        ORDER BY started_at DESC NULLS LAST, source_name
        """
    ).bindparams(bindparam("pipeline_run_ids", expanding=True))
    source_health_sql = text(
        """
        WITH ranked AS (
            SELECT
                source_name,
                started_at,
                status,
                COALESCE(records_extracted, 0) AS records_extracted,
                COALESCE(records_loaded, 0) AS records_loaded,
                COALESCE(records_skipped, 0) AS records_skipped,
                error_message,
                row_number() OVER (
                    PARTITION BY source_name
                    ORDER BY started_at DESC NULLS LAST, created_at DESC
                ) AS rn
            FROM control.source_runs
        )
        SELECT
            source_name,
            started_at,
            status,
            records_extracted,
            records_loaded,
            records_skipped,
            error_message,
            rn
        FROM ranked
        WHERE rn <= 10
        ORDER BY source_name, rn
        """
    )

    try:
        with engine.connect() as connection:
            pipeline_rows = connection.execute(
                pipeline_sql,
                {"hours": hours, "limit": limit},
            ).mappings().all()

            pipeline_run_ids = [row["id"] for row in pipeline_rows]
            source_rows = []
            if pipeline_run_ids:
                source_rows = connection.execute(
                    source_sql,
                    {"pipeline_run_ids": pipeline_run_ids},
                ).mappings().all()

            source_health_rows = connection.execute(source_health_sql).mappings().all()
    except Exception as exc:
        return {
            "lookback_hours": hours,
            "pipeline_count": 0,
            "pipelines": [],
            "source_health": [],
            "error": str(exc),
            "error_type": exc.__class__.__name__,
            "traceback": traceback.format_exc().splitlines()[-8:],
        }

    sources_by_pipeline: dict[object, list[dict[str, object]]] = {}
    for row in source_rows:
        pipeline_id = row["pipeline_run_id"]
        item = {
            "source_name": row["source_name"],
            "status": row["status"],
            "records_extracted": row["records_extracted"] or 0,
            "records_loaded": row["records_loaded"] or 0,
            "records_skipped": row["records_skipped"] or 0,
            "error_message": row["error_message"],
        }
        sources_by_pipeline.setdefault(pipeline_id, []).append(item)

    source_health_by_name: dict[str, list[dict[str, object]]] = {}
    for row in source_health_rows:
        source_health_by_name.setdefault(row["source_name"], []).append(dict(row))

    source_health = []
    terminal_statuses = {"success", "partial", "failed", "cancelled"}
    for source_name, rows in source_health_by_name.items():
        bad_streak = 0
        latest_status = None
        latest_error_message = None
        for index, row in enumerate(rows):
            status = row["status"]
            records_extracted = int(row["records_extracted"] or 0)
            records_loaded = int(row["records_loaded"] or 0)
            records_skipped = int(row["records_skipped"] or 0)
            if index == 0:
                latest_status = status
                latest_error_message = row["error_message"]
            if status not in terminal_statuses:
                continue
            # 2026-05-02: count zero-load runs as bad only when they also have no useful extraction/skip signal.
            has_useful_signal = (
                records_loaded > 0
                or records_extracted > 0
                or records_skipped > 0
            ) and not row["error_message"]
            is_bad = status in {"failed", "cancelled"} or not has_useful_signal
            if is_bad:
                bad_streak += 1
                continue
            break

        source_health.append(
            {
                "source_name": source_name,
                "bad_streak": bad_streak,
                "latest_status": latest_status,
                "latest_error_message": latest_error_message,
                "temporarily_disable_recommended": bad_streak >= 3,
            }
        )

    pipelines = []
    for row in pipeline_rows:
        pipeline_id = row["id"]
        pipelines.append(
            {
                "id": pipeline_id,
                "pipeline_name": row["pipeline_name"],
                "status": row["status"],
                "created_at": row["created_at"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "records_extracted": sum(
                    item["records_extracted"] for item in sources_by_pipeline.get(pipeline_id, [])
                ),
                "records_loaded": sum(
                    item["records_loaded"] for item in sources_by_pipeline.get(pipeline_id, [])
                ),
                "records_skipped": sum(
                    item["records_skipped"] for item in sources_by_pipeline.get(pipeline_id, [])
                ),
                "metadata": row["metadata"] or {},
                "error_message": row["error_message"],
                "sources": sources_by_pipeline.get(pipeline_id, []),
            }
        )

    return {
        "lookback_hours": hours,
        "pipeline_count": len(pipelines),
        "pipelines": pipelines,
        "source_health": source_health,
    }


@router.post("/repair-stale-runs")
def repair_stale_runs(
    stale_after_hours: int = Query(default=8, ge=2, le=72),
) -> dict[str, object]:
    return _repair_stale_runs(stale_after_hours=stale_after_hours)


@router.post("/cancel-pipeline-run")
def cancel_pipeline_run(
    pipeline_run_id: str = Query(..., min_length=36, max_length=36),
    reason: str = Query(
        default="Cancelled by /ops after orphaned Bronze execution was detected.",
        min_length=8,
        max_length=300,
    ),
) -> dict[str, object]:
    # 2026-05-02: allow operators to clear orphaned running rows after a reload
    # so the next scheduled Bronze cycle is not blocked for hours.
    return _cancel_pipeline_run(pipeline_run_id=pipeline_run_id, reason=reason)


@router.post("/bronze-cycle")
def bronze_cycle(async_run: bool = Query(default=True)) -> dict[str, object]:
    # 2026-05-02: default to async trigger because the full Bronze order can run
    # for longer than common HTTP client timeouts in the Codex automation host.
    repair_summary = _repair_stale_runs()
    active_runs = _find_active_bronze_runs()
    if active_runs:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "A Bronze cycle is already running in control.pipeline_runs.",
                "active_runs": active_runs,
                "repair_summary": repair_summary,
            },
        )

    if not async_run:
        return _run_bronze_cycle()

    reconcile_summary = _reconcile_async_bronze_lock(active_runs)
    acquired = _BRONZE_CYCLE_LOCK.acquire(blocking=False)
    if not acquired:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "A Bronze cycle is already running via /ops/bronze-cycle.",
                "active_runs": active_runs,
                "repair_summary": repair_summary,
                "reconcile_summary": reconcile_summary,
            },
        )

    global _BRONZE_CYCLE_THREAD
    global _BRONZE_CYCLE_STARTED_AT
    _BRONZE_CYCLE_STARTED_AT = datetime.now(timezone.utc)

    def runner() -> None:
        global _BRONZE_CYCLE_THREAD
        global _BRONZE_CYCLE_STARTED_AT
        try:
            _run_bronze_cycle()
        finally:
            _BRONZE_CYCLE_THREAD = None
            _BRONZE_CYCLE_STARTED_AT = None
            _BRONZE_CYCLE_LOCK.release()

    _BRONZE_CYCLE_THREAD = Thread(
        target=runner,
        name="bronze-cycle-runner",
        daemon=True,
    )
    _BRONZE_CYCLE_THREAD.start()
    return {
        "accepted": True,
        "mode": "async",
        "message": "Bronze cycle started in background. Poll /ops/recent-runs for progress.",
        "reconcile_summary": reconcile_summary,
    }
