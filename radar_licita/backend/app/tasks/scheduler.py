import asyncio
from datetime import timedelta

import structlog
from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import settings
from app.core.time import local_today
from app.db import SessionLocal
from app.ingestion.service import run_sync

log = structlog.get_logger()
scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")


def scheduled_sync() -> None:
    db = SessionLocal()
    end = local_today()
    try:
        asyncio.run(
            run_sync(
                db,
                mode="updates",
                start_date=end - timedelta(days=settings.pncp_sync_lookback_days),
                end_date=end,
                modality_ids=[4, 6, 8, 12, 16, 18],
                max_pages=None,
                hydrate_details=True,
            )
        )
    except Exception as exc:
        log.exception("scheduled_sync_failed", error=str(exc))
    finally:
        db.close()


def start_scheduler() -> None:
    if scheduler.running:
        return
    scheduler.add_job(
        scheduled_sync,
        "interval",
        minutes=settings.sync_interval_minutes,
        id="pncp_incremental_sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()


def stop_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)

