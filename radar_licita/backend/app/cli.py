import argparse
import asyncio
from datetime import date, timedelta

from app.core.time import local_today
from app.db import SessionLocal
from app.documents.service import process_pending_documents
from app.ingestion.service import run_sync


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="python -m app.cli", description="Radar Licita operations")
    commands = root.add_subparsers(dest="command", required=True)
    sync = commands.add_parser("sync", help="Synchronize public PNCP opportunities")
    sync.add_argument("--mode", choices=["open", "publication", "updates"], default="open")
    sync.add_argument("--start", type=date.fromisoformat)
    sync.add_argument("--end", type=date.fromisoformat, default=local_today())
    sync.add_argument("--pages", type=int, default=2)
    sync.add_argument("--without-details", action="store_true")
    docs = commands.add_parser("documents", help="Process pending PNCP documents")
    docs.add_argument("--procurement-id", type=int)
    docs.add_argument("--limit", type=int, default=20)
    return root


async def run() -> None:
    args = parser().parse_args()
    db = SessionLocal()
    try:
        if args.command == "sync":
            job = await run_sync(
                db,
                mode=args.mode,
                start_date=args.start or args.end - timedelta(days=3),
                end_date=args.end,
                modality_ids=[4, 6, 8, 12, 16, 18],
                max_pages=args.pages,
                hydrate_details=not args.without_details,
            )
            print(
                {
                    "job_id": job.id,
                    "status": job.status,
                    "records_seen": job.records_seen,
                    "created": job.records_created,
                    "updated": job.records_updated,
                    "errors": job.error_count,
                }
            )
        else:
            print(await process_pending_documents(db, args.procurement_id, args.limit))
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(run())

