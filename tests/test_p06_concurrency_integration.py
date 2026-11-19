"""Run only against an isolated, disposable PostgreSQL database.

Example: RADAR_P06_INTEGRATION=1 RADAR_TEST_DB=test_... \
    python tests/test_p06_concurrency_integration.py
"""

from __future__ import annotations

import multiprocessing
import os
import time
import unittest
from datetime import date
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.core.database import engine
from scripts import import_megamix_catalog as importer


FIXTURE = Path(__file__).parent / "fixtures" / "megamix_snapshot_synthetic.json"


def _import_concurrently(
    barrier: multiprocessing.Barrier,
    results: multiprocessing.Queue,
    lock_acquired: multiprocessing.Event,
    release_lock: multiprocessing.Event,
    day: int,
) -> None:
    original_create = importer.create_pipeline_run

    def hold_snapshot_lock(*args, **kwargs):
        # This callback runs after the transaction acquired the advisory lock.
        # Hold it until the parent observes the other process waiting in PG.
        lock_acquired.set()
        if not release_lock.wait(timeout=20):
            raise TimeoutError("parent did not release the snapshot lock")
        return original_create(*args, **kwargs)

    importer.create_pipeline_run = hold_snapshot_lock
    barrier.wait(timeout=15)
    started = time.monotonic()
    try:
        result = importer.import_megamix_catalog(
            path=FIXTURE, snapshot_date=date(2026, 6, day)
        )
        results.put(("loaded", day, result.loaded, started, time.monotonic()))
    except ValueError as exc:
        results.put(("rejected", day, str(exc), started, time.monotonic()))
    except Exception as exc:
        results.put(("unexpected", day, repr(exc), started, time.monotonic()))


@unittest.skipUnless(os.getenv("RADAR_P06_INTEGRATION") == "1", "disposable DB required")
class MegaMixConcurrencyIntegrationTest(unittest.TestCase):
    def test_same_bytes_with_competing_dates_create_only_one_snapshot(self) -> None:
        url = make_url(os.environ["DATABASE_URL"])
        self.assertEqual(url.database, os.environ["RADAR_TEST_DB"])
        self.assertTrue(url.database.startswith("test_"))
        self.assertTrue(url.host.startswith("radar-p06-pg-"))

        context = multiprocessing.get_context("fork")
        barrier = context.Barrier(2)
        results = context.Queue()
        lock_acquired = context.Event()
        release_lock = context.Event()
        processes = [
            context.Process(
                target=_import_concurrently,
                args=(barrier, results, lock_acquired, release_lock, day),
            )
            for day in (1, 2)
        ]
        try:
            for process in processes:
                process.start()
            self.assertTrue(lock_acquired.wait(timeout=15), "no process acquired the lock")
            deadline = time.monotonic() + 15
            waiting_on_lock = False
            while time.monotonic() < deadline:
                with engine.connect() as connection:
                    waiting_on_lock = connection.execute(
                        text(
                            """SELECT EXISTS (
                                SELECT 1 FROM pg_stat_activity
                                WHERE datname = current_database()
                                  AND wait_event_type = 'Lock'
                                  AND wait_event = 'advisory'
                                  AND query LIKE '%pg_advisory_xact_lock%'
                            )"""
                        )
                    ).scalar_one()
                if waiting_on_lock:
                    break
                time.sleep(0.05)
            self.assertTrue(waiting_on_lock, "second process never waited on the advisory lock")
            release_lock.set()
            for process in processes:
                process.join(timeout=20)
            self.assertTrue(all(process.exitcode == 0 for process in processes))
            observed = [results.get(timeout=2) for _ in processes]
            self.assertEqual({row[0] for row in observed}, {"loaded", "rejected"}, observed)
            self.assertEqual(next(row[2] for row in observed if row[0] == "loaded"), 2)
            self.assertIn("another snapshot date", next(row[2] for row in observed if row[0] == "rejected"))
            self.assertLess(max(row[3] for row in observed), min(row[4] for row in observed), observed)

            with engine.connect() as connection:
                row_count = connection.execute(text("SELECT count(*) FROM bronze.supplier_products_raw WHERE supplier_slug = 'megamix'")).scalar_one()
                source_runs = connection.execute(text("SELECT count(*) FROM control.source_runs WHERE source_name = 'megamix'")).scalar_one()
            self.assertEqual(row_count, 2)
            self.assertEqual(source_runs, 1)
        finally:
            release_lock.set()
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
