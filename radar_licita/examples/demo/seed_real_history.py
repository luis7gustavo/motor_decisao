"""Build a new demo database exclusively from the verified public PNCP capture."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"
APPROVED = (BACKEND / "data" / "demo").resolve()
sys.path.insert(0, str(BACKEND))
sys.path.insert(0, str(ROOT))


def build(destination: Path) -> dict:
    destination = destination.expanduser().resolve()
    if APPROVED not in destination.parents or destination.suffix.lower() != ".db":
        raise ValueError(f"Destino fora da pasta isolada: {destination}")
    if destination.exists():
        raise FileExistsError(f"Snapshot existente preservado: {destination}")
    temporary = APPROVED / f".real-history-{uuid4().hex}.db"
    if temporary.exists():
        raise FileExistsError(temporary)
    env = os.environ.copy()
    env.update(DATABASE_URL=f"sqlite:///{temporary.as_posix()}", APP_ENV="demo", AUTO_START_SCHEDULER="false")
    database_engine = None
    try:
        os.environ.update(DATABASE_URL=env["DATABASE_URL"], APP_ENV="demo", AUTO_START_SCHEDULER="false")
        from app.technology.historical import FIXTURE, historical_view, load_historical_snapshot
        snapshot = load_historical_snapshot()
        historical_view(snapshot, mode="snapshot")
        subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                       cwd=BACKEND, env=env, check=True)
        from sqlalchemy import select
        from app.db import SessionLocal, engine as database_engine
        from app.ingestion.service import upsert_items, upsert_procurement
        from app.models import PartnerSearchRun, ProcurementItem
        from app.partners.service import _candidate

        with SessionLocal() as db:
            for record in snapshot["records"]:
                purchase = record["purchase"]
                procurement, _, _ = upsert_procurement(db, purchase)
                upsert_items(db, procurement, [entry["item"] for entry in record["items"]])
                db.flush()
                for entry in record["items"]:
                    item_payload = entry["item"]
                    number = int(item_payload["numeroItem"])
                    item = db.scalar(select(ProcurementItem).where(
                        ProcurementItem.procurement_id == procurement.id,
                        ProcurementItem.item_number == number))
                    search = PartnerSearchRun(
                        procurement_item_id=item.id,
                        query=procurement.official_control_number,
                        max_results=len(entry["results"]),
                        status="success", complete=False, request_count=0,
                        source="pncp_capture", source_version="historical-2026-10-15",
                        consulted_window={"captured_at": snapshot["captured_at"]},
                        limitations=["Recorte histórico selecionado; não é busca exaustiva nem disponibilidade atual."],
                        parameters_hash=hashlib.sha256(
                            f"{procurement.official_control_number}:{number}:{entry['item_hash']}".encode()
                        ).hexdigest(),
                        raw_json={"item_hash": entry["item_hash"], "result_hashes": entry["result_hashes"],
                                  "fixture": str(FIXTURE.relative_to(ROOT))},
                    )
                    db.add(search)
                    db.flush()
                    for result in entry["results"]:
                        _candidate(db, {"cnpj": procurement.organization_cnpj, "year": procurement.year,
                                        "sequence": procurement.sequence, "item": item_payload, "result": result},
                                   item, search, procurement.official_control_number)
            db.commit()
        database_engine.dispose()
        from scripts.demo.check_demo_db import check  # type: ignore[import-not-found]
        # The validation script is intentionally independent of the app database engine.
        checked = check(temporary)
        temporary.replace(destination)
        checked["database"] = str(destination)
        checked["fixture_sha256"] = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
        return checked
    except Exception:
        if database_engine is not None:
            database_engine.dispose()
        if temporary.exists():
            temporary.unlink()
        raise


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) == 2 else APPROVED / "radar_real_history_snapshot.db"
    print(json.dumps(build(target), ensure_ascii=False))
