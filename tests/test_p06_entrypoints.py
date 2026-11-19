"""P06 regression tests for MegaMix import entry points, without a database."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import decision_engine
from scripts import collect_all


def test_api_requires_source_date_before_importing_megamix() -> None:
    app = FastAPI()
    app.include_router(decision_engine.router)
    with TestClient(app) as client, patch.object(
        decision_engine, "import_megamix_catalog"
    ) as importer, patch.object(decision_engine, "build_decision_opportunities") as builder:
        response = client.post("/decision-engine/run?import_megamix=true&import_coletek=false")
    assert response.status_code == 422
    assert "megamix_snapshot_date" in response.json()["detail"]
    importer.assert_not_called()
    builder.assert_not_called()


def test_api_passes_explicit_source_date_and_skips_default_import() -> None:
    app = FastAPI()
    app.include_router(decision_engine.router)
    with TestClient(app) as client, patch.object(
        decision_engine, "import_megamix_catalog",
        return_value=SimpleNamespace(loaded=2),
    ) as importer, patch.object(
        decision_engine, "build_decision_opportunities",
        return_value=SimpleNamespace(opportunities_scored=2),
    ):
        default_response = client.post("/decision-engine/run?import_coletek=false")
        dated_response = client.post(
            "/decision-engine/run?import_megamix=true&import_coletek=false"
            "&megamix_snapshot_date=2026-06-01"
        )
    assert default_response.status_code == 200
    assert default_response.json()["import_megamix"] is None
    assert dated_response.status_code == 200
    assert dated_response.json()["import_megamix"]["loaded"] == 2
    importer.assert_called_once()
    assert importer.call_args.kwargs["snapshot_date"] == date(2026, 6, 1)


def test_collect_all_only_imports_megamix_with_explicit_date() -> None:
    with patch.object(collect_all, "_run", return_value=True) as run:
        assert collect_all.stage_engine() is True
        assert run.call_args.args == (
            "Decision Engine", "build_decision_engine.py", "--import-coletek"
        )
        assert collect_all.stage_engine("2026-06-01") is True
        assert run.call_args.args == (
            "Decision Engine", "build_decision_engine.py", "--import-coletek",
            "--import-megamix", "--megamix-snapshot-date", "2026-06-01"
        )
