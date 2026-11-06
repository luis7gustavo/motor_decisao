"""Real-copy integration checks; run only with RADAR_PREVIEW_DATABASE_URL set."""

import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from app.api.radar_c2_preview import router
from app import radar_c2_reader as reader


def client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_unconfigured_copy_is_explicitly_unavailable(monkeypatch):
    monkeypatch.delenv("RADAR_PREVIEW_DATABASE_URL", raising=False)
    reader._engine.cache_clear()
    response = client().get("/radar-demo")
    assert response.status_code == 503
    assert "Cópia isolada indisponível" in response.text


def test_operational_destinations_are_refused(monkeypatch):
    monkeypatch.setenv("RADAR_PREVIEW_DATABASE_URL",
                       "postgresql+psycopg://radar_reader:x@127.0.0.1:55432/test_20261020_ba894ce71405")
    reader._engine.cache_clear()
    assert client().get("/radar-demo").status_code == 503


@pytest.mark.skipif(not os.environ.get("RADAR_PREVIEW_DATABASE_URL"), reason="isolated copy not configured")
def test_real_search_filters_pagination_detail_and_unknown_states():
    reader._engine.cache_clear()
    c = client()
    home = c.get("/radar-demo")
    assert home.status_code == 200
    assert "Buscar licitações registradas" in home.text
    first = c.get("/radar-demo?uf=SP&page=1")
    second = c.get("/radar-demo?uf=SP&page=2")
    assert first.status_code == second.status_code == 200
    assert "Cobertura parcial" in first.text
    assert "Frescor da fonte: desconhecido" in first.text
    a, more = reader.search(uf="SP", page=1)
    b, _ = reader.search(uf="SP", page=2)
    assert len(a) == len(b) == reader.PAGE_SIZE and more
    assert not set(r["source_id"] for r in a) & set(r["source_id"] for r in b)
    assert all(r["uf"] == "SP" and r["opening_state"] == "unknown"
               and r["freshness"] == "unknown" for r in a + b)
    modality_rows, _ = reader.search(uf="SP", modality=a[0]["modality"])
    assert modality_rows and all(r["uf"] == "SP" and r["modality"] == a[0]["modality"]
                                 for r in modality_rows)
    detail_url = f'/radar-demo/{a[0]["source_id"]}'
    assert c.get(detail_url).status_code == 200
    assert "Prazo bruto do PNCP" in c.get(detail_url).text
    assert c.get("/radar-demo?uf=XX&q=zzzzsemresultado").status_code == 200
    assert "Nenhum registro" in c.get("/radar-demo?uf=XX&q=zzzzsemresultado").text
    assert c.get("/radar-demo/identidade-inexistente").status_code == 404
    assert c.get("/radar-demo?page=501").status_code == 422


@pytest.mark.skipif(not os.environ.get("RADAR_PREVIEW_DATABASE_URL"), reason="isolated copy not configured")
def test_real_text_state_and_quarantined_retification():
    reader._engine.cache_clear()
    rows, _ = reader.search(q="papel", uf="SP")
    assert rows and all("papel" in r["object"].casefold() and r["uf"] == "SP" for r in rows)
    revoked, _ = reader.search(state="Revogada")
    assert revoked and all(r["official_state_raw"] == "Revogada"
                           and r["opening_state"] == "unknown" for r in revoked)
    annulled, _ = reader.search(state="Anulada")
    assert annulled and all(r["official_state_raw"] == "Anulada"
                             and r["opening_state"] == "unknown" for r in annulled)
    # Known lagging pointer in this frozen backup: version 2 is persisted,
    # but version 1 is still referenced as current. Never show version 1.
    drift_id = "45780079000159-1-000131/2025"
    assert reader.detail(drift_id)[0] is None
    assert client().get(f"/radar-demo/{drift_id}").status_code == 404


@pytest.mark.skipif(not os.environ.get("RADAR_PREVIEW_RETIFICATION_ID"),
                    reason="temporary isolated retification not enabled")
def test_retification_becomes_current_without_resurrection():
    reader._engine.cache_clear()
    source_id = os.environ["RADAR_PREVIEW_RETIFICATION_ID"]
    row, versions = reader.detail(source_id)
    assert row["version"] == 2
    assert versions[0]["version_no"] == 2 and versions[1]["version_no"] == 1
    assert row["official_state_raw"] == "Revogada"
    assert row["opening_state"] == "unknown"
    response = client().get(f"/radar-demo/{source_id}")
    assert response.status_code == 200
    assert "versão vigente no snapshot: <strong>2</strong>" in response.text
    assert "Revogada" in response.text


@pytest.mark.skipif(not os.environ.get("RADAR_PREVIEW_DATABASE_URL"), reason="isolated copy not configured")
def test_reader_role_cannot_write_and_db_failure_is_visible(monkeypatch):
    reader._engine.cache_clear()
    rights = reader._read("SELECT current_user AS who, "
                          "has_table_privilege(current_user, "
                          "'radar_preview.discovery_current', 'INSERT') AS can_insert, "
                          "current_setting('default_transaction_read_only') AS read_only", {})[0]
    assert rights["who"] == "radar_reader"
    assert rights["can_insert"] is False
    assert rights["read_only"] == "on"
    configured = os.environ["RADAR_PREVIEW_DATABASE_URL"]
    faulty = configured.replace(":5432/", ":5433/").replace(":55434/", ":55435/")
    monkeypatch.setattr(reader, "_engine", lambda: create_engine(
        faulty, connect_args={"connect_timeout": 1}))
    assert client().get("/radar-demo").status_code == 503
