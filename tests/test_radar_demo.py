from datetime import date, datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import radar_demo as demo_api
from app.radar_demo import Catalog, grouped_versions, is_fresh, load_catalog, opening_state, search


def test_retification_is_current_and_repeated_payload_does_not_duplicate():
    catalog = load_catalog()
    groups = grouped_versions(catalog)
    assert len(groups["SINTETICO-001"]) == 2
    assert groups["SINTETICO-001"][-1]["version"] == 2
    assert groups["SINTETICO-001"][-1]["deadline"] == "2026-11-25"
    rows, total = search(catalog, today=date(2026, 11, 4), q="papel")
    assert total == 2  # Two distinct official-like IDs with the same initial title.
    assert [row["source_id"] for row in rows] == ["SINTETICO-002", "SINTETICO-001"]
    assert rows[-1]["object"] == "Papel para impressão e envelopes"


def test_filters_and_unknown_opening_are_conservative():
    catalog = load_catalog()
    rows, total = search(
        catalog, today=date(2026, 11, 4), uf="SP", category="material de escritório",
        state="open", deadline_from=date(2026, 11, 20), deadline_to=date(2026, 11, 30),
    )
    assert total == 1
    assert rows[0]["source_id"] == "SINTETICO-001"
    unknown = grouped_versions(catalog)["SINTETICO-003"][-1]
    assert opening_state(unknown, date(2026, 11, 4)) == "unknown"
    assert opening_state(rows[0], date(2026, 11, 26)) == "closed"


def test_pagination_and_stale_snapshot():
    catalog = load_catalog()
    rows, total = search(catalog, today=date(2026, 11, 4), page=2)
    assert total == 3
    assert rows == []
    assert is_fresh(catalog, catalog.observed_at + timedelta(hours=25)) is False
    assert is_fresh(catalog, catalog.observed_at + timedelta(hours=1)) is True
    assert is_fresh(Catalog(datetime.now(timezone.utc) + timedelta(hours=1), ()), datetime.now(timezone.utc)) is False
    expanded = Catalog(catalog.observed_at, tuple(
        {**catalog.records[0], "source_id": f"SINTETICO-{n:03d}"} for n in range(12)
    ))
    first, first_total = search(expanded, today=date(2026, 11, 4), page=1)
    second, second_total = search(expanded, today=date(2026, 11, 4), page=2)
    assert first_total == second_total == 12
    assert len(first) == 10 and len(second) == 2


def test_pages_render_source_versions_empty_state_and_unavailable(monkeypatch):
    app = FastAPI()
    app.include_router(demo_api.router)
    client = TestClient(app)
    response = client.get("/radar-demo/sintetico?uf=SP")
    assert response.status_code == 200
    assert "SINTETICO-001" in response.text
    assert "SINTETICO-002" not in response.text
    assert "dados sintéticos" in response.text
    assert "Alertas locais de alteração" in response.text
    assert "Retificação sintética" in client.get("/radar-demo/sintetico/alteracoes").text
    detail = client.get("/radar-demo/sintetico/SINTETICO-001")
    assert detail.status_code == 200
    assert "Versão 1" in detail.text and "versão atual 2" in detail.text
    assert "Nenhum registro" in client.get("/radar-demo/sintetico?uf=BA").text
    assert client.get("/radar-demo/sintetico/inexistente").status_code == 404

    original_catalog = load_catalog()
    monkeypatch.setattr(demo_api, "load_catalog", lambda: Catalog(
        original_catalog.observed_at, original_catalog.records, source_available=False,
    ))
    offline = client.get("/radar-demo/sintetico")
    assert offline.status_code == 200
    assert "SINTETICO-001" in offline.text
    assert "Fonte sintética indisponível" in offline.text
    assert "Abertura não confirmada" in offline.text

    def unavailable():
        raise OSError("fixture missing")

    monkeypatch.setattr(demo_api, "load_catalog", unavailable)
    response = client.get("/radar-demo/sintetico")
    assert response.status_code == 503
    assert "Fixture indisponível" in response.text
