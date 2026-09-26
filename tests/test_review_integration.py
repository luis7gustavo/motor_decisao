from __future__ import annotations

import json
import re
from decimal import Decimal
from urllib.parse import urlsplit
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.core.database import engine
from app.main import app
from app.repositories.review_repository import ReviewConflictError, ReviewRepository
from app.schemas.review import ReviewDecisionInput, ReviewFilters
from app.services.review_service import ReviewService


PIPELINE_ID = UUID("10000000-0000-0000-0000-000000000001")
RUN_ID = UUID("20000000-0000-0000-0000-000000000001")
OPPORTUNITY_HIGH = UUID("30000000-0000-0000-0000-000000000001")
OPPORTUNITY_LOW = UUID("30000000-0000-0000-0000-000000000002")
OPPORTUNITY_IGNORED = UUID("30000000-0000-0000-0000-000000000003")


def _is_test_database() -> bool:
    return "test" in (engine.url.database or "").lower()


pytestmark = pytest.mark.skipif(
    not _is_test_database(),
    reason="Review integration tests require a dedicated database whose name contains 'test'.",
)


@pytest.fixture(autouse=True)
def clean_test_database() -> None:
    if not _is_test_database():
        yield
        return
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                TRUNCATE TABLE
                    feedback.opportunity_reviews,
                    gold.ml_model_runs,
                    gold.decision_engine_runs,
                    bronze.supplier_products_raw,
                    control.pipeline_runs
                CASCADE
                """
            )
        )
    yield


@pytest.fixture
def service() -> ReviewService:
    return ReviewService(ReviewRepository(engine))


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client


def _seed_opportunities() -> None:
    products = [
        {
            "opportunity_id": OPPORTUNITY_HIGH,
            "supplier_raw_id": UUID("40000000-0000-0000-0000-000000000001"),
            "supplier_product_id": UUID("50000000-0000-0000-0000-000000000001"),
            "snapshot_id": UUID("60000000-0000-0000-0000-000000000001"),
            "supplier": "mirao",
            "title": "SSD Kingston NV2 1TB Preto 220V",
            "price": Decimal("100.00"),
            "market_price": Decimal("200.00"),
            "profit": Decimal("30.00"),
            "margin": Decimal("0.1500"),
            "match": Decimal("82.00"),
            "score": Decimal("90.00"),
            "recommendation": "revisar",
            "risk_flags": ["match_revisar", "modelo_nao_confirmado"],
            "payload_hash": "1" * 64,
            "evidence": {
                "top_matches": [
                    {
                        "source_kind": "market_web",
                        "source_name": "kabum",
                        "title": "SSD Kingston NV2 1TB Preto 110V",
                        "price": 200.0,
                        "match_confidence": 82.0,
                        "position": 1,
                        "reviews_count": 42,
                        "sold_quantity": 8,
                        "item_url": "https://example.com/market-ssd",
                        "fetched_date": "2026-08-17",
                    }
                ]
            },
        },
        {
            "opportunity_id": OPPORTUNITY_LOW,
            "supplier_raw_id": UUID("40000000-0000-0000-0000-000000000002"),
            "supplier_product_id": UUID("50000000-0000-0000-0000-000000000002"),
            "snapshot_id": UUID("60000000-0000-0000-0000-000000000002"),
            "supplier": "coletek",
            "title": "Mouse Gamer Orion RGB",
            "price": Decimal("50.00"),
            "market_price": Decimal("100.00"),
            "profit": Decimal("25.00"),
            "margin": Decimal("0.2500"),
            "match": Decimal("72.00"),
            "score": Decimal("70.00"),
            "recommendation": "revisar",
            "risk_flags": ["demanda_incompleta"],
            "payload_hash": "2" * 64,
            "evidence": {"top_matches": []},
        },
        {
            "opportunity_id": OPPORTUNITY_IGNORED,
            "supplier_raw_id": UUID("40000000-0000-0000-0000-000000000003"),
            "supplier_product_id": UUID("50000000-0000-0000-0000-000000000003"),
            "snapshot_id": UUID("60000000-0000-0000-0000-000000000003"),
            "supplier": "mirao",
            "title": "Produto ignorado",
            "price": Decimal("10.00"),
            "market_price": Decimal("20.00"),
            "profit": Decimal("2.00"),
            "margin": Decimal("0.1000"),
            "match": Decimal("90.00"),
            "score": Decimal("99.00"),
            "recommendation": "ignorar",
            "risk_flags": [],
            "payload_hash": "3" * 64,
            "evidence": {"top_matches": []},
        },
    ]
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO control.pipeline_runs (id, pipeline_name, status)
                VALUES (:id, 'review_test', 'success')
                """
            ),
            {"id": PIPELINE_ID},
        )
        connection.execute(
            text(
                """
                INSERT INTO gold.decision_engine_runs (
                    id, pipeline_run_id, scoring_version, status, config_snapshot, finished_at
                ) VALUES (
                    :id, :pipeline_id, 'heuristic_test_v1', 'success',
                    CAST(:config AS jsonb), NOW()
                )
                """
            ),
            {
                "id": RUN_ID,
                "pipeline_id": PIPELINE_ID,
                "config": json.dumps(
                    {"margin": {"ml_fee_pct": 0.12, "tax_pct": 0.17, "shipping_pct": 0.03}}
                ),
            },
        )
        for product in products:
            connection.execute(
                text(
                    """
                    INSERT INTO bronze.supplier_products_raw (
                        id, supplier_slug, source_url, raw_title, raw_price, raw_stock,
                        payload, payload_hash, fetched_date, fetched_at
                    ) VALUES (
                        :supplier_raw_id, :supplier, :source_url, :title, :price, 4,
                        CAST(:payload AS jsonb), :payload_hash, CURRENT_DATE, NOW()
                    )
                    """
                ),
                {
                    **product,
                    "source_url": "https://example.com/supplier-product",
                    "payload": json.dumps({"image_url": "https://example.com/product.jpg"}),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO silver.supplier_products_normalized (
                        id, supplier_raw_id, supplier_slug, source_product_key, raw_title,
                        normalized_title, title_tokens, token_count, supplier_price,
                        source_url, fetched_date, fetched_at, payload
                    ) VALUES (
                        :supplier_product_id, :supplier_raw_id, :supplier,
                        CAST(:supplier_product_id AS text), :title, lower(:title),
                        ARRAY['produto'], 1, :price, 'https://example.com/supplier-product',
                        CURRENT_DATE, NOW(), '{}'::jsonb
                    )
                    """
                ),
                product,
            )
            connection.execute(
                text(
                    """
                    INSERT INTO gold.decision_opportunities (
                        id, decision_run_id, scoring_version, supplier_product_id,
                        supplier_raw_id, supplier_slug, product_title, normalized_title,
                        supplier_price, estimated_market_price, estimated_net_profit,
                        net_margin_pct, total_fee_pct, market_offer_count,
                        market_source_count, price_history_count, mercado_livre_count,
                        demand_score, match_confidence, decision_score, recommendation,
                        confidence_level, risk_flags, evidence
                    ) VALUES (
                        :opportunity_id, :run_id, 'heuristic_test_v1', :supplier_product_id,
                        :supplier_raw_id, :supplier, :title, lower(:title), :price,
                        :market_price, :profit, :margin, 0.3200, 1, 1, 0, 0,
                        65.00, :match, :score, :recommendation, 'media',
                        :risk_flags, CAST(:evidence_json AS jsonb)
                    )
                    """
                ),
                {
                    **product,
                    "run_id": RUN_ID,
                    "evidence_json": json.dumps(product["evidence"]),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO gold.decision_opportunity_snapshots (
                        id, decision_run_id, scoring_version, supplier_product_id,
                        supplier_raw_id, supplier_slug, product_title, normalized_title,
                        supplier_price, estimated_market_price, estimated_net_profit,
                        net_margin_pct, total_fee_pct, market_offer_count,
                        market_source_count, price_history_count, mercado_livre_count,
                        demand_score, match_confidence, decision_score, recommendation,
                        confidence_level, risk_flags, evidence
                    ) VALUES (
                        :snapshot_id, :run_id, 'heuristic_test_v1', :supplier_product_id,
                        :supplier_raw_id, :supplier, :title, lower(:title), :price,
                        :market_price, :profit, :margin, 0.3200, 1, 1, 0, 0,
                        65.00, :match, :score, :recommendation, 'media',
                        :risk_flags, CAST(:evidence_json AS jsonb)
                    )
                    """
                ),
                {
                    **product,
                    "run_id": RUN_ID,
                    "evidence_json": json.dumps(product["evidence"]),
                },
            )
        connection.execute(
            text(
                """
                INSERT INTO bronze.market_web_listings_raw (
                    source_name, source_role, query, position, title, price,
                    item_url, image_url, shipping_text, blocked, payload,
                    payload_hash, fetched_date, fetched_at
                ) VALUES (
                    'kabum', 'benchmark', 'ssd', 1,
                    'SSD Kingston NV2 1TB Preto 110V', 200.00,
                    'https://example.com/market-ssd', 'https://example.com/market.jpg',
                    'Frete calculado no checkout', FALSE, '{}'::jsonb,
                    :payload_hash, DATE '2026-08-17', NOW()
                )
                """
            ),
            {"payload_hash": "9" * 64},
        )


def _csrf_token(response_text: str) -> str:
    match = re.search(r'name="csrf_token" value="([^"]+)"', response_text)
    assert match is not None
    return match.group(1)


def _decision_payload(token: str, **overrides: str) -> dict[str, str]:
    payload = {
        "csrf_token": token,
        "decision": "approved_test_purchase",
        "reviewer": "Teste",
        "filter_sort": "score",
    }
    payload.update(overrides)
    return payload


def test_queue_contains_only_pending_review_items_in_score_order(service: ReviewService) -> None:
    _seed_opportunities()

    rows = service.pending(ReviewFilters())

    assert [row["id"] for row in rows] == [OPPORTUNITY_HIGH, OPPORTUNITY_LOW]
    assert OPPORTUNITY_IGNORED not in {row["id"] for row in rows}


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        (ReviewFilters(supplier="coletek"), [OPPORTUNITY_LOW]),
        (ReviewFilters(q="Kingston"), [OPPORTUNITY_HIGH]),
        (ReviewFilters(min_margin=Decimal("20")), [OPPORTUNITY_LOW]),
        (ReviewFilters(min_profit=Decimal("28")), [OPPORTUNITY_HIGH]),
        (ReviewFilters(min_score=Decimal("80")), [OPPORTUNITY_HIGH]),
        (ReviewFilters(min_match=Decimal("80")), [OPPORTUNITY_HIGH]),
        (ReviewFilters(risk="demanda_incompleta"), [OPPORTUNITY_LOW]),
        (ReviewFilters(sort="margin"), [OPPORTUNITY_LOW, OPPORTUNITY_HIGH]),
    ],
)
def test_queue_filters(filters: ReviewFilters, expected: list[UUID], service: ReviewService) -> None:
    _seed_opportunities()

    assert [row["id"] for row in service.pending(filters)] == expected


def test_page_loads_product_market_financial_and_matching_details(client: TestClient) -> None:
    _seed_opportunities()

    response = client.get("/review")

    assert response.status_code == 200
    assert "SSD Kingston NV2 1TB Preto 220V" in response.text
    assert "SSD Kingston NV2 1TB Preto 110V" in response.text
    assert "Voltagem" in response.text
    assert "Resumo financeiro" in response.text
    assert "Aprovar para compra teste" in response.text
    assert '<form method="post"' in response.text


def test_valid_approval_is_persisted_and_removed_from_queue(
    client: TestClient, service: ReviewService
) -> None:
    _seed_opportunities()
    page = client.get("/review")

    response = client.post(
        f"/review/{OPPORTUNITY_HIGH}/decision",
        data=_decision_payload(
            _csrf_token(page.text),
            max_purchase_price="105.50",
            notes="Compra controlada",
        ),
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert urlsplit(response.headers["location"]).path == "/review"
    assert [row["id"] for row in service.pending(ReviewFilters())] == [OPPORTUNITY_LOW]
    history = service.history()
    assert history[0]["human_decision"] == "approved_test_purchase"
    assert history[0]["max_purchase_price"] == Decimal("105.50")
    assert history[0]["original_heuristic_recommendation"] == "revisar"
    assert history[0]["original_heuristic_score"] == Decimal("90.0000")


def test_reanalysis_with_reason_and_rejection_with_reason_are_valid(
    client: TestClient, service: ReviewService
) -> None:
    _seed_opportunities()
    first = client.get("/review")
    reanalysis = client.post(
        f"/review/{OPPORTUNITY_HIGH}/decision",
        data=_decision_payload(
            _csrf_token(first.text),
            decision="needs_reanalysis",
            reason_code="find_more_evidence",
        ),
        follow_redirects=False,
    )
    second = client.get(f"/review/{OPPORTUNITY_LOW}")
    rejection = client.post(
        f"/review/{OPPORTUNITY_LOW}/decision",
        data=_decision_payload(
            _csrf_token(second.text),
            decision="rejected",
            reason_code="insufficient_margin",
        ),
        follow_redirects=False,
    )

    assert reanalysis.status_code == 303
    assert rejection.status_code == 303
    assert service.summary() == {
        "pending": 0,
        "approved": 0,
        "needs_reanalysis": 1,
        "rejected": 1,
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"decision": "rejected"},
        {"decision": "needs_reanalysis"},
        {"decision": "rejected", "reason_code": "other", "notes": ""},
    ],
)
def test_invalid_reason_combinations_return_422_without_writing(
    payload: dict[str, str], client: TestClient, service: ReviewService
) -> None:
    _seed_opportunities()
    page = client.get("/review")

    response = client.post(
        f"/review/{OPPORTUNITY_HIGH}/decision",
        data=_decision_payload(_csrf_token(page.text), **payload),
        follow_redirects=False,
    )

    assert response.status_code == 422
    assert service.history() == []


def test_duplicate_active_decision_is_prevented(service: ReviewService) -> None:
    _seed_opportunities()
    decision = ReviewDecisionInput(decision="approved_test_purchase")
    service.create_decision(OPPORTUNITY_HIGH, decision)

    with pytest.raises(ReviewConflictError):
        service.create_decision(OPPORTUNITY_HIGH, decision)

    assert len(service.history()) == 1


def test_undo_preserves_history_and_returns_item_to_queue(service: ReviewService) -> None:
    _seed_opportunities()
    service.create_decision(
        OPPORTUNITY_HIGH,
        ReviewDecisionInput(
            decision="rejected",
            reason_code="incorrect_matching",
            notes="Voltagem conflitante",
        ),
    )

    service.undo(OPPORTUNITY_HIGH, reviewer="Auditor")

    history = service.history()
    assert len(history) == 2
    assert history[0]["event_type"] == "undo"
    assert history[0]["undoes_review_id"] == history[1]["id"]
    assert history[1]["is_active"] is False
    assert OPPORTUNITY_HIGH in {row["id"] for row in service.pending(ReviewFilters())}


def test_nonexistent_opportunity_returns_404(client: TestClient) -> None:
    _seed_opportunities()
    page = client.get("/review")

    response = client.post(
        "/review/ffffffff-ffff-ffff-ffff-ffffffffffff/decision",
        data=_decision_payload(_csrf_token(page.text)),
        follow_redirects=False,
    )

    assert response.status_code == 404


def test_csrf_is_required(client: TestClient) -> None:
    _seed_opportunities()

    response = client.post(
        f"/review/{OPPORTUNITY_HIGH}/decision",
        data={"decision": "approved_test_purchase", "filter_sort": "score"},
        follow_redirects=False,
    )

    assert response.status_code == 403


def test_history_summary_and_existing_routes_keep_working(client: TestClient) -> None:
    _seed_opportunities()

    assert client.get("/health").status_code == 200
    assert client.get("/decision-engine/opportunities?limit=10").status_code == 200
    summary = client.get("/review/summary")
    history = client.get("/review/history")

    assert summary.status_code == 200
    assert summary.json()["pending"] == 2
    assert history.status_code == 200
    assert "Nenhuma revisão registrada" in history.text
