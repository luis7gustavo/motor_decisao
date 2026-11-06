from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import Procurement


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)


@pytest.fixture()
def client(db):
    app.dependency_overrides[get_db] = lambda: db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture()
def procurement_payload():
    close_at = (datetime.now(UTC) + timedelta(days=10)).replace(microsecond=0)
    return {
        "numeroControlePNCP": "12345678000199-1-000001/2026",
        "orgaoEntidade": {"cnpj": "12345678000199", "razaoSocial": "MUNICIPIO DE TESTE"},
        "unidadeOrgao": {
            "codigoUnidade": "1",
            "nomeUnidade": "PREFEITURA",
            "ufSigla": "SP",
            "municipioNome": "Teste",
            "codigoIbge": "9999999",
        },
        "anoCompra": 2026,
        "sequencialCompra": 1,
        "numeroCompra": "PE-1",
        "processo": "1/2026",
        "objetoCompra": "Aquisição de notebooks",
        "modalidadeId": 6,
        "modalidadeNome": "Pregão - Eletrônico",
        "modoDisputaNome": "Aberto",
        "situacaoCompraId": 1,
        "situacaoCompraNome": "Divulgada no PNCP",
        "valorTotalEstimado": 100000,
        "dataAberturaProposta": datetime.now(UTC).isoformat(),
        "dataEncerramentoProposta": close_at.isoformat(),
        "dataPublicacaoPncp": datetime.now(UTC).isoformat(),
        "dataAtualizacaoGlobal": datetime.now(UTC).isoformat(),
        "linkSistemaOrigem": "https://example.invalid/disputa",
    }


@pytest.fixture()
def procurement(db, procurement_payload):
    from app.ingestion.service import upsert_procurement

    result, _, _ = upsert_procurement(db, procurement_payload)
    db.commit()
    return db.get(Procurement, result.id)

