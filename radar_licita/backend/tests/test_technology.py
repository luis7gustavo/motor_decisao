import asyncio
from datetime import UTC, datetime, timedelta

from app.models import ProcurementItem
from app.technology.router import TechnologySearchRequest
from app.technology.service import categories, search_technology


class FakeAdapter:
    request_count = 4

    def __init__(self, current, historical):
        self.current = current
        self.historical = historical

    async def open_proposal_rows(self, *_args, **_kwargs):
        return [self.current]

    async def publication_rows(self, *_args, **_kwargs):
        return [self.historical]

    async def items(self, cnpj, year, sequence):
        if sequence == 1:
            return [
                {"numeroItem": 1, "descricao": "Notebook corporativo 16 GB SSD 512 GB", "quantidade": 2,
                 "valorUnitarioEstimado": 4000, "valorTotal": 8000, "unidadeMedida": "UN"},
                {"numeroItem": 2, "descricao": "Cadeira de escritório", "valorTotal": 1000},
            ]
        return [{"numeroItem": 3, "descricao": "Notebook corporativo 16 GB SSD 512 GB",
                 "temResultado": True, "unidadeMedida": "UN"}]

    async def results(self, *_args):
        return [{"tipoPessoa": "PJ", "niFornecedor": "11222333000144",
                 "nomeRazaoSocialFornecedor": "EMPRESA TESTE LTDA", "valorUnitarioHomologado": 3500,
                 "dataResultado": "2026-07-01"},
                {"tipoPessoa": "PF", "niFornecedor": "123", "nomeRazaoSocialFornecedor": "Pessoa"}]


def test_technology_crosses_open_items_budgets_and_historical_company(db, procurement_payload):
    current = dict(procurement_payload)
    current["dataEncerramentoProposta"] = (datetime.now(UTC) + timedelta(days=3)).isoformat()
    history = dict(procurement_payload)
    history.update(numeroControlePNCP="12345678000199-1-000002/2026", sequencialCompra=2,
                   objetoCompra="Aquisição de notebooks homologada")
    response = asyncio.run(search_technology(db, TechnologySearchRequest(budgets=[5000, 10000]), FakeAdapter(current, history)))
    assert len(response["opportunities"]) == 1
    item = response["opportunities"][0]
    assert item["public_estimate"] == "8000.00"
    assert [b["estimate_fits"] for b in item["budgets"]] == [False, True]
    assert item["companies"][0]["tax_id"] == "11222333000144"
    assert item["companies"][0]["technical_match"] == "pending"
    assert "resultados" in item["companies"][0]["evidence"][0]["source_url"]
    assert db.get(ProcurementItem, item["item_id"]) is not None


def test_classification_avoids_generic_non_technology_terms():
    assert not categories("Monitoramento de pacientes e sistema de oxigênio")
    assert not categories("Prestação de limpeza predial com apoio administrativo de software e hardware")
    assert not categories("Manutenção de scanner por raio X para inspeção de bagagens")
    assert categories("Licenciamento de software antivírus") == {"software"}


def test_budget_validation_rejects_negative(client):
    response = client.post("/api/v1/technology/search", json={"budgets": [-1]})
    assert response.status_code == 422
