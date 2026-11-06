from datetime import timedelta

from app.core.time import local_today
from app.models import MatchClass, ProcurementItem, Supplier, SupplierProduct
from app.suppliers.service import match_product, quote_is_valid


def test_technically_different_items_are_not_matched_by_name_only(db, procurement):
    item = ProcurementItem(
        procurement_id=procurement.id,
        item_number=1,
        description="Notebook memória: 16GB processador i7",
        judgment_criterion="Por item",
        raw_json={},
    )
    supplier = Supplier(name="Fornecedor")
    db.add_all([item, supplier])
    db.flush()
    product = SupplierProduct(
        supplier_id=supplier.id,
        name="Notebook",
        technical_description="Notebook básico",
        specifications={"memória": "8GB", "processador": "i3"},
    )
    db.add(product)
    db.flush()
    result = match_product(db, item, product)
    assert result.classification == MatchClass.INCOMPATIBLE


def test_specification_before_its_key_does_not_create_false_mismatch(db, procurement):
    item = ProcurementItem(
        procurement_id=procurement.id,
        item_number=2,
        description="Notebook profissional 16 GB",
        complementary_description="16 GB RAM, SSD 512 GB",
        raw_json={},
    )
    supplier = Supplier(name="Fornecedor compativel")
    db.add_all([item, supplier])
    db.flush()
    product = SupplierProduct(
        supplier_id=supplier.id,
        name="Notebook profissional",
        technical_description="16 GB RAM SSD 512 GB",
        specifications={"RAM": "16 GB", "SSD": "512 GB"},
    )
    db.add(product)
    db.flush()
    result = match_product(db, item, product)
    assert result.classification != MatchClass.INCOMPATIBLE
    assert result.mandatory_spec_checks["RAM"]["required"] == ["16 GB"]


def test_supplier_without_valid_quote_is_detected():
    assert quote_is_valid(local_today() - timedelta(days=1)) is False
    assert quote_is_valid(local_today() + timedelta(days=1)) is True


def test_api_health_and_empty_dashboard(client):
    assert client.get("/api/v1/health").status_code == 200
    summary = client.get("/api/v1/dashboard/summary")
    assert summary.status_code == 200
    assert summary.json()["open_opportunities"] == 0

