from pathlib import Path

from pipelines.collection.parsers import parse_market_listings, parse_supplier_products
from pipelines.market_web.sources import SOURCE_CONFIGS


FIXTURES = Path(__file__).parent / "fixtures"


def test_kabum_parser_contract() -> None:
    html = (FIXTURES / "kabum" / "search.html").read_text(encoding="utf-8")
    items = parse_market_listings(
        html,
        config=SOURCE_CONFIGS["kabum"],
        query="mouse gamer",
        page_url="https://www.kabum.com.br/busca/mouse-gamer",
        max_results=10,
    )
    assert len(items) == 1
    assert items[0].title == "Mouse Gamer X RGB"
    assert items[0].price == 299.99
    assert items[0].item_url == "https://www.kabum.com.br/produto/123/mouse-gamer-x"


def test_market_evidence_above_purchase_ceiling_is_parsed() -> None:
    html = (FIXTURES / "amazon" / "search.html").read_text(encoding="utf-8")
    items = parse_market_listings(
        html,
        config=SOURCE_CONFIGS["amazon"],
        query="ssd nvme",
        page_url="https://www.amazon.com.br/s?k=ssd+nvme",
        max_results=10,
    )
    assert len(items) == 1
    assert items[0].title == "SSD NVMe 1TB Marca X"
    assert items[0].price == 3300.0
    assert items[0].reviews_count == 128


def test_mirao_parser_contract() -> None:
    html = (FIXTURES / "mirao" / "catalog.html").read_text(encoding="utf-8")
    items = parse_supplier_products(
        html,
        supplier_slug="mirao",
        page_url="https://www.mirao.com.br/perifericos.html",
        selectors={
            "product": ".product-item",
            "title": "strong",
            "price": "span + span",
            "product_url": "a@href",
        },
    )
    assert len(items) == 1
    assert items[0].raw_title == "Mouse Optico USB Marca Y"
    assert items[0].raw_price == 29.99
    assert items[0].source_url == "https://www.mirao.com.br/mouse-optico"


def test_grupo_tek_parser_contract() -> None:
    html = (FIXTURES / "grupo_tek" / "catalog.html").read_text(encoding="utf-8")
    items = parse_supplier_products(
        html,
        supplier_slug="grupo_tek",
        page_url="https://www.tekdistribuidor.com.br/",
        selectors={
            "product": "a.product-info",
            "title": ".product-name",
            "price": ".current-price",
            "product_url": "@href",
        },
    )
    assert len(items) == 1
    assert items[0].raw_title == "Fonte PoE Ubiquiti 24V 0.5A"
    assert items[0].raw_price == 69.14
    assert items[0].source_url == "https://www.tekdistribuidor.com.br/fonte-poe-ubiquiti-24v"


def test_cia_informatica_parser_contract() -> None:
    html = (FIXTURES / "cia_informatica" / "catalog.html").read_text(encoding="utf-8")
    items = parse_supplier_products(
        html,
        supplier_slug="cia_informatica",
        page_url="https://www.ciainfor.com.br/loja/",
        selectors={
            "product": "ul.products li.product",
            "title": ".woocommerce-loop-product__title",
            "price": ".price",
            "product_url": "a.woocommerce-LoopProduct-link@href",
        },
    )
    assert len(items) == 1
    assert items[0].raw_title == "SSD NVMe 1TB Marca Z"
    assert items[0].raw_price == 499.90
    assert items[0].source_url == "https://www.ciainfor.com.br/produto/ssd-nvme-1tb/"
