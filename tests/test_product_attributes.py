from entity_resolution.product_attributes import (
    attribute_conflicts,
    conflict_penalty,
    extract_product_attributes,
)


def test_storage_capacity_and_interface_conflicts_are_high_risk() -> None:
    supplier = extract_product_attributes("SSD SATA 480GB Kingston")
    evidence = extract_product_attributes("SSD NVME 1TB Kingston")

    conflicts = attribute_conflicts(supplier, evidence)

    assert "capacities_conflict" in conflicts
    assert "storage_interfaces_conflict" in conflicts
    assert conflict_penalty(conflicts) == 0.30


def test_connector_and_power_variants_conflict() -> None:
    supplier = extract_product_attributes("Fonte 650W Cabo HDMI")
    evidence = extract_product_attributes("Fonte 500W Cabo USB C")

    conflicts = attribute_conflicts(supplier, evidence)

    assert "power_watts_conflict" in conflicts
    assert "connector_interfaces_conflict" in conflicts


def test_bivolt_is_compatible_with_specific_voltage() -> None:
    supplier = extract_product_attributes("Carregador USB C Bivolt")
    evidence = extract_product_attributes("Carregador USB C 110V")

    assert "voltages_conflict" not in attribute_conflicts(supplier, evidence)


def test_payload_attributes_enrich_brand_and_model_extraction() -> None:
    payload = {
        "product": {
            "name": "Mouse sem fio",
            "attributes": [
                {"id": "BRAND", "value_name": "Logitech"},
                {"id": "MODEL", "value_name": "M170"},
            ],
        }
    }

    attributes = extract_product_attributes("Mouse Wireless", payload=payload)

    assert "logitech" in attributes.brands
    assert "m170" in attributes.model_codes


def test_mousepad_does_not_become_mouse_type() -> None:
    attributes = extract_product_attributes("Mousepad gamer preto")

    assert attributes.product_types == ("mousepad",)
