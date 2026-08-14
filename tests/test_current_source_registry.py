from pathlib import Path

from pipelines.collection import SourceRegistry


def test_current_source_registry_is_valid_and_staggered() -> None:
    root = Path(__file__).resolve().parents[1]
    registry = SourceRegistry.from_directory(root / "config" / "sources")

    assert registry.get("mercado_livre").strategy.value == "api"
    assert not registry.get("mercado_livre").enabled
    assert "Token expirado" in registry.get("mercado_livre").config["disabled_reason"]
    assert registry.get("mirao").strategy.value == "parsel"
    assert [source.source_id for source in registry.list(profile="market", enabled_only=True)] == [
        "amazon",
        "kabum",
        "cia_informatica",
    ]
    for blocked_source in ("terabyte", "buscape", "zoom"):
        source = registry.get(blocked_source)
        assert not source.enabled
        assert source.access.value == "COLLECTION_BLOCKED"
    assert registry.get("grupo_tek").access.value == "PUBLIC_HTTP"
    assert registry.get("fujioka_distribuidor").access.value == "AUTH_REQUIRED"
    assert registry.get("gamerstar").access.value == "COLLECTION_BLOCKED"
    minute_offsets = [
        source.scheduling.minute_offset
        for source in registry.list(enabled_only=True)
        if source.scheduling.enabled
    ]
    assert len(minute_offsets) == len(set(minute_offsets))
