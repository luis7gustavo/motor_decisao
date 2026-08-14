from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.collection import SourceRegistry, SourceRegistryError


def _write_source(path: Path, source_id: str, priority: int, profiles: list[str]) -> None:
    path.write_text(
        f"""
source_id: {source_id}
display_name: {source_id.title()}
enabled: true
source_type: market
market_scope: retailer
collector: {source_id}
strategy: parsel
access: PUBLIC_HTTP
priority: {priority}
profiles: {profiles!r}
""".strip(),
        encoding="utf-8",
    )


def test_registry_loads_and_orders_profile(tmp_path: Path) -> None:
    _write_source(tmp_path / "terabyte.yaml", "terabyte", 40, ["market", "full"])
    _write_source(tmp_path / "kabum.yaml", "kabum", 20, ["market", "full"])

    registry = SourceRegistry.from_directory(tmp_path)

    assert [source.source_id for source in registry.list(profile="market")] == [
        "kabum",
        "terabyte",
    ]
    assert registry.get("KABUM").display_name == "Kabum"


def test_registry_rejects_duplicate_source_ids(tmp_path: Path) -> None:
    _write_source(tmp_path / "one.yaml", "kabum", 20, ["market"])
    _write_source(tmp_path / "two.yaml", "kabum", 30, ["full"])

    with pytest.raises(SourceRegistryError, match="Duplicate source_id"):
        SourceRegistry.from_directory(tmp_path)
