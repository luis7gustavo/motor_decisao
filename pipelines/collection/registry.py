from __future__ import annotations

from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from pipelines.collection.models import SourceConfig


class SourceRegistryError(ValueError):
    pass


class SourceRegistry:
    """Registro imutavel de configuracoes de fonte validadas."""

    def __init__(self, sources: Iterable[SourceConfig]) -> None:
        by_id: dict[str, SourceConfig] = {}
        for source in sources:
            if source.source_id in by_id:
                raise SourceRegistryError(f"Duplicate source_id: {source.source_id}")
            by_id[source.source_id] = source
        self._sources = by_id

    def __len__(self) -> int:
        return len(self._sources)

    def __iter__(self) -> Iterator[SourceConfig]:
        yield from self.list()

    def get(self, source_id: str) -> SourceConfig:
        normalized = source_id.strip().lower()
        try:
            return self._sources[normalized]
        except KeyError as error:
            raise SourceRegistryError(f"Unknown source_id: {normalized}") from error

    def list(
        self,
        *,
        enabled_only: bool = False,
        profile: str | None = None,
    ) -> list[SourceConfig]:
        normalized_profile = profile.strip().lower() if profile else None
        sources = [
            source
            for source in self._sources.values()
            if (not enabled_only or source.enabled)
            and (normalized_profile is None or normalized_profile in source.profiles)
        ]
        return sorted(sources, key=lambda item: (item.priority, item.source_id))

    @classmethod
    def from_directory(cls, directory: str | Path) -> "SourceRegistry":
        root = Path(directory)
        if not root.is_dir():
            raise SourceRegistryError(f"Source registry directory not found: {root}")

        sources: list[SourceConfig] = []
        paths = sorted([*root.glob("*.yaml"), *root.glob("*.yml")])
        if not paths:
            raise SourceRegistryError(f"No YAML source files found in: {root}")
        for path in paths:
            sources.extend(cls._load_file(path))
        return cls(sources)

    @staticmethod
    def _load_file(path: Path) -> list[SourceConfig]:
        try:
            with path.open("r", encoding="utf-8") as file:
                payload = yaml.safe_load(file)
        except (OSError, yaml.YAMLError) as error:
            raise SourceRegistryError(f"Cannot read {path}: {error}") from error

        raw_sources = SourceRegistry._normalize_payload(payload, path=path)
        parsed: list[SourceConfig] = []
        for raw_source in raw_sources:
            try:
                parsed.append(SourceConfig.model_validate(raw_source))
            except ValidationError as error:
                raise SourceRegistryError(f"Invalid source config in {path}: {error}") from error
        return parsed

    @staticmethod
    def _normalize_payload(payload: Any, *, path: Path) -> list[dict[str, Any]]:
        if not isinstance(payload, dict):
            raise SourceRegistryError(f"Registry file must be a mapping: {path}")

        if "source_id" in payload:
            return [payload]

        if "sources" in payload:
            raw_sources = payload["sources"]
            if not isinstance(raw_sources, list) or not all(
                isinstance(item, dict) for item in raw_sources
            ):
                raise SourceRegistryError(f"'sources' must be a list of mappings: {path}")
            return list(raw_sources)

        normalized: list[dict[str, Any]] = []
        for source_id, config in payload.items():
            if not isinstance(config, dict):
                raise SourceRegistryError(f"Source '{source_id}' must be a mapping: {path}")
            normalized.append({"source_id": source_id, **config})
        return normalized
