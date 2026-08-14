from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class WorkerLimits(BaseModel):
    model_config = ConfigDict(extra="forbid")
    http_flow_limit: int = Field(ge=1, le=32)
    browser_flow_limit: int = Field(ge=1, le=8)
    http_desired_concurrency: int = Field(ge=1, le=100)
    http_max_concurrency: int = Field(ge=1, le=100)
    browser_desired_concurrency: int = Field(ge=1, le=10)
    browser_max_concurrency: int = Field(ge=1, le=10)

    @model_validator(mode="after")
    def validate_concurrency(self) -> "WorkerLimits":
        if self.http_desired_concurrency > self.http_max_concurrency:
            raise ValueError("http desired concurrency must not exceed maximum")
        if self.browser_desired_concurrency > self.browser_max_concurrency:
            raise ValueError("browser desired concurrency must not exceed maximum")
        return self


class PrefectPlatformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    work_pool: str = "sillo-collection"
    work_queues: dict[str, str]
    schedules_enabled_by_default: bool = False


class CollectionPlatformConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query_groups: dict[str, list[str]]
    workers: dict[str, WorkerLimits]
    prefect: PrefectPlatformConfig

    @classmethod
    def load(cls, path: str | Path = "config/collection.yaml") -> "CollectionPlatformConfig":
        with Path(path).open("r", encoding="utf-8") as file:
            payload: Any = yaml.safe_load(file) or {}
        return cls.model_validate(payload)

    def queries_for(self, query_group: str | None) -> list[str]:
        if not query_group:
            return []
        try:
            return list(self.query_groups[query_group])
        except KeyError as error:
            raise ValueError(f"Unknown query group: {query_group}") from error
