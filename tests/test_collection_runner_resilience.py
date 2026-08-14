from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

from pipelines.collection.models import CollectionResult, CollectionStatus, CollectionStrategy
from pipelines.collection.runner import CollectionRunner


class _Transaction:
    def __enter__(self) -> object:
        return object()

    def __exit__(self, *_args: object) -> None:
        return None


class _Engine:
    def begin(self) -> _Transaction:
        return _Transaction()


def test_profile_continues_after_one_source_fails(monkeypatch) -> None:
    source_ids = ["kabum", "amazon", "mercado_livre", "terabyte"]
    runner = object.__new__(CollectionRunner)
    runner.registry = SimpleNamespace(
        list=lambda **_kwargs: [
            SimpleNamespace(
                source_id=source_id,
                canary_enabled=True,
                model_dump=lambda **_kwargs: {},
            )
            for source_id in source_ids
        ]
    )
    calls: list[str] = []

    async def fake_run_source(source_id: str, **kwargs) -> CollectionResult:
        calls.append(source_id)
        now = datetime.now(timezone.utc)
        failed = source_id == "kabum"
        return CollectionResult(
            run_id=f"run-{source_id}",
            pipeline_run_id=str(kwargs["pipeline_run_id"]),
            source_id=source_id,
            started_at=now,
            finished_at=now,
            status=CollectionStatus.FAILED if failed else CollectionStatus.SUCCESS,
            items_discovered=0 if failed else 1,
            items_valid=0 if failed else 1,
            items_persisted=0 if failed else 1,
            requests_total=1,
            requests_success=0 if failed else 1,
            requests_failed=1 if failed else 0,
            http_requests=1,
            collection_strategy=CollectionStrategy.HTTP,
            worker_id="pc1-http",
            hostname="pc1",
        )

    runner.run_source = fake_run_source
    monkeypatch.setattr("pipelines.collection.runner.engine", _Engine())
    monkeypatch.setattr(
        "pipelines.collection.runner.create_pipeline_run",
        lambda *_args, **_kwargs: UUID("00000000-0000-0000-0000-000000000001"),
    )
    monkeypatch.setattr("pipelines.collection.runner.finish_pipeline_run", lambda *_args, **_kwargs: None)

    result = asyncio.run(runner.run_profile("market", canary=True))

    assert calls == source_ids
    assert result["status"] == "partial"
    assert [item["status"] for item in result["results"]] == [
        "failed",
        "success",
        "success",
        "success",
    ]
