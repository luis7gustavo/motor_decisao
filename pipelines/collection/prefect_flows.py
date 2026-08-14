from __future__ import annotations

from typing import Any

from prefect import flow

from pipelines.collection.runner import CollectionRunner


@flow(name="sillo-collect-source", log_prints=False)
async def collect_source(
    source_id: str,
    canary: bool = False,
    max_results: int | None = None,
) -> dict[str, Any]:
    result = await CollectionRunner().run_source_guarded(
        source_id,
        triggered_by="prefect_collect_source",
        canary=canary,
        max_results=max_results,
    )
    return result.model_dump(mode="json")


@flow(name="sillo-collect-profile", log_prints=False)
async def collect_profile(
    profile: str,
    canary: bool = False,
    max_results: int | None = None,
) -> dict[str, Any]:
    return await CollectionRunner().run_profile(
        profile,
        triggered_by="prefect_collect_profile",
        canary=canary,
        max_results=max_results,
    )


@flow(name="sillo-collect-all", log_prints=False)
async def collect_all(canary: bool = False, max_results: int | None = None) -> dict[str, Any]:
    return await collect_profile(profile="full", canary=canary, max_results=max_results)
