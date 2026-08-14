from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from prefect.client.schemas.schedules import IntervalSchedule
from prefect.types.entrypoint import EntrypointType

from app.core.settings import get_settings
from pipelines.collection.platform_config import CollectionPlatformConfig
from pipelines.collection.prefect_flows import collect_profile, collect_source
from pipelines.collection.registry import SourceRegistry


def _queue_for(source, queues: dict[str, str]) -> str:
    return queues["browser"] if source.strategy.uses_browser else queues["api_http"]


def main() -> None:
    settings = get_settings()
    registry = SourceRegistry.from_directory(settings.source_registry_path)
    config = CollectionPlatformConfig.load()
    pool = config.prefect.work_pool
    queues = config.prefect.work_queues
    schedules_enabled = os.getenv("SILLO_ENABLE_SCHEDULES", "false").lower() in {"1", "true", "yes"}

    deployments = [
        collect_source.to_deployment(
            name="manual",
            work_pool_name=pool,
            work_queue_name=queues["manual"],
            paused=False,
            tags=["sillo", "collection", "manual"],
            job_variables={"working_dir": "/app"},
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
        collect_profile.to_deployment(
            name="manual",
            work_pool_name=pool,
            work_queue_name=queues["manual"],
            paused=False,
            tags=["sillo", "collection", "profile"],
            job_variables={"working_dir": "/app"},
            entrypoint_type=EntrypointType.MODULE_PATH,
        ),
    ]

    for source in registry.list(enabled_only=False):
        policy = source.scheduling
        if not policy.enabled or policy.interval_hours is None:
            continue
        timezone = ZoneInfo(policy.timezone)
        anchor = datetime.now(timezone).replace(minute=policy.minute_offset, second=0, microsecond=0)
        schedule = IntervalSchedule(
            interval=timedelta(hours=policy.interval_hours),
            anchor_date=anchor,
            timezone=policy.timezone,
        )
        deployments.append(
            collect_source.to_deployment(
                name=f"scheduled-{source.source_id}",
                parameters={"source_id": source.source_id, "canary": False, "max_results": None},
                schedule=schedule,
                paused=not schedules_enabled or not source.enabled,
                work_pool_name=pool,
                work_queue_name=_queue_for(source, queues),
                tags=[
                    "sillo",
                    "collection",
                    source.source_id,
                    source.strategy.value,
                    "enabled" if source.enabled else "disabled",
                ],
                job_variables={"working_dir": "/app"},
                entrypoint_type=EntrypointType.MODULE_PATH,
            )
        )

    deployment_ids = [deployment.apply(work_pool_name=pool) for deployment in deployments]
    state = "ativas" if schedules_enabled else "pausadas por seguranca"
    print(f"{len(deployment_ids)} deployments sincronizados; agendas {state}.")


if __name__ == "__main__":
    main()
