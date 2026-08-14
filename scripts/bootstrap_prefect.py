from __future__ import annotations

import asyncio

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.exceptions import ObjectNotFound

from pipelines.collection.platform_config import CollectionPlatformConfig


async def bootstrap() -> None:
    config = CollectionPlatformConfig.load()
    pool_name = config.prefect.work_pool
    workers = config.workers
    queue_limits = {
        config.prefect.work_queues["api_http"]: sum(
            worker.http_flow_limit for worker in workers.values()
        ),
        config.prefect.work_queues["browser"]: sum(
            worker.browser_flow_limit for worker in workers.values()
        ),
        config.prefect.work_queues["manual"]: 1,
    }
    async with get_client() as client:
        await client.create_work_pool(
            WorkPoolCreate(
                name=pool_name,
                type="process",
                description="Workers locais PC1/PC2 da coleta SILLO",
                concurrency_limit=sum(queue_limits.values()),
            ),
            overwrite=True,
        )
        for priority, (queue_name, limit) in enumerate(queue_limits.items(), start=1):
            try:
                queue = await client.read_work_queue_by_name(queue_name, work_pool_name=pool_name)
            except ObjectNotFound:
                await client.create_work_queue(
                    name=queue_name,
                    work_pool_name=pool_name,
                    concurrency_limit=limit,
                    priority=priority,
                )
            else:
                await client.update_work_queue(queue.id, concurrency_limit=limit, priority=priority)
    print(f"Work pool {pool_name!r} e filas sincronizados.")


if __name__ == "__main__":
    asyncio.run(bootstrap())
