from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from app.core.database import engine
from app.core.mercado_livre_tokens import refresh_mercado_livre_tokens
from app.core.settings import get_settings
from pipelines.collection.contracts import Collector
from pipelines.collection.crawlee import classify_collection_error
from pipelines.collection.models import CollectionContext, CollectionError, CollectionResult, CollectionStatus
from pipelines.mercado_livre.client import MercadoLivreClient
from pipelines.mercado_livre.ingest import _extract_item_fields, _insert_raw_item


class MercadoLivreAPICollector(Collector):
    def __init__(self, source_config, *, queries: list[str], max_items_per_query: int = 40) -> None:
        super().__init__(source_config)
        self.queries = queries
        self.max_items_per_query = max_items_per_query

    async def collect(self, run_id: str, context: CollectionContext) -> CollectionResult:
        return await asyncio.to_thread(self._collect_sync, run_id, context)

    def _collect_sync(self, run_id: str, context: CollectionContext) -> CollectionResult:
        started = datetime.now(timezone.utc)
        settings = get_settings()
        project = settings.load_project_config()
        api_config = project.get("market_sources", {}).get("mercado_livre", {})
        site_id = project.get("pipeline", {}).get("site_id", "MLB")
        selected_queries = self.queries[:1] if context.canary else self.queries
        discovered = 0
        valid = 0
        persisted = 0
        errors: list[CollectionError] = []

        def refresh_access_token() -> str:
            token_payload = refresh_mercado_livre_tokens()
            return str(token_payload["access_token"])

        can_refresh = bool(
            settings.ml_client_id
            and settings.ml_client_secret
            and settings.ml_refresh_token
        )
        client = MercadoLivreClient(
            api_base=str(api_config.get("api_base", settings.ml_api_base)),
            site_id=site_id,
            timeout_seconds=int(api_config.get("timeout_seconds", 20)),
            max_retries=int(api_config.get("max_retries", 3)),
            rate_limit_ms=int(api_config.get("rate_limit_ms", 250)),
            access_token=os.getenv("ML_ACCESS_TOKEN") or None,
            refresh_access_token=refresh_access_token if can_refresh else None,
        )
        try:
            with client:
                for query in selected_queries:
                    try:
                        page = client.search_items(
                            query=query,
                            limit=min(50, 1 if context.canary else self.max_items_per_query),
                        )
                        discovered += len(page.results)
                        with engine.begin() as connection:
                            for item in page.results:
                                fields = _extract_item_fields(item)
                                if not fields["external_id"] or not fields["title"]:
                                    continue
                                valid += 1
                                if _insert_raw_item(
                                    connection,
                                    source_run_id=UUID(run_id),
                                    site_id=site_id,
                                    category_id=None,
                                    query=query,
                                    item=item,
                                ):
                                    persisted += 1
                    except Exception as error:  # noqa: BLE001 - isolate each query.
                        errors.append(classify_collection_error(error, request_id=query))
        finally:
            client.close()

        finished = datetime.now(timezone.utc)
        status = CollectionStatus.SUCCESS
        if errors:
            status = CollectionStatus.PARTIAL if persisted else CollectionStatus.FAILED
        return CollectionResult(
            run_id=run_id,
            pipeline_run_id=context.pipeline_run_id,
            source_id=self.source_id,
            started_at=started,
            finished_at=finished,
            status=status,
            items_discovered=discovered,
            items_valid=valid,
            items_invalid=discovered - valid,
            items_persisted=persisted,
            requests_total=client.requests_total,
            requests_success=client.requests_success,
            requests_failed=client.requests_failed,
            http_requests=client.requests_total,
            browser_requests=0,
            retry_count=client.retry_count,
            errors=errors,
            collection_strategy=self.source_config.strategy,
            worker_id=context.worker_id,
            hostname=context.hostname,
            metadata={"queries": selected_queries, "site_id": site_id, "canary": context.canary},
        )
