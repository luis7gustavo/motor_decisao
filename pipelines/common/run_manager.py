from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import Connection, text

from pipelines.common.serialization import to_json_text


def create_pipeline_run(
    connection: Connection,
    *,
    pipeline_name: str,
    triggered_by: str,
    config_snapshot: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> UUID:
    row = connection.execute(
        text(
            """
            INSERT INTO control.pipeline_runs (
                pipeline_name,
                status,
                triggered_by,
                config_snapshot,
                metadata
            )
            VALUES (
                :pipeline_name,
                'running',
                :triggered_by,
                CAST(:config_snapshot AS jsonb),
                CAST(:metadata AS jsonb)
            )
            RETURNING id
            """
        ),
        {
            "pipeline_name": pipeline_name,
            "triggered_by": triggered_by,
            "config_snapshot": to_json_text(config_snapshot),
            "metadata": to_json_text(metadata or {}),
        },
    ).one()
    return row.id


def finish_pipeline_run(
    connection: Connection,
    *,
    pipeline_run_id: UUID,
    status: str,
    metadata: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    connection.execute(
        text(
            """
            UPDATE control.pipeline_runs
            SET
                status = :status,
                metadata = COALESCE(CAST(:metadata AS jsonb), metadata),
                error_message = :error_message,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE id = :pipeline_run_id
            """
        ),
        {
            "pipeline_run_id": pipeline_run_id,
            "status": status,
            "metadata": to_json_text(metadata or {}),
            "error_message": error_message,
        },
    )


def create_source_run(
    connection: Connection,
    *,
    pipeline_run_id: UUID,
    source_name: str,
    source_type: str,
    raw_table_name: str,
    metadata: dict[str, Any] | None = None,
) -> UUID:
    row = connection.execute(
        text(
            """
            INSERT INTO control.source_runs (
                pipeline_run_id,
                source_name,
                source_type,
                status,
                raw_table_name,
                metadata
            )
            VALUES (
                :pipeline_run_id,
                :source_name,
                :source_type,
                'running',
                :raw_table_name,
                CAST(:metadata AS jsonb)
            )
            RETURNING id
            """
        ),
        {
            "pipeline_run_id": pipeline_run_id,
            "source_name": source_name,
            "source_type": source_type,
            "raw_table_name": raw_table_name,
            "metadata": to_json_text(metadata or {}),
        },
    ).one()
    return row.id


def finish_source_run(
    connection: Connection,
    *,
    source_run_id: UUID,
    status: str,
    records_extracted: int,
    records_loaded: int,
    records_skipped: int,
    metadata: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    connection.execute(
        text(
            """
            UPDATE control.source_runs
            SET
                status = :status,
                records_extracted = :records_extracted,
                records_loaded = :records_loaded,
                records_skipped = :records_skipped,
                metadata = COALESCE(CAST(:metadata AS jsonb), metadata),
                error_message = :error_message,
                finished_at = NOW(),
                updated_at = NOW()
            WHERE id = :source_run_id
            """
        ),
        {
            "source_run_id": source_run_id,
            "status": status,
            "records_extracted": records_extracted,
            "records_loaded": records_loaded,
            "records_skipped": records_skipped,
            "metadata": to_json_text(metadata or {}),
            "error_message": error_message,
        },
    )


def record_quality_check(
    connection: Connection,
    *,
    pipeline_run_id: UUID,
    source_run_id: UUID | None,
    schema_name: str,
    table_name: str,
    check_name: str,
    status: str,
    metric_name: str | None = None,
    metric_value: float | int | None = None,
    threshold_value: float | int | None = None,
    details: dict[str, Any] | None = None,
    message: str | None = None,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO control.data_quality_checks (
                pipeline_run_id,
                source_run_id,
                schema_name,
                table_name,
                check_name,
                status,
                metric_name,
                metric_value,
                threshold_value,
                details,
                message
            )
            VALUES (
                :pipeline_run_id,
                :source_run_id,
                :schema_name,
                :table_name,
                :check_name,
                :status,
                :metric_name,
                :metric_value,
                :threshold_value,
                CAST(:details AS jsonb),
                :message
            )
            """
        ),
        {
            "pipeline_run_id": pipeline_run_id,
            "source_run_id": source_run_id,
            "schema_name": schema_name,
            "table_name": table_name,
            "check_name": check_name,
            "status": status,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "threshold_value": threshold_value,
            "details": to_json_text(details or {}),
            "message": message,
        },
    )

