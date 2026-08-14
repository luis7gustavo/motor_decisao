from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, text

from pipelines.collection.models import CollectionErrorType, CollectionResult
from pipelines.common.serialization import to_json_text


@dataclass(frozen=True)
class Alert:
    alert_type: str
    severity: str
    message: str
    observed_value: float | None = None
    threshold_value: float | None = None
    details: dict[str, Any] | None = None


def evaluate_alerts(
    connection: Connection,
    *,
    source_run_id: UUID,
    result: CollectionResult,
) -> list[Alert]:
    alerts: list[Alert] = []
    if result.items_discovered == 0:
        alerts.append(Alert("zero_items", "critical", "A coleta terminou sem descobrir itens", 0, 1))

    invalid_ratio = result.items_invalid / result.items_discovered if result.items_discovered else 0.0
    if invalid_ratio > 0.25:
        alerts.append(
            Alert("invalid_ratio", "warning", "Taxa de itens invalidos acima do limite", invalid_ratio, 0.25)
        )

    error_rate = result.requests_failed / result.requests_total if result.requests_total else 0.0
    if error_rate > 0.25:
        alerts.append(
            Alert("request_error_rate", "critical", "Taxa de erros HTTP acima do limite", error_rate, 0.25)
        )

    error_types = {item.error_type for item in result.errors}
    if CollectionErrorType.HTTP_403 in error_types:
        alerts.append(Alert("http_403", "critical", "A fonte respondeu HTTP 403"))
    if CollectionErrorType.HTTP_429 in error_types:
        alerts.append(Alert("http_429", "warning", "A fonte aplicou rate limit HTTP 429"))
    if CollectionErrorType.PARSER_ERROR in error_types:
        alerts.append(Alert("parser_error", "critical", "O contrato do parser deixou de ser atendido"))
    timeout_count = sum(item.error_type == CollectionErrorType.TIMEOUT for item in result.errors)
    if timeout_count >= 2:
        alerts.append(
            Alert("consecutive_timeouts", "warning", "Multiplos timeouts na mesma coleta", timeout_count, 2)
        )

    previous = connection.execute(
        text(
            """
            SELECT records_extracted
            FROM control.source_runs
            WHERE source_name = :source_name AND id <> :source_run_id
              AND status = 'success' AND records_extracted > 0
            ORDER BY finished_at DESC NULLS LAST
            LIMIT 1
            """
        ),
        {"source_name": result.source_id, "source_run_id": source_run_id},
    ).scalar_one_or_none()
    is_canary = bool(result.metadata.get("canary"))
    if previous and not is_canary and result.items_discovered < previous * 0.2:
        drop_ratio = 1 - (result.items_discovered / previous)
        alerts.append(
            Alert(
                "volume_drop",
                "critical",
                "Volume coletado caiu pelo menos 80% em relacao a ultima execucao bem-sucedida",
                drop_ratio,
                0.8,
                {"previous_items": previous, "current_items": result.items_discovered},
            )
        )

    previous_statuses = list(
        connection.execute(
            text(
                """
                SELECT status FROM control.source_runs
                WHERE source_name = :source_name AND id <> :source_run_id
                ORDER BY started_at DESC LIMIT 2
                """
            ),
            {"source_name": result.source_id, "source_run_id": source_run_id},
        ).scalars()
    )
    failed_statuses = {"failed", "blocked"}
    if result.status.value in failed_statuses and previous_statuses and previous_statuses[0] in failed_statuses:
        alerts.append(Alert("consecutive_failures", "critical", "A fonte falhou em execucoes consecutivas"))

    # Cada execucao representa o estado mais recente da fonte. Alertas antigos
    # deixam de ficar abertos indefinidamente e os problemas ainda presentes
    # sao reabertos abaixo, ligados ao run atual.
    connection.execute(
        text(
            """
            UPDATE control.collection_alerts
            SET status = 'resolved', resolved_at = NOW()
            WHERE source_name = :source_name AND status = 'open'
            """
        ),
        {"source_name": result.source_id},
    )

    for alert in alerts:
        connection.execute(
            text(
                """
                INSERT INTO control.collection_alerts (
                    pipeline_run_id, source_run_id, source_name, alert_type, severity,
                    message, observed_value, threshold_value, details
                ) VALUES (
                    :pipeline_run_id, :source_run_id, :source_name, :alert_type, :severity,
                    :message, :observed_value, :threshold_value, CAST(:details AS jsonb)
                )
                """
            ),
            {
                "pipeline_run_id": result.pipeline_run_id,
                "source_run_id": source_run_id,
                "source_name": result.source_id,
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "message": alert.message,
                "observed_value": alert.observed_value,
                "threshold_value": alert.threshold_value,
                "details": to_json_text(alert.details or {}),
            },
        )
    return alerts
