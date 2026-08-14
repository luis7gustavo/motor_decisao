from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from pipelines.collection.alerts import evaluate_alerts
from pipelines.collection.models import CollectionResult, CollectionStatus, CollectionStrategy


class _Result:
    def __init__(self, *, scalar: int | None = None, values: list[str] | None = None) -> None:
        self._scalar = scalar
        self._values = values or []

    def scalar_one_or_none(self) -> int | None:
        return self._scalar

    def scalars(self):
        return iter(self._values)


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement, _parameters=None) -> _Result:
        sql = str(statement)
        self.statements.append(sql)
        if "SELECT records_extracted" in sql:
            return _Result(scalar=100)
        if "SELECT status FROM control.source_runs" in sql:
            return _Result(values=["success", "success"])
        return _Result()


def _result(*, canary: bool) -> CollectionResult:
    now = datetime.now(timezone.utc)
    return CollectionResult(
        run_id="00000000-0000-0000-0000-000000000002",
        pipeline_run_id="00000000-0000-0000-0000-000000000001",
        source_id="kabum",
        started_at=now,
        finished_at=now,
        status=CollectionStatus.SUCCESS,
        items_discovered=1,
        items_valid=1,
        items_persisted=1,
        requests_total=1,
        requests_success=1,
        browser_requests=1,
        collection_strategy=CollectionStrategy.ADAPTIVE_PLAYWRIGHT,
        worker_id="pc1-browser",
        hostname="pc1",
        metadata={"canary": canary},
    )


def test_canary_does_not_open_volume_drop_and_resolves_previous_alerts() -> None:
    connection = _Connection()

    alerts = evaluate_alerts(
        connection,
        source_run_id=UUID("00000000-0000-0000-0000-000000000002"),
        result=_result(canary=True),
    )

    assert alerts == []
    assert any("UPDATE control.collection_alerts" in sql for sql in connection.statements)
    assert not any("INSERT INTO control.collection_alerts" in sql for sql in connection.statements)


def test_full_run_opens_volume_drop_when_volume_falls_below_threshold() -> None:
    connection = _Connection()

    alerts = evaluate_alerts(
        connection,
        source_run_id=UUID("00000000-0000-0000-0000-000000000002"),
        result=_result(canary=False),
    )

    assert [alert.alert_type for alert in alerts] == ["volume_drop"]
    assert any("INSERT INTO control.collection_alerts" in sql for sql in connection.statements)
