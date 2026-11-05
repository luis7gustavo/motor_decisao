"""Read-only measurements for the finite PNCP history collection test."""
import json
from datetime import datetime, timezone

from sqlalchemy import text

from app.procurement.history import HistoryBackfill
from scripts.radar_pncp_history import target_store


MISSION_KEY = 'pncp-history-2026-10-27'
RESTART_UNIQUE_BASELINE = 62953


def main():
    store, engine = target_store()
    try:
        with store.transaction() as session:
            session.execute(text('SET TRANSACTION READ ONLY'))
            mission = session.execute(text('''SELECT id,state,scope,attempt_limit,attempts_consumed,
                budget_scope_key FROM control.source_missions WHERE mission_key=:key'''),
                {'key': MISSION_KEY}).mappings().one()
            start = datetime.fromisoformat(mission['scope']['test_window_start_utc'].replace('Z', '+00:00'))
            end = datetime.fromisoformat(mission['scope']['test_window_end_utc'].replace('Z', '+00:00'))
            params = {'mission': mission['id'], 'start': start, 'end': end}
            budget = session.execute(text('''SELECT attempt_limit,attempts_consumed,
                min_interval_ms,enabled,not_before FROM control.source_budgets
                WHERE scope_key=:key'''), {'key': mission['budget_scope_key']}).mappings().one()
            hourly = session.execute(text('''SELECT date_trunc('hour', a.reserved_at) AS hour_utc,
                count(*) AS attempts, count(*) FILTER (WHERE o.attempt_id IS NULL) AS missing_outcomes,
                count(*) FILTER (WHERE o.outcome='http_429') AS http_429,
                count(*) FILTER (WHERE o.outcome IN ('html_200','invalid_json_200')) AS malformed_200,
                count(*) FILTER (WHERE o.outcome='http_403') AS http_403,
                count(*) FILTER (WHERE o.http_status BETWEEN 500 AND 599) AS http_5xx,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY
                    (b.request_params_safe->>'client_elapsed_ms')::numeric) AS latency_p50_ms,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY
                    (b.request_params_safe->>'client_elapsed_ms')::numeric) AS latency_p95_ms
                FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                LEFT JOIN bronze.pncp_responses b ON b.id=o.bronze_response_id
                WHERE a.mission_id=:mission AND a.reserved_at>=:start AND a.reserved_at<:end
                GROUP BY 1 ORDER BY 1'''), params).mappings().all()
            unique_now = session.execute(text('''SELECT count(DISTINCT i.source_id)
                FROM control.source_partition_identities i JOIN control.source_partitions p
                ON p.id=i.partition_id WHERE p.mission_id=:mission'''),
                {'mission': mission['id']}).scalar_one()
            missing_all = session.execute(text('''SELECT count(*) FROM control.source_attempts a
                LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
                WHERE a.budget_scope_key=:key AND o.attempt_id IS NULL'''),
                {'key': mission['budget_scope_key']}).scalar_one()
        status = HistoryBackfill(store, lambda _: None).status(mission['id'])
        attempts = sum(row['attempts'] for row in hourly)
        result = {
            'observed_utc': datetime.now(timezone.utc).isoformat(),
            'test_window_start_utc': start.isoformat(), 'test_window_end_utc': end.isoformat(),
            'mission_state': mission['state'], 'mission_attempts_consumed': mission['attempts_consumed'],
            'mission_attempt_limit': mission['attempt_limit'],
            'source_enabled': budget['enabled'], 'source_attempts_consumed': budget['attempts_consumed'],
            'source_attempt_limit': budget['attempt_limit'],
            'effective_min_interval_ms': budget['min_interval_ms'],
            'source_not_before': budget['not_before'], 'test_attempts': attempts,
            'test_mean_attempts_per_minute': round(attempts / max(1, min(
                (datetime.now(timezone.utc) - start).total_seconds(), 86400)) * 60, 3),
            'test_http_429': sum(row['http_429'] for row in hourly),
            'test_malformed_200': sum(row['malformed_200'] for row in hourly),
            'test_http_403': sum(row['http_403'] for row in hourly),
            'test_http_5xx': sum(row['http_5xx'] for row in hourly),
            'missing_source_outcomes': missing_all,
            'unique_historical_identities': unique_now,
            'unique_identity_baseline_at_rewindow': RESTART_UNIQUE_BASELINE,
            'new_unique_identities_vs_pretest_baseline': unique_now - RESTART_UNIQUE_BASELINE,
            'interrupted_previous_test_attempts': 100,
            'historical_partitions': status['historical_partitions'],
            'recurring_partitions': status['recurring_partitions'],
            'hourly': [{**dict(row), 'hour_utc': row['hour_utc'].isoformat()}
                       for row in hourly],
            'pncp_network_calls': 0, 'database_writes': 0,
        }
        print(json.dumps(result, sort_keys=True, default=str))
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
