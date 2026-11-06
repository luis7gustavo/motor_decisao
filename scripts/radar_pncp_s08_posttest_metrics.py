"""Read-only hourly metrics for the finished finite PNCP history test.

Run in the isolated ops environment with this source passed to ``python -``.
Output is aggregate CSV; no PNCP client, mutation, or secret is used.
"""

import csv
import sys

from sqlalchemy import text

from scripts.radar_pncp_history import target_store


QUERY = """
WITH mission AS (
    SELECT id,
           (scope->>'test_window_start_utc')::timestamptz AS start_at,
           (scope->>'test_window_end_utc')::timestamptz AS end_at
    FROM control.source_missions
    WHERE mission_key='pncp-history-2026-10-27'
), attempts AS (
    SELECT a.reserved_at, o.recorded_at, o.http_status,
           (b.request_params_safe->>'client_elapsed_ms')::numeric AS network_ms,
           lag(o.recorded_at) OVER (ORDER BY a.reserved_at,a.id) AS prior_outcome
    FROM control.source_attempts a
    JOIN mission m ON m.id=a.mission_id
    LEFT JOIN control.source_attempt_outcomes o ON o.attempt_id=a.id
    LEFT JOIN bronze.pncp_responses b ON b.id=o.bronze_response_id
    WHERE a.reserved_at>=m.start_at AND a.reserved_at<m.end_at
)
SELECT to_char(date_trunc('hour',reserved_at AT TIME ZONE 'America/Sao_Paulo'),
               'YYYY-MM-DD HH24:MI') AS hour_sp,
       count(*) AS attempts,
       count(*) FILTER (WHERE http_status BETWEEN 500 AND 599) AS http_5xx,
       count(network_ms) AS timed_attempts,
       round((percentile_cont(.5) WITHIN GROUP (ORDER BY network_ms))::numeric,1)
           AS latency_p50_ms,
       round((percentile_cont(.95) WITHIN GROUP (ORDER BY network_ms))::numeric,1)
           AS latency_p95_ms,
       round(sum(network_ms)/1000,1) AS network_seconds,
       round(sum(greatest(extract(epoch FROM reserved_at-prior_outcome),0)),1)
           AS after_outcome_seconds,
       count(*) FILTER (WHERE reserved_at-prior_outcome>interval '60 seconds')
           AS gap_over_60_count,
       round(sum(CASE WHEN reserved_at-prior_outcome>interval '60 seconds'
                      THEN extract(epoch FROM reserved_at-prior_outcome) ELSE 0 END),1)
           AS gap_over_60_seconds
FROM attempts
GROUP BY 1 ORDER BY 1
"""


def main():
    store, engine = target_store()
    try:
        with store.transaction() as session:
            session.execute(text('SET TRANSACTION READ ONLY'))
            rows = session.execute(text(QUERY)).mappings().all()
        writer = csv.writer(sys.stdout, lineterminator='\n')
        writer.writerow([
            'hour_sp', 'attempts', 'http_5xx', 'timed_attempts',
            'latency_p50_ms', 'latency_p95_ms', 'network_seconds',
            'after_outcome_seconds', 'gap_over_60_count', 'gap_over_60_seconds',
        ])
        for row in rows:
            writer.writerow(list(row.values()))
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
