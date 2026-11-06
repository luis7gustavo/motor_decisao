from datetime import datetime, timedelta, timezone

from scripts import radar_pncp_history


class FakeService:
    def __init__(self, delay):
        self.delay = delay
        self.calls = 0

    def step(self, _mission_id, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            return {'state': 'deferred',
                    'not_before': datetime.now(timezone.utc) + self.delay}
        return {'state': 'complete'}


def test_short_source_interval_retries_same_step(monkeypatch):
    waits = []
    monkeypatch.setattr(radar_pncp_history.time, 'sleep', waits.append)
    service = FakeService(timedelta(milliseconds=250))

    result = radar_pncp_history.step_after_short_source_wait(
        service, 'mission', priority_floor=100, resource_filter=None)

    assert result['state'] == 'complete'
    assert service.calls == 2
    assert len(waits) == 1 and 0 < waits[0] < 1


def test_long_source_deferral_stays_deferred(monkeypatch):
    monkeypatch.setattr(
        radar_pncp_history.time, 'sleep',
        lambda _delay: (_ for _ in ()).throw(AssertionError('unexpected wait')),
    )
    service = FakeService(timedelta(seconds=10))

    result = radar_pncp_history.step_after_short_source_wait(
        service, 'mission', priority_floor=100, resource_filter=None)

    assert result['state'] == 'deferred'
    assert service.calls == 1
