import json

import pytest

from app.procurement.source_isolation import ensure_source_hold, require_source_hold, transfer_source_hold


def test_hold_is_durable_idempotent_and_rejects_other_mission(tmp_path):
    hold = tmp_path / 'pncp' / 'exclusive.lock'
    assert ensure_source_hold(hold, 'history-a', drain_seconds=0)
    assert json.loads(hold.read_text())['mission_key'] == 'history-a'
    require_source_hold(hold, 'history-a')
    assert ensure_source_hold(hold, 'history-a', drain_seconds=0) is False
    with pytest.raises(ValueError, match='another mission'):
        ensure_source_hold(hold, 'history-b', drain_seconds=0)
    with pytest.raises(ValueError, match='another mission'):
        require_source_hold(hold, 'history-b')
    transfer_source_hold(hold, 'history-a', 'history-b')
    require_source_hold(hold, 'history-b')
