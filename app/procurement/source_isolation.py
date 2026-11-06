"""Persistent advisory hold for the other known PNCP client during C2 operations."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time


HOLD_PATH = Path('/evidence/pncp-control/exclusive.lock')


def require_source_hold(path, mission_key):
    try:
        owner = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError) as error:
        raise ValueError('PNCP source hold is absent or invalid') from error
    if owner.get('mission_key') != mission_key:
        raise ValueError('PNCP source hold belongs to another mission')


def ensure_source_hold(path, mission_key, *, drain_seconds=60):
    """Block the root client, then drain requests already in flight before C2 sends."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({'mission_key': mission_key,
                          'created_at': datetime.now(timezone.utc).isoformat()},
                         sort_keys=True).encode()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        require_source_hold(path, mission_key)
        return False
    with os.fdopen(fd, 'wb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    if drain_seconds:
        time.sleep(drain_seconds)
    return True


def transfer_source_hold(path, old_mission_key, new_mission_key):
    """Change hold owner without a moment when the root client can send."""
    path = Path(path)
    require_source_hold(path, old_mission_key)
    temp = path.with_name(path.name + '.transfer')
    if temp.exists():
        raise FileExistsError('Unresolved PNCP source hold transfer')
    payload = json.dumps({'mission_key': new_mission_key,
                          'transferred_from': old_mission_key,
                          'created_at': datetime.now(timezone.utc).isoformat()},
                         sort_keys=True).encode()
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    directory = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    require_source_hold(path, new_mission_key)
