"""Strict loader for source authorization policies."""

import json
from pathlib import Path


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate source policy key: {key}")
        result[key] = value
    return result


def load_source_policy(path):
    policy = json.loads(Path(path).read_text(encoding="utf-8"),
                        object_pairs_hook=_unique_pairs)
    if not isinstance(policy, dict):
        raise ValueError("Source policy must be a JSON object")
    return policy
