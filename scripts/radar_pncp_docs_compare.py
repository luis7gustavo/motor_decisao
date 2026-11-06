"""Compare the archived and current PNCP consultation OpenAPI semantically."""
import hashlib
import json
from pathlib import Path


OLD = Path('/app/config/contracts/pncp-consulta-20261013.json')
NEW = Path('/evidence/pncp-docs/20261028/openapi.json')
REPORT = Path('/evidence/pncp-docs/20261028/openapi-comparison.json')


def changed_paths(before, after, prefix=''):
    if isinstance(before, dict) and isinstance(after, dict):
        changes = []
        for key in sorted(set(before) | set(after)):
            if key not in before or key not in after:
                changes.append(prefix + '/' + key)
            else:
                changes.extend(changed_paths(before[key], after[key], prefix + '/' + key))
        return changes
    if isinstance(before, list) and isinstance(after, list):
        if len(before) != len(after):
            return [prefix + '/length']
        changes = []
        for index, (left, right) in enumerate(zip(before, after)):
            changes.extend(changed_paths(left, right, prefix + '/' + str(index)))
        return changes
    return [] if before == after else [prefix]


def main():
    if REPORT.exists():
        raise FileExistsError('Comparison evidence exists')
    before_bytes, after_bytes = OLD.read_bytes(), NEW.read_bytes()
    before, after = json.loads(before_bytes), json.loads(after_bytes)
    changes = changed_paths(before, after)
    result = {
        'old_sha256': hashlib.sha256(before_bytes).hexdigest(),
        'new_sha256': hashlib.sha256(after_bytes).hexdigest(),
        'semantic_equal': not changes, 'changed_paths': changes,
        'path_count_old': len(before['paths']), 'path_count_new': len(after['paths']),
    }
    REPORT.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
