"""Capture one bounded public PNCP OpenAPI document through Docker.

This is documentation evidence, not a procurement data request. A durable
attempt record is created before DNS/connection; no policy or quota is enabled.
"""
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


URLS = {
    'consultation': 'https://pncp.gov.br/pncp-consulta/v3/api-docs',
    'integration': 'https://pncp.gov.br/pncp-api/v3/api-docs',
}
ROOT = Path('/evidence/pncp-docs/20261028')
MAX_BYTES = 10 * 1024 * 1024


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('document', choices=URLS, nargs='?', default='consultation')
    args = parser.parse_args()
    url = URLS[args.document]
    ROOT.mkdir(parents=True, exist_ok=True)
    prefix = 'openapi' if args.document == 'consultation' else 'integration-openapi'
    attempt = ROOT / (prefix + '-attempt.json')
    receipt = ROOT / (prefix + '-receipt.json')
    snapshot = ROOT / (prefix + '.json')
    if any(path.exists() for path in (attempt, receipt, snapshot)):
        raise FileExistsError('Documentation evidence exists; inspect before retry')
    attempt.write_text(json.dumps({
        'url': url, 'reserved_utc': datetime.now(timezone.utc).isoformat(),
        'documentary_attempt_budget': 1, 'attempt_number': 1,
    }, indent=2) + '\n')
    request = Request(url, headers={'Accept': 'application/json',
                                    'User-Agent': 'RadarLicita-documentation-audit/1.0'})
    with urlopen(request, timeout=20) as response:
        if response.status != 200 or urlsplit(response.geturl()).hostname != 'pncp.gov.br':
            raise ValueError('Unexpected OpenAPI response or redirect')
        body = response.read(MAX_BYTES + 1)
        if len(body) > MAX_BYTES:
            raise ValueError('OpenAPI document exceeds bounded size')
        parsed = json.loads(body)
        if not isinstance(parsed, dict) or not parsed.get('openapi') or not parsed.get('paths'):
            raise ValueError('Response is not an OpenAPI document')
        fetched_url = response.geturl()
        content_type = response.headers.get('Content-Type', '')
    with snapshot.open('xb') as stream:
        stream.write(body)
    sha256 = hashlib.sha256(body).hexdigest()
    result = {
        'url': fetched_url, 'received_utc': datetime.now(timezone.utc).isoformat(),
        'http_status': 200, 'content_type': content_type, 'bytes': len(body),
        'sha256': sha256, 'openapi_version': parsed.get('openapi'),
        'path_count': len(parsed['paths']), 'snapshot': snapshot.name,
    }
    receipt.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
