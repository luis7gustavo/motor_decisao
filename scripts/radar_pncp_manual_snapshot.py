"""Archive the official PNCP consultation manual with one recorded doc GET."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


URL = ('https://www.gov.br/pncp/pt-br/central-de-conteudo/manuais/'
       'versoes-anteriores/ManualPNCPAPIConsultasVerso1.0.pdf')
ROOT = Path('/evidence/pncp-docs/20261028')
MAX_BYTES = 3 * 1024 * 1024


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    attempt = ROOT / 'manual-attempt.json'
    receipt = ROOT / 'manual-receipt.json'
    snapshot = ROOT / 'manual-consultas-v1.pdf'
    if any(path.exists() for path in (attempt, receipt, snapshot)):
        raise FileExistsError('Manual evidence exists; inspect before retry')
    attempt.write_text(json.dumps({
        'url': URL, 'reserved_utc': datetime.now(timezone.utc).isoformat(),
        'documentary_attempt_budget': 1, 'attempt_number': 1,
    }, indent=2) + '\n')
    request = Request(URL, headers={'Accept': 'application/pdf',
                                    'User-Agent': 'RadarLicita-documentation-audit/1.0'})
    with urlopen(request, timeout=30) as response:
        final_url = response.geturl()
        if response.status != 200 or urlsplit(final_url).hostname != 'www.gov.br':
            raise ValueError('Unexpected manual response or redirect')
        body = response.read(MAX_BYTES + 1)
        if len(body) > MAX_BYTES or not body.startswith(b'%PDF-'):
            raise ValueError('Manual is too large or not a PDF')
        content_type = response.headers.get('Content-Type', '')
    with snapshot.open('xb') as stream:
        stream.write(body)
    result = {
        'url': final_url, 'received_utc': datetime.now(timezone.utc).isoformat(),
        'http_status': 200, 'content_type': content_type, 'bytes': len(body),
        'sha256': hashlib.sha256(body).hexdigest(), 'snapshot': snapshot.name,
    }
    receipt.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
