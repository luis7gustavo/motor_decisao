"""One explicit official-document request through the persistent session quota."""
import argparse
import hashlib
import json
from pathlib import Path

from app.procurement.pncp_transport import Budget


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--session',required=True)
    parser.add_argument('--init',action='store_true')
    parser.add_argument('--url')
    args = parser.parse_args()
    path = Path('/evidence/c2') / args.session
    if path.parent != Path('/evidence/c2') or not args.session.startswith('c2-'):
        raise ValueError('Invalid session')
    if args.init:
        Budget.create(path,session_id=args.session,limit=50,interval=1)
    budget = Budget(path)
    if args.url:
        try:
            result = budget.request(args.url)
            print(json.dumps({'status':result.status,'attempt':result.attempt,'bytes':len(result.body),
                'sha256':hashlib.sha256(result.body).hexdigest(),'headers':result.headers}))
        except Exception as exc:
            print(json.dumps({'failure':type(exc).__name__,'consumed':budget.state()['consumed']}))
            return 1
    else:
        state = budget.state()
        print(json.dumps({'session':state['session'],'consumed':state['consumed'],'remaining':state['limit']-state['consumed']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
