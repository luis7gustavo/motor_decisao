"""RF-001 / RT-001: durable quota, serial transport and source safety."""
from datetime import datetime, timezone

import pytest

from app.procurement.pncp_transport import Budget, BudgetExhausted, Response, validate_url, retry_delay


def test_budget_reserved_before_failure_and_survives_reopen(tmp_path):
    """RT-001: failed sends consume quota permanently, including restart."""
    path = tmp_path / 'session'
    Budget.create(path, session_id='test', limit=1, interval=1)
    budget = Budget(path)
    def fail(url):
        assert Budget(path).state()['consumed'] == 1
        raise TimeoutError('synthetic')
    with pytest.raises(TimeoutError):
        budget.request('https://pncp.gov.br/api/consulta/v3/api-docs', fail)
    with pytest.raises(BudgetExhausted):
        Budget(path).request('https://pncp.gov.br/api/consulta/v3/api-docs', fail)
    assert budget.state()['consumed'] == 1


def test_response_archived_before_return(tmp_path):
    """RN-026: exact bytes and safe headers are durable before transformation."""
    path = tmp_path / 'session'
    Budget.create(path, session_id='test', limit=2, interval=1)
    budget = Budget(path)
    response = budget.request('https://pncp.gov.br/api/consulta/v3/api-docs',
        lambda url: Response(200, {'content-type':'application/json','set-cookie':'secret'}, b'{"x":1}'))
    state = budget.state()
    assert state['consumed'] == 1
    assert (path / state['attempts'][0]['body_file']).read_bytes() == response.body
    assert 'set-cookie' not in state['attempts'][0]['headers']
    with pytest.raises(FileExistsError):
        Budget.create(path, session_id='test', limit=50, interval=1)


@pytest.mark.parametrize('url', ['http://pncp.gov.br/a','https://pncp.gov.br:444/a',
    'https://pncp.gov.br.evil.test/a','https://user@pncp.gov.br/a','https://127.0.0.1/a',
    'https://pncp.gov.br/a#fragment','https://pncp.gov.br/a\r\nX: y'])
def test_unapproved_destinations_rejected(url):
    """RNF-003: URL allowlist precedes any HTTP attempt."""
    with pytest.raises(ValueError):
        validate_url(url)


@pytest.mark.parametrize('address', ['127.0.0.1','10.0.0.1','169.254.169.254','::1','::ffff:127.0.0.1'])
def test_nonpublic_dns_rejected(address):
    """RNF-003: domain allowlist alone cannot authorize a private peer."""
    from app.procurement.pncp_transport import public_address
    with pytest.raises(ValueError):
        public_address(address)


def test_retry_after_and_invalid_configuration(tmp_path):
    """RT-001: server delay is never capped below Retry-After."""
    assert retry_delay('120', 2) == 120
    assert retry_delay('bad', 4) == 4
    assert retry_delay('Tue, 13 Oct 2026 12:02:00 GMT', 2,
        now=datetime(2026,10,13,12,0,tzinfo=timezone.utc)) == 120
    with pytest.raises(ValueError):
        Budget.create(tmp_path/'bad',session_id='test',limit=-1,interval=1)
    with pytest.raises(ValueError):
        Budget.create(tmp_path/'bad2',session_id='test',limit=2,interval=0.5)
    with pytest.raises(FileNotFoundError):
        Budget(tmp_path/'missing').state()


def test_shared_ledger_serializes_workers_and_spacing(tmp_path):
    """RT-001: independent clients share quota and cannot overlap sends."""
    from concurrent.futures import ThreadPoolExecutor
    import time
    path=tmp_path/'shared'
    Budget.create(path,session_id='test',limit=2,interval=1)
    starts,ends=[],[]
    def send(url):
        starts.append(time.monotonic())
        time.sleep(0.02)
        ends.append(time.monotonic())
        return Response(200,{},b'{}')
    def run(_):
        return Budget(path).request('https://pncp.gov.br/api/consulta/v3/api-docs',send)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(run,range(2)))
    assert starts[1]>=ends[0]+0.9
    assert Budget(path).state()['consumed']==2


def test_connected_peer_is_rechecked_after_dns(monkeypatch):
    """RNF-003: connecting a different/private peer fails before HTTP/TLS."""
    from app.procurement import pncp_transport as module
    class Socket:
        def settimeout(self,_): pass
        def connect(self,address): self.address=address
        def getpeername(self): return ('127.0.0.1',443)
        def close(self): pass
    sock=Socket()
    monkeypatch.setattr(module.socket,'getaddrinfo',lambda *args,**kwargs:[
        (module.socket.AF_INET,module.socket.SOCK_STREAM,6,'',('8.8.8.8',443))])
    monkeypatch.setattr(module.socket,'socket',lambda *args,**kwargs:sock)
    with pytest.raises(ValueError):
        module.send_get('https://pncp.gov.br/api/consulta/v3/api-docs')
    assert sock.address==('8.8.8.8',443)


@pytest.mark.parametrize('field,value', [
    ('limit', True), ('consumed', False), ('interval', float('nan')),
    ('not_before', float('nan')), ('not_before', float('inf')),
    ('session', ''), ('attempts', {}),
])
def test_corrupt_budget_refuses_before_send(tmp_path,field,value):
    """RT-001: corrupted persistent state cannot silently relax quota/spacing."""
    import json
    path=tmp_path/'corrupt'
    Budget.create(path,session_id='test',limit=2,interval=1)
    state=Budget(path).state()
    state[field]=value
    (path/'ledger.json').write_text(json.dumps(state))
    sent=[]
    with pytest.raises(ValueError):
        Budget(path).request('https://pncp.gov.br/api/consulta/v3/api-docs',
            lambda url: sent.append(url))
    assert sent==[]


def test_attempt_sequence_must_reconcile_before_resume(tmp_path):
    """RT-001: duplicate receipt numbers cannot overwrite an earlier archive."""
    import json
    path=tmp_path/'sequence'
    Budget.create(path,session_id='test',limit=3,interval=1)
    budget=Budget(path)
    budget.request('https://pncp.gov.br/api/consulta/v3/api-docs',
        lambda _:Response(200,{},b'original'))
    state=budget.state()
    state['attempts'][0]['attempt']=2
    (path/'ledger.json').write_text(json.dumps(state))
    with pytest.raises(ValueError):
        budget.state()
    assert (path/'001.body').read_bytes()==b'original'
