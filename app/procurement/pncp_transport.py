"""Bounded GET transport; session quota is shared durably by all processes.

Linux flock is intentional: runtime is Docker. No proxy, automatic redirect or
retry. Only a validated public IP is connected, with TLS hostname verification.
"""
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import http.client
import ipaddress
import json
import math
import os
from pathlib import Path
import socket
import ssl
import time
from urllib.parse import urlsplit

ALLOWED_HOSTS = frozenset({'pncp.gov.br', 'www.gov.br'})
SAFE_HEADERS = frozenset({'content-type','content-length','retry-after','date','etag','last-modified','location'})


class BudgetExhausted(RuntimeError):
    pass


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict
    body: bytes
    attempt: int | None = None
    received_at: str | None = None


def validate_url(url):
    if any(ord(char) < 32 or ord(char) == 127 for char in url):
        raise ValueError('Control characters in URL')
    parts = urlsplit(url)
    if (parts.scheme != 'https' or parts.hostname not in ALLOWED_HOSTS or
            parts.port not in (None,443) or parts.username is not None or parts.password is not None or parts.fragment):
        raise ValueError('Destination outside official HTTPS allowlist')
    return parts


def public_address(address):
    ip = ipaddress.ip_address(address)
    if not ip.is_global or ip.is_multicast or ip.is_unspecified or getattr(ip,'ipv4_mapped',None):
        raise ValueError('Nonpublic destination')
    return str(ip)


def retry_delay(value, base, *, now=None):
    if value:
        try:
            delay = float(value)
        except ValueError:
            try:
                instant = parsedate_to_datetime(value)
                delay = (instant - (now or datetime.now(timezone.utc))).total_seconds()
            except (ValueError, TypeError, OverflowError):
                delay = 0
        if math.isfinite(delay) and delay >= 0:
            return max(base, delay)
    return base


def send_get(url, *, max_bytes=4*1024*1024, timeout=30):
    parts = validate_url(url)
    addresses = socket.getaddrinfo(parts.hostname,443,type=socket.SOCK_STREAM)
    for entry in addresses:
        public_address(entry[4][0])
    if not addresses:
        raise OSError('No public destination')
    entry = sorted(addresses,key=lambda item:item[0] != socket.AF_INET)[0]
    address = public_address(entry[4][0])
    sock = socket.socket(entry[0], socket.SOCK_STREAM)
    connection = http.client.HTTPSConnection(parts.hostname,timeout=timeout)
    try:
        sock.settimeout(min(10,timeout))
        sock.connect(entry[4])
        if public_address(sock.getpeername()[0]) != address:
            raise ValueError('Connected peer mismatch')
        tls = ssl.create_default_context().wrap_socket(sock,server_hostname=parts.hostname)
        connection.sock = tls
        tls.settimeout(timeout)
        target = parts.path or '/'
        if parts.query:
            target += '?' + parts.query
        connection.request('GET',target,headers={'Accept':'application/json, text/html, application/pdf',
            'Accept-Encoding':'identity','User-Agent':'RadarLicita-validation/0.1'})
        response = connection.getresponse()
        headers = {key.lower():value for key,value in response.getheaders() if key.lower() in SAFE_HEADERS}
        length = headers.get('content-length')
        if length and int(length) > max_bytes:
            raise ValueError('Response exceeds byte limit')
        chunks, seen = [], 0
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Body deadline exceeded')
            tls.settimeout(remaining)
            chunk = response.read(min(65536,max_bytes-seen+1))
            if not chunk:
                break
            seen += len(chunk)
            if seen > max_bytes:
                raise ValueError('Response exceeds byte limit')
            chunks.append(chunk)
        return Response(response.status,headers,b''.join(chunks))
    finally:
        connection.close()
        sock.close()


def durable_write(path, body):
    temp = path.with_suffix(path.suffix+'.tmp')
    with temp.open('wb') as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp,path)
    fd = os.open(path.parent,os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Budget:
    def __init__(self,path,*,category=None):
        self.path = Path(path).resolve()
        self.category = category

    @classmethod
    def create(cls,path,*,session_id,limit,interval):
        if type(limit) is not int or limit < 0 or not math.isfinite(interval) or interval < 1:
            raise ValueError('Invalid quota/interval')
        path = Path(path)
        path.mkdir(parents=True,exist_ok=False)
        durable_write(path/'ledger.json',json.dumps({'session':session_id,'limit':limit,'interval':interval,
            'consumed':0,'not_before':0,'attempts':[]}).encode())
        (path/'session.lock').touch(exist_ok=False)

    def state(self):
        data = json.loads((self.path/'ledger.json').read_text())
        if (not isinstance(data,dict) or type(data.get('limit')) is not int
                or type(data.get('consumed')) is not int
                or not isinstance(data.get('session'),str) or not data['session']
                or type(data.get('attempts')) is not list
                or any(type(data.get(key)) not in (int,float) or not math.isfinite(data[key])
                       for key in ('interval','not_before'))
                or not 0 <= data['consumed'] <= data['limit']
                or data['consumed'] != len(data['attempts'])
                or data['interval'] < 1 or data['not_before'] < 0
                or any(not isinstance(entry,dict) or type(entry.get('attempt')) is not int
                       or entry['attempt']!=index for index,entry in enumerate(data['attempts'],1))):
            raise ValueError('Quota ledger cannot be reconciled')
        if 'counters' in data:
            counters=data['counters']
            if not isinstance(counters,dict) or set(counters)!={'documentation','data'}:
                raise ValueError('Invalid split counters')
            for category,limit in (('documentation',15),('data',50)):
                counter=counters[category]
                if (not isinstance(counter,dict) or type(counter.get('limit')) is not int
                        or counter['limit']!=limit or type(counter.get('consumed')) is not int
                        or not 0<=counter['consumed']<=limit
                        or counter['consumed']!=sum(e.get('category')==category for e in data['attempts'])):
                    raise ValueError('Split counter cannot be reconciled')
            if sum(c['consumed'] for c in counters.values())!=data['consumed'] or data['limit']!=65:
                raise ValueError('Split totals cannot be reconciled')
        return data

    def split_operator_budgets(self):
        """Explicit 2026-10-15 decision; migrate once, never reset attempts."""
        with self.lock():
            state=self.state()
            if 'counters' in state:
                return state
            if state['session']!='c2-20261013' or state['consumed']!=7:
                raise ValueError('Unexpected legacy session; manual reconciliation required')
            if any('/v1/contratacoes/' in e['url'] for e in state['attempts']):
                raise ValueError('Legacy data calls cannot be reclassified')
            archive=self.path/'ledger-before-split.json'
            if archive.exists() and archive.read_bytes()!=(self.path/'ledger.json').read_bytes():
                raise ValueError('Legacy archive mismatch')
            durable_write(archive,(self.path/'ledger.json').read_bytes())
            for entry in state['attempts']:
                entry['category']='documentation'
            state.update(limit=65,counters={'documentation':{'limit':15,'consumed':7},
                                           'data':{'limit':50,'consumed':0}})
            self.save(state)
            return self.state()

    def save(self,data):
        durable_write(self.path/'ledger.json',json.dumps(data,sort_keys=True,indent=2).encode())

    @contextmanager
    def lock(self):
        # Opening r+b refuses missing lock/state; never silently recreates budget.
        with (self.path/'session.lock').open('r+b') as stream:
            fcntl.flock(stream,fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream,fcntl.LOCK_UN)

    def request(self,url,send=send_get,*,minimum_delay=0,on_reserved=None):
        validate_url(url)
        with self.lock():
            state = self.state()
            if 'counters' in state:
                if self.category not in state['counters']:
                    raise ValueError('Explicit request category required')
                counter=state['counters'][self.category]
                if counter['consumed']>=counter['limit']:
                    raise BudgetExhausted('Category quota exhausted')
            if state['consumed'] >= state['limit']:
                raise BudgetExhausted('Session quota exhausted')
            wait = max(0,state['not_before']-time.time(),minimum_delay)
            if wait:
                time.sleep(wait)
            state['consumed'] += 1
            index = state['consumed']
            entry = {'attempt':index,'url':url,'reserved_at':datetime.now(timezone.utc).isoformat(),
                'outcome':'reserved'}
            if 'counters' in state:
                entry['category']=self.category
                state['counters'][self.category]['consumed']+=1
            state['attempts'].append(entry)
            state['not_before'] = time.time()+state['interval']
            self.save(state)  # Commit reservation BEFORE DNS/connect/send.
            try:
                if on_reserved is not None:
                    on_reserved()
                response = send(url)
                entry.update(status=response.status,headers={k.lower():v for k,v in response.headers.items()
                    if k.lower() in SAFE_HEADERS},body_file=f'{index:03d}.body',
                    body_sha256=hashlib.sha256(response.body).hexdigest(),bytes=len(response.body),
                    received_at=datetime.now(timezone.utc).isoformat())
                durable_write(self.path/entry['body_file'],response.body)
                entry['outcome'] = 'received'
                state['not_before'] = time.time()+retry_delay(entry['headers'].get('retry-after'),state['interval'])
                self.save(state)
                return replace(response,attempt=index,received_at=entry['received_at'],headers=entry['headers'])
            except BaseException as exc:
                entry['outcome'] = type(exc).__name__
                state['not_before'] = max(state['not_before'],time.time()+state['interval'])
                self.save(state)
                raise
