"""Ensaio de mounts/permissões, sem rede e sem aplicação."""
from pathlib import Path
import os, hashlib, json, time, signal
signal.alarm(30)
t=time.monotonic()
r={'uid':os.getuid(),'input_utf8':Path('/input/texto.txt').read_text(encoding='utf-8-sig').strip(),'read_only_checks':{},'scripts':{}}
for p in ['/input/escrita-proibida.txt','/tool/escrita-proibida.txt','/escrita-proibida.txt']:
    try:
        Path(p).write_text('unexpected')
        r['read_only_checks'][p]=False
    except OSError:
        r['read_only_checks'][p]=True
for p in Path('/tool').glob('*'):
    if p.suffix in ('.py','.cjs'):
        b=p.read_bytes();r['scripts'][p.name]={'sha256':hashlib.sha256(b).hexdigest(),'lf_only':b'\r' not in b}
Path('/output/escrita-uid-65534.txt').write_text('saída controlada; espaços e acentos',encoding='utf8')
r['duration_seconds']=round(time.monotonic()-t,3)
r['ok']=r['uid']==65534 and all(r['read_only_checks'].values()) and all(x['lf_only'] for x in r['scripts'].values())
Path('/output/portabilidade.json').write_text(json.dumps(r,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(r,ensure_ascii=False))
raise SystemExit(0 if r['ok'] else 1)
