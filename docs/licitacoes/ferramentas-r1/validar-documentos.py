"""Auxiliar documental R1; stdlib, sem imports da aplicação e sem rede."""
from pathlib import Path
import hashlib, json, re, sys, time, signal, platform, os
from datetime import datetime, timezone
signal.alarm(120)
start=time.monotonic()
root=Path('/input'); out=Path('/output'); mode=sys.argv[1]
files=sorted(p for p in root.rglob('*') if p.is_file() and p.suffix in ('.md','.puml') and 'ferramentas-r1/entradas' not in str(p))
texts={str(p.relative_to(root)):p.read_text(encoding='utf-8-sig') for p in files}
hashes={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
report={'date_utc':datetime.now(timezone.utc).isoformat(),'mode':mode,'python':platform.python_version(),'architecture':platform.machine(),'uid':os.getuid(),'inputs_sha256':hashes,'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
if mode=='baseline':
    report['preserved']={k:v for k,v in texts.items() if k.startswith(('09-','12-','diagramas/D10'))}
else:
    errors=[]; links=0
    def slug(s):
        s=re.sub(r'[*`]','',s.strip().lower())
        return re.sub(r'[^\w\- ]','',s).replace(' ','-')
    for name,t in texts.items():
        if name.endswith('.md') and len(re.findall(r'^```',t,re.M))%2: errors.append([name,'cercas desequilibradas'])
        for dest in re.findall(r'\[[^\]\n]+\]\(([^)]+)\)',t):
            if re.match(r'[a-z]+:',dest): continue
            dest=dest.strip('<>'); path,sep,anchor=dest.partition('#')
            target=(root/name).parent/path if path else root/name
            if not target.exists(): errors.append([name,'link ausente',dest]); continue
            links+=1
            if anchor and target.suffix=='.md':
                headings=re.findall(r'^#{1,6} (.+)$',target.read_text(encoding='utf-8-sig'),re.M)
                if anchor not in [slug(h) for h in headings]: errors.append([name,'ancora nao resolvida pelo verificador limitado',dest])
    rf=texts['04-requisitos-funcionais.md']
    fields=['Identificador','Nome','Descrição','Ator interessado','Entradas','Regras de processamento','Saídas','Dependências','Prioridade MoSCoW','Fase de implementação','Critérios de aceitação verificáveis']
    blocks=re.split(r'^### (RF-\d{3})[^\n]*\n',rf,flags=re.M)
    for i in range(1,len(blocks),2):
        for field in fields:
            if f'**{field}:**' not in blocks[i+1]: errors.append([blocks[i],'campo RF ausente',field])
    report['rf_definitions']=len(blocks)//2
    must=[]
    for i in range(1,len(blocks),2):
        if re.search(r'\*\*Prioridade MoSCoW:\*\* Must',blocks[i+1]): must.append(blocks[i])
    report['must_count']=len(must)
    for ident in must:
        for filename in ['00-prd.md','00-trd.md','17-matriz-rastreabilidade.md']:
            # Requisitos Must aparecem explicitamente nas linhas normativas originais.
            if ident not in texts[filename]: errors.append([ident,'Must sem referencia explicita',filename])
    rnfblocks=re.split(r'^### (RNF-\d{3})[^\n]*\n',texts['05-requisitos-nao-funcionais.md'],flags=re.M)
    for i in range(1,len(rnfblocks),2):
        for field in ['**Status:**','**Prioridade:**','**Fase inicial:**','**Rastreabilidade:**','**Verificação:**']:
            if field not in rnfblocks[i+1]: errors.append([rnfblocks[i],'campo RNF ausente',field])
        if 'Meta' not in rnfblocks[i+1]: errors.append([rnfblocks[i],'meta RNF ausente'])
    rtrows=re.findall(r'^\| (RT-\d{3}) \| (.+)$',texts['00-trd.md'],re.M)
    report['rt_definitions']=len(rtrows)
    if len({x[0] for x in rtrows})!=len(rtrows): errors.append(['RT','definicoes duplicadas'])
    for ident,row in rtrows:
        cells=[x.strip() for x in row.split('|') if x.strip()]
        if len(cells)!=4 or not re.search(r'R(?:F|NF|N)-\d',cells[1]) or 'TST-' not in cells[2]: errors.append([ident,'campos RT/origem/verificacao incompletos'])
    report['planned_tests']=len(set(re.findall(r'^\| (TST-\d{3})',texts['14-plano-de-testes.md'],re.M)))
    for family,filename,pattern in [('RNF','05-requisitos-nao-funcionais.md',r'^### (RNF-\d+)'),('RN','06-regras-de-negocio.md',r'^### (RN-\d+)'),('ADR','19-decisoes-arquiteturais.md',r'^## (ADR-\d+)')]:
        ids=re.findall(pattern,texts[filename],re.M)
        report[family+'_definitions']=len(ids)
        if len(ids)!=len(set(ids)): errors.append([family,'definicoes duplicadas'])
    baseline=json.loads((out/'baseline.json').read_text())
    report['changed_since_baseline']=[k for k,v in hashes.items() if k in baseline['inputs_sha256'] and v!=baseline['inputs_sha256'][k]]
    report['new_since_baseline']=[k for k in hashes if k not in baseline['inputs_sha256']]
    report['missing_since_baseline']=[k for k in baseline['inputs_sha256'] if k not in hashes]
    if report['missing_since_baseline']: errors.append(['baseline','arquivos anteriores ausentes',report['missing_since_baseline']])
    definitions={}
    for family,filename,pattern in [('RF','04-requisitos-funcionais.md',r'^### (RF-\d+)'),('RNF','05-requisitos-nao-funcionais.md',r'^### (RNF-\d+)'),('RN','06-regras-de-negocio.md',r'^### (RN-\d+)'),('ADR','19-decisoes-arquiteturais.md',r'^## (ADR-\d+)'),('RT','00-trd.md',r'^\| (RT-\d+)'),('CP','00-prd.md',r'^\| (CP-\d+)'),('TST','14-plano-de-testes.md',r'^\| (TST-\d+)'),('HU','15-roadmap-backlog.md',r'^####? (HU-\d+)')]:
        definitions[family]=set(re.findall(pattern,texts[filename],re.M))
    for name,t in texts.items():
        if name.startswith(('amostras/','ferramentas-r1/','_progresso')): continue
        for ident in set(re.findall(r'\b(?:RF|RNF|RN|ADR|RT|CP|TST|HU)-\d{2,3}\b',t)):
            family=ident.split('-')[0]
            if ident not in definitions[family]: errors.append([name,'ID sem definicao',ident])
    for k,v in baseline['preserved'].items():
        # Cabeçalho de revisão pode ser acrescentado; corpo anterior deve continuar íntegro.
        current=texts[k]
        body=v[v.find('\n\n')+2:]
        if body not in current: errors.append([k,'corpo original alterado; revisar manualmente'])
    report.update(links_checked=links,errors=errors,limitations=['Sem validador de semantica de negocio','Sem testes da aplicacao','Ancoras GitHub aproximadas; leitura humana complementar','Dependencias condicionais revisadas manualmente'])
    mdir=out/'mermaid'; mdir.mkdir(exist_ok=True)
    manifest=[]
    for name,t in texts.items():
        if not name.startswith('diagramas/'): continue
        for i,source in enumerate(re.findall(r'```mermaid\s*\n(.*?)```',t,re.S),1):
            filename=Path(name).stem+'-'+str(i)+'.mmd'
            (mdir/filename).write_text(source,encoding='utf8',newline='\n'); manifest.append({'source':name,'file':filename})
    (mdir/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    rendered=out/'renderizados/mermaid-report.json'
    if rendered.exists():
        prior=json.loads(rendered.read_text())
        for item in prior['results']:
            source=mdir/item['file']
            if not source.exists() or hashlib.sha256(source.read_bytes()).hexdigest()!=item['source_sha256'] or item['code']!=0:
                errors.append([item['file'],'render ausente, falho ou fonte mudou'])
        report['rendered_sources_unchanged']=len(prior['results'])
    for name in ['validacao-documental.json','diagramas/validacao-mermaid.json']:
        json.loads((root/name).read_text(encoding='utf-8-sig'))
report['duration_seconds']=round(time.monotonic()-start,3)
(out/(mode+'.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps({k:v for k,v in report.items() if k not in ('inputs_sha256','preserved')},ensure_ascii=False))
sys.exit(bool(report.get('errors')))

