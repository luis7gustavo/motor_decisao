"""Offline receipt reconciliation independent of C2's current sample size."""

import hashlib
import json

from scripts.radar_pncp_live_validation import assess_session_evidence


def evidence(pages):
    scope={'start':'2026-10-06','end':'2026-10-14','modality':6}
    responses=[]
    attempts=[]
    records=0
    for number, count in enumerate(pages, 1):
        body=json.dumps({'data':[{'numeroControlePNCP':f'id-{number}-{i}'} for i in range(count)],
                         'numeroPagina':number,'totalRegistros':sum(pages),
                         'totalPaginas':len(pages),'paginasRestantes':len(pages)-number}).encode()
        digest=hashlib.sha256(body).hexdigest()
        responses.append({'id':f'raw-{number}','job_id':'job-1','page':number,
                          'http_status':200,'body':body,'byte_length':len(body),
                          'body_sha256':digest})
        attempts.append({'attempt':number,'category':'data','status':200,
                         'body_sha256':digest,'bytes':len(body)})
        records+=count
    job={'id':'job-1','state':'success','error_code':None,'next_page':len(pages)+1,
         'request_count':len(pages),'records_seen':records,
         'checkpoint':{'session':'c2-20261013','scope':scope,'processed':records,
                       'seen':[f'hash-{i}' for i in range(records)]}}
    ledger={'session':'c2-20261013','attempts':attempts}
    return ledger,{'job_id':'job-1','scope':scope},[job],responses,{'job-1':records}


def test_verifier_accepts_reconciled_growth_without_fixed_response_count():
    for page_counts in ([2],[2,1],[2,1,3,2]):
        issues,warnings,public=assess_session_evidence(*evidence(page_counts))
        assert issues==[] and warnings==[]
        assert len(public)==len(page_counts)


def test_verifier_rejects_changed_bronze_bytes_and_missing_observation():
    ledger,job_file,jobs,responses,observations=evidence([2,1,3,2])
    responses[2]['body']=b'corrupted'
    observations['job-1']-=1
    issues,_,_=assess_session_evidence(ledger,job_file,jobs,responses,observations)
    assert 'bronze_body_hash_or_size_mismatch' in issues
    assert 'job_observation_count_mismatch' in issues


def test_verifier_rejects_advanced_cursor_without_received_page():
    ledger,job_file,jobs,responses,observations=evidence([2,1])
    jobs[0]['next_page']=4
    issues,_,_=assess_session_evidence(ledger,job_file,jobs,responses,observations)
    assert 'advanced_cursor_without_bronze_page' in issues
