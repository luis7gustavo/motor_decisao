"""Read-only identity and inventory check for the preserved C2 target."""

import json
import os
from pathlib import Path
import re
import hashlib

import psycopg
from sqlalchemy import create_engine, text


ROOT = Path('/evidence/c2/c2-20261013')


def main():
    record = json.loads((ROOT / 'target-credentials.json').read_text())
    report = json.loads(Path('/evidence/20261020T152131Z_e6215d7c.json').read_text())
    principal = os.environ['PGDATABASE']
    database = record['database']
    role = record['role']
    if (principal != 'motor_decisao' or database == principal
            or not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}', database)
            or not re.fullmatch(r'radar_test_[a-f0-9]{12}', role)):
        raise ValueError('Unexpected principal or isolated target identity')
    with psycopg.connect() as admin:
        admin.execute('SET TRANSACTION READ ONLY')
        server = admin.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0]
        actual_principal = admin.execute('SELECT current_database()').fetchone()[0]
        target_oid = admin.execute('SELECT oid FROM pg_database WHERE datname=%s', (database,)).fetchone()
        role_oid = admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s', (role,)).fetchone()
        principal_revision = admin.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        principal_procurement = admin.execute("SELECT count(*) FROM pg_namespace WHERE nspname='procurement'").fetchone()[0]
        postgres_version = admin.execute('SHOW server_version').fetchone()[0]
    if (server != report['server'] or actual_principal != principal or target_oid is None
            or target_oid[0] != record['database_oid']
            or target_oid[0] != report['database_oid'] or role_oid is None
            or role_oid[0] != report['role_oid']):
        raise ValueError('Server, principal, or target OID drift')
    ledger = json.loads((ROOT / 'ledger.json').read_text())
    job_record = json.loads((ROOT / 'collection-job.json').read_text())
    job_id = job_record['job_id']
    attempts = ledger['attempts']
    if (ledger['session'] != 'c2-20261013' or ledger['consumed'] != len(attempts)
            or [a['attempt'] for a in attempts] != list(range(1, len(attempts) + 1))):
        raise ValueError('C2 ledger sequence or count drift')
    for category in ('documentation', 'data'):
        if ledger['counters'][category]['consumed'] != sum(a['category'] == category for a in attempts):
            raise ValueError('C2 category count drift')
    for attempt in attempts:
        if 'body_file' in attempt:
            body = (ROOT / attempt['body_file']).read_bytes()
            if (len(body) != attempt['bytes']
                    or hashlib.sha256(body).hexdigest() != attempt['body_sha256']):
                raise ValueError('C2 receipt hash or size drift')
    with psycopg.connect(dbname=database) as admin_target:
        admin_target.execute('SET TRANSACTION READ ONLY')
        revision = admin_target.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        counts = {name: admin_target.execute('SELECT count(*) FROM ' + name).fetchone()[0]
                  for name in ('control.ingestion_jobs', 'bronze.pncp_responses',
                               'procurement.procurements', 'procurement.procurement_versions',
                               'procurement.version_observations')}
        job = admin_target.execute('''SELECT state,error_code,next_page,request_count,records_seen,checkpoint
            FROM control.ingestion_jobs WHERE id=%s''', (job_id,)).fetchone()
        bronze_hashes = {row[0] for row in admin_target.execute(
            'SELECT body_sha256 FROM bronze.pncp_responses').fetchall()}
    data_hashes = {a['body_sha256'] for a in attempts
                   if a['category'] == 'data' and 'body_sha256' in a}
    if (job is None or not data_hashes.issubset(bronze_hashes)
            or job[5].get('scope') != job_record['scope']
            or job[5].get('session') != ledger['session']):
        raise ValueError('C2 job checkpoint or Bronze receipts drift')
    engine = create_engine(record['url'], connect_args={'connect_timeout': 10})
    try:
        with engine.connect() as conn:
            conn.execute(text('SET TRANSACTION READ ONLY'))
            identity = conn.execute(text('''SELECT current_database(), current_user,
                (SELECT oid FROM pg_database WHERE datname=current_database()),
                (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)''')).one()
    finally:
        engine.dispose()
    if tuple(identity) != (database, role, target_oid[0], False):
        raise ValueError('Target database or non-superuser role drift')
    print(json.dumps({'server_matches_preserved_report': True,
                      'postgres_version': postgres_version,
                      'principal': {'database': principal, 'revision': principal_revision,
                                    'procurement_schema_count': principal_procurement},
                      'isolated_target': {'database': database, 'oid': target_oid[0],
                                          'non_superuser': True, 'revision': revision,
                                          'counts': counts},
                      'c2': {'job_id': job_id, 'job_state': job[0], 'error_code': job[1],
                             'next_page': job[2], 'request_count': job[3],
                             'records_seen': job[4], 'ledger_attempts': len(attempts),
                             'documentation_attempts': ledger['counters']['documentation']['consumed'],
                             'data_attempts': ledger['counters']['data']['consumed'],
                'all_c2_bronze_receipts_present': True,
                'additional_bronze_hashes': len(bronze_hashes - data_hashes)}}, sort_keys=True))


if __name__ == '__main__':
    main()
