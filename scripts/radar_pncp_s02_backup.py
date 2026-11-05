"""Backup and full restore rehearsal of the preserved isolated C2 database."""

import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
from datetime import datetime, timezone

import psycopg
from psycopg import sql


ROOT = Path('/evidence/c2/c2-20261013')
REPORT = Path('/evidence/20261020T152131Z_e6215d7c.json')
TABLES = ('control.ingestion_jobs', 'bronze.pncp_responses',
          'procurement.procurements', 'procurement.procurement_versions',
          'procurement.version_observations')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def snapshot(connection):
    revision=connection.execute('SELECT version_num FROM alembic_version').fetchone()[0]
    counts={table: connection.execute('SELECT count(*) FROM '+table).fetchone()[0]
            for table in TABLES}
    return {'revision':revision,'counts':counts}


def main():
    record=json.loads((ROOT/'target-credentials.json').read_text())
    prior=json.loads(REPORT.read_text())
    target=record['database']
    principal=os.environ['PGDATABASE']
    if (principal!='motor_decisao' or not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}',target)
            or target==principal):
        raise ValueError('Unexpected principal or isolated target')
    admin=psycopg.connect(autocommit=True)
    restored=None
    restored_oid=None
    dump=ROOT/'s02-before-0011.dump'
    report_file=ROOT/'s02-backup-report.json'
    try:
        server=admin.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0]
        target_oid=admin.execute('SELECT oid FROM pg_database WHERE datname=%s',(target,)).fetchone()
        role=admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s',(record['role'],)).fetchone()
        principal_revision=admin.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        if (server!=prior['server'] or target_oid is None or target_oid[0]!=prior['database_oid']
                or target_oid[0]!=record['database_oid'] or role is None
                or role[0]!=prior['role_oid'] or principal_revision!='20260817_0009'):
            raise ValueError('Protected server, role, target, or principal revision drift')
        with psycopg.connect(dbname=target) as source:
            source.execute('SET TRANSACTION READ ONLY')
            before=snapshot(source)
        if before['revision']!='20261013_0010':
            raise ValueError('C2 target must remain at C1 before backup')
        if dump.exists() or report_file.exists():
            raise FileExistsError('S02 backup already exists; never overwrite evidence')
        subprocess.run(['pg_dump','--format=custom','--no-owner','--file',str(dump),
                        '--dbname',target],check=True,stdout=subprocess.DEVNULL)
        if dump.stat().st_size==0:
            raise ValueError('Empty backup')
        toc=subprocess.run(['pg_restore','--list',str(dump)],check=True,
                           stdout=subprocess.PIPE,stderr=subprocess.PIPE).stdout.decode()
        if 'TABLE DATA' not in toc or 'alembic_version' not in toc:
            raise ValueError('Backup TOC lacks data or revision table')
        restored='rehearsal_'+datetime.now(timezone.utc).strftime('%Y%m%d')+'_'+secrets.token_hex(6)
        if admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(restored,)).fetchone():
            raise ValueError('Unexpected rehearsal database collision')
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(restored)))
        restored_oid=admin.execute('SELECT oid FROM pg_database WHERE datname=%s',(restored,)).fetchone()[0]
        subprocess.run(['pg_restore','--exit-on-error','--single-transaction','--no-owner',
                        '--dbname',restored,str(dump)],check=True,stdout=subprocess.DEVNULL,
                       stderr=subprocess.PIPE)
        with psycopg.connect(dbname=restored) as copy:
            copy.execute('SET TRANSACTION READ ONLY')
            after=snapshot(copy)
        if after!=before:
            raise ValueError('Full restore revision or protected table counts mismatch')
        report={'created_utc':datetime.now(timezone.utc).isoformat(),
                'principal':principal,'principal_revision':principal_revision,
                'target':target,'target_oid':target_oid[0],
                'backup_name':dump.name,'backup_bytes':dump.stat().st_size,
                'backup_sha256':digest(dump),'pg_restore_list_verified':True,
                'full_restore_exit_code':0,'restored_revision':after['revision'],
                'protected_counts_equal':True,'protected_counts':after['counts'],
                'server_matches_preserved_report':True}
    finally:
        if restored is not None and restored_oid is not None:
            actual=admin.execute('SELECT oid FROM pg_database WHERE datname=%s',(restored,)).fetchone()
            if actual!=(restored_oid,):
                raise ValueError('Rehearsal database identity changed before cleanup')
            admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(restored)))
        admin.close()
    report['rehearsal_cleaned']=True
    report_file.write_text(json.dumps(report,indent=2)+'\n')
    report_file.chmod(0o600)
    print(json.dumps(report,sort_keys=True))


if __name__=='__main__':
    main()
