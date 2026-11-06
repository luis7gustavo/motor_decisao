"""Rehearse a full C2 restore, then apply the additive S04 revision there only."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys

import psycopg
from psycopg import sql
from sqlalchemy.engine import URL


ROOT = Path('/evidence/c2/c2-20261013')
PRIOR = Path('/evidence/20261020T152131Z_e6215d7c.json')
TABLES = (
    'control.ingestion_jobs', 'bronze.pncp_responses',
    'procurement.procurements', 'procurement.procurement_versions',
    'procurement.version_observations', 'control.source_budgets',
    'control.source_missions', 'control.source_partitions',
    'control.source_partition_identities', 'control.source_watermarks',
    'control.source_queue', 'control.source_attempts',
    'control.source_attempt_outcomes',
)


def snapshot(connection):
    return {
        'revision': connection.execute('SELECT version_num FROM alembic_version').fetchone()[0],
        'counts': {table: connection.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                   for table in TABLES},
    }


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    credentials = json.loads((ROOT / 'target-credentials.json').read_text())
    prior = json.loads(PRIOR.read_text())
    s02 = json.loads((ROOT / 's02-migration-report.json').read_text())
    target = credentials['database']
    principal = os.environ['PGDATABASE']
    if (principal != 'motor_decisao'
            or not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}', target)
            or target == principal or s02['after_revision'] != '20261027_0011'):
        raise ValueError('S04 requires the protected isolated C2 target')
    dump = ROOT / 's04-before-0012.dump'
    report_path = ROOT / 's04-migration-report.json'
    if dump.exists() or report_path.exists():
        raise FileExistsError('S04 evidence exists; inspect before retry')
    admin = psycopg.connect(autocommit=True)
    rehearsal = None
    rehearsal_oid = None
    try:
        server = admin.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0]
        target_oid = admin.execute('SELECT oid FROM pg_database WHERE datname=%s', (target,)).fetchone()
        role_oid = admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s',
                                 (credentials['role'],)).fetchone()
        principal_revision = admin.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        principal_has_procurement = admin.execute(
            "SELECT to_regnamespace('procurement') IS NOT NULL").fetchone()[0]
        if (server != prior['server'] or target_oid != (prior['database_oid'],)
                or target_oid != (credentials['database_oid'],)
                or role_oid != (prior['role_oid'],)
                or principal_revision != '20260817_0009' or principal_has_procurement):
            raise ValueError('Principal, server, target, or role drift')
        with psycopg.connect(dbname=target) as source:
            source.execute('SET TRANSACTION READ ONLY')
            before = snapshot(source)
        if (before['revision'] != '20261027_0011'
                or any(before['counts'][name] != count for name, count in
                       s02['protected_counts'].items())
                or any(before['counts']['control.' + name] != 0 for name in
                       s02['new_tables_empty'])):
            raise ValueError('C2 revision or protected counts drift')
        subprocess.run(['pg_dump', '--format=custom', '--no-owner',
                        '--file', str(dump), '--dbname', target],
                       check=True, stdout=subprocess.DEVNULL)
        if dump.stat().st_size == 0:
            raise ValueError('Empty S04 backup')
        toc = subprocess.run(['pg_restore', '--list', str(dump)], check=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode()
        if 'TABLE DATA' not in toc or 'alembic_version' not in toc:
            raise ValueError('S04 backup TOC lacks data or revision')
        rehearsal = 'rehearsal_' + datetime.now(timezone.utc).strftime('%Y%m%d') + '_' + secrets.token_hex(6)
        if admin.execute('SELECT 1 FROM pg_database WHERE datname=%s', (rehearsal,)).fetchone():
            raise ValueError('Rehearsal database collision')
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(rehearsal)))
        rehearsal_oid = admin.execute('SELECT oid FROM pg_database WHERE datname=%s',
                                      (rehearsal,)).fetchone()[0]
        subprocess.run(['pg_restore', '--exit-on-error', '--single-transaction',
                        '--no-owner', '--dbname', rehearsal, str(dump)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        with psycopg.connect(dbname=rehearsal) as restored:
            restored.execute('SET TRANSACTION READ ONLY')
            if snapshot(restored) != before:
                raise ValueError('Full restore differs from protected C2 snapshot')
        if dump.stat().st_size < 1000:
            raise ValueError('S04 backup unexpectedly small')
        backup_hash = sha256(dump)
        url = URL.create('postgresql+psycopg', username=os.environ['PGUSER'],
                         password=os.environ['PGPASSWORD'], host=os.environ['PGHOST'],
                         port=int(os.environ['PGPORT']), database=target)
        env = os.environ.copy()
        env['DATABASE_URL'] = url.render_as_string(hide_password=False)
        result = subprocess.run([sys.executable, '-m', 'alembic', '-c', 'alembic.ini',
                                 'upgrade', '20261027_0012'], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode:
            raise RuntimeError('Isolated S04 migration failed; inspect C2 revision')
        with psycopg.connect(dbname=target) as migrated:
            migrated.execute('SET TRANSACTION READ ONLY')
            after = snapshot(migrated)
            columns = {row[0] for row in migrated.execute("""SELECT column_name
                FROM information_schema.columns WHERE table_schema='procurement'
                AND table_name='version_observations'""")}
            index_valid = migrated.execute("""SELECT i.indisvalid FROM pg_index i
                JOIN pg_class c ON c.oid=i.indexrelid
                WHERE c.relname='ix_observation_source_time'""").fetchone()
        if (after['revision'] != '20261027_0012' or after['counts'] != before['counts']
                or not {'source_updated_at', 'source_time_text', 'observed_resource'} <= columns
                or index_valid != (True,)):
            raise ValueError('S04 migration verification failed')
        if admin.execute('SELECT version_num FROM alembic_version').fetchone()[0] != principal_revision:
            raise ValueError('Principal revision changed')
        report = {
            'created_utc': datetime.now(timezone.utc).isoformat(),
            'target': target, 'target_oid': target_oid[0], 'role_oid': role_oid[0],
            'server_matches_preserved_report': True,
            'principal_revision': principal_revision,
            'principal_procurement_schema_absent': True,
            'before_revision': before['revision'], 'after_revision': after['revision'],
            'protected_counts_unchanged': True, 'protected_counts': after['counts'],
            'backup_name': dump.name, 'backup_bytes': dump.stat().st_size,
            'backup_sha256': backup_hash, 'pg_restore_list_verified': True,
            'full_restore_exit_code': 0, 'restored_counts_equal': True,
            'new_columns_verified': True, 'index_valid': True,
            'rehearsal_cleaned': False,
        }
    finally:
        if rehearsal is not None and rehearsal_oid is not None:
            if admin.execute('SELECT oid FROM pg_database WHERE datname=%s',
                             (rehearsal,)).fetchone() == (rehearsal_oid,):
                admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(rehearsal)))
        admin.close()
    report['rehearsal_cleaned'] = True
    report_path.write_text(json.dumps(report, separators=(',', ':')) + '\n')
    print(json.dumps(report, separators=(',', ':')))


if __name__ == '__main__':
    main()
