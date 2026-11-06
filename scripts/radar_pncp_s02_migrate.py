"""Apply S02 only to the preserved C2 database after verified full backup/restore."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from datetime import datetime, timezone

import psycopg
from psycopg import sql
from sqlalchemy.engine import URL


ROOT = Path('/evidence/c2/c2-20261013')
PRIOR = Path('/evidence/20261020T152131Z_e6215d7c.json')
TABLES = ('control.ingestion_jobs', 'bronze.pncp_responses',
          'procurement.procurements', 'procurement.procurement_versions',
          'procurement.version_observations')
NEW_TABLES = ('source_budgets', 'source_missions', 'source_partitions',
              'source_partition_identities', 'source_watermarks', 'source_queue',
              'source_attempts', 'source_attempt_outcomes')
INDEXES = ('ix_source_missions_budget', 'ix_source_partitions_mission_state',
           'ix_source_partitions_parent', 'ix_source_partition_identities_raw',
           'ix_source_queue_ready', 'ix_source_queue_partition',
           'ix_source_attempts_mission', 'ix_source_attempts_job',
           'ix_source_attempts_active', 'ix_source_attempt_outcomes_bronze')


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def snapshot(connection):
    return {table: connection.execute('SELECT count(*) FROM ' + table).fetchone()[0]
            for table in TABLES}


def main():
    credentials = json.loads((ROOT / 'target-credentials.json').read_text())
    prior = json.loads(PRIOR.read_text())
    backup = json.loads((ROOT / 's02-backup-report.json').read_text())
    target = credentials['database']
    role = credentials['role']
    if (os.environ['PGDATABASE'] != 'motor_decisao'
            or not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}', target)
            or target == os.environ['PGDATABASE']
            or backup['target'] != target or not backup['protected_counts_equal']
            or not backup['rehearsal_cleaned'] or backup['full_restore_exit_code'] != 0):
        raise ValueError('S02 requires the preserved isolated C2 and verified restoration')
    dump = ROOT / backup['backup_name']
    if dump.stat().st_size != backup['backup_bytes'] or sha256(dump) != backup['backup_sha256']:
        raise ValueError('Verified S02 dump changed')
    report_path = ROOT / 's02-migration-report.json'
    if report_path.exists():
        raise FileExistsError('S02 migration report exists; inspect before another attempt')
    admin = psycopg.connect(autocommit=True)
    try:
        server = admin.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0]
        oid = admin.execute('SELECT oid FROM pg_database WHERE datname=%s', (target,)).fetchone()
        role_oid = admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s', (role,)).fetchone()
        principal_revision = admin.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        if (server != prior['server'] or oid != (prior['database_oid'],)
                or oid != (credentials['database_oid'],)
                or role_oid != (prior['role_oid'],)
                or principal_revision != '20260817_0009'):
            raise ValueError('Protected server, database, role, or principal revision drift')
        with psycopg.connect(dbname=target) as connection:
            before_revision = connection.execute('SELECT version_num FROM alembic_version').fetchone()[0]
            before_counts = snapshot(connection)
        if before_counts != backup['protected_counts']:
            raise ValueError('C2 protected counts drifted after backup')
        if before_revision not in ('20261013_0010', '20261027_0011'):
            raise ValueError('Unexpected C2 revision')
        if before_revision == '20261013_0010':
            env = os.environ.copy()
            env['DATABASE_URL'] = URL.create(
                'postgresql+psycopg', username=env['PGUSER'], password=env['PGPASSWORD'],
                host=env['PGHOST'], port=int(env['PGPORT']), database=target,
            ).render_as_string(hide_password=False)
            result = subprocess.run([sys.executable, '-m', 'alembic', '-c', 'alembic.ini',
                                     'upgrade', '20261027_0011'], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode:
                raise RuntimeError('Isolated migration failed; inspect database revision before retry')
        with psycopg.connect(dbname=target) as connection:
            if connection.execute('SELECT version_num FROM alembic_version').fetchone()[0] != '20261027_0011':
                raise ValueError('S02 revision not installed')
            if snapshot(connection) != backup['protected_counts']:
                raise ValueError('Historical counts changed during S02 migration')
            for table in NEW_TABLES:
                qualified = sql.SQL('control.{}').format(sql.Identifier(table))
                if connection.execute(sql.SQL('SELECT count(*) FROM {}').format(qualified)).fetchone()[0]:
                    raise ValueError('New S02 control table unexpectedly contains rows')
                connection.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE ON {} TO {}').format(
                    qualified, sql.Identifier(role)))
            for index in INDEXES:
                valid = connection.execute("""SELECT i.indisvalid AND i.indisready
                    FROM pg_index i WHERE i.indexrelid=to_regclass(%s)""",
                    (f'control.{index}',)).fetchone()
                if valid != (True,):
                    raise ValueError('S02 control index missing or invalid')
            bad_fk = connection.execute("""SELECT count(*) FROM pg_constraint c
                JOIN pg_class t ON t.oid=c.conrelid
                JOIN pg_namespace n ON n.oid=t.relnamespace
                WHERE n.nspname='control' AND t.relname=ANY(%s)
                AND c.contype='f' AND (c.confdeltype<>'r' OR NOT c.convalidated)""",
                (list(NEW_TABLES),)).fetchone()[0]
            if bad_fk:
                raise ValueError('S02 foreign key policy or validation failed')
            allowed = {f'control.{table}' for table in NEW_TABLES}
            allowed |= {'control.pipeline_runs', 'control.ingestion_jobs',
                        'bronze.pncp_responses', 'commercial.system_settings',
                        'procurement.procurements', 'procurement.procurement_versions',
                        'procurement.procurement_items', 'procurement.judgment_units',
                        'procurement.judgment_unit_items', 'procurement.version_observations'}
            grants = connection.execute("""SELECT n.nspname||'.'||c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')
                AND has_table_privilege(%s,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')""",
                (role,)).fetchall()
            outside = sorted({name for (name,) in grants} - allowed)
            if outside:
                raise ValueError('Ingestion role has privileges outside isolated allowlist')
            for table in NEW_TABLES:
                qualified = f'control.{table}'
                for privilege in ('SELECT', 'INSERT', 'UPDATE'):
                    if not connection.execute('SELECT has_table_privilege(%s,%s,%s)',
                                              (role, qualified, privilege)).fetchone()[0]:
                        raise ValueError('Restricted role lacks S02 control privilege')
        principal_grants = admin.execute("""SELECT count(*) FROM pg_class c
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND c.relkind IN ('r','p')
            AND has_table_privilege(%s,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')""",
            (role,)).fetchone()[0]
        if principal_grants:
            raise ValueError('Restricted role can access principal tables')
        if admin.execute('SELECT version_num FROM alembic_version').fetchone()[0] != principal_revision:
            raise ValueError('Principal revision changed')
        report = {'created_utc': datetime.now(timezone.utc).isoformat(),
                  'target': target, 'target_oid': oid[0], 'role_oid': role_oid[0],
                  'before_revision': before_revision, 'after_revision': '20261027_0011',
                  'principal_revision': principal_revision,
                  'protected_counts_unchanged': True, 'protected_counts': before_counts,
                  'new_tables_empty': list(NEW_TABLES),
                  'indexes_valid': list(INDEXES), 'foreign_keys_validated': True,
                  'role_grants_verified': True, 'role_principal_table_privileges': principal_grants,
                  'backup_sha256': backup['backup_sha256']}
        report_path.write_text(json.dumps(report, indent=2) + '\n')
        report_path.chmod(0o600)
        print(json.dumps(report, sort_keys=True))
    finally:
        admin.close()


if __name__ == '__main__':
    main()
