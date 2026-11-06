"""S06: back up isolated C2 and verify a full restore in a disposable database.

Run only through the radar-ops Docker service. This never migrates or restores
into the principal or the persistent C2 database.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess

import psycopg
from psycopg import sql


ROOT = Path('/evidence/c2/c2-20261013')
BASELINE = Path('/evidence/20261020T152131Z_e6215d7c.json')
TABLES = (
    'control.ingestion_jobs', 'bronze.pncp_responses',
    'procurement.procurements', 'procurement.procurement_versions',
    'procurement.version_observations', 'control.source_budgets',
    'control.source_missions', 'control.source_partitions',
    'control.source_partition_identities', 'control.source_watermarks',
    'control.source_queue', 'control.source_attempts',
    'control.source_attempt_outcomes',
)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def snapshot(connection):
    connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
    revision = connection.execute('SELECT version_num FROM alembic_version').fetchone()[0]
    counts = {table: connection.execute('SELECT count(*) FROM ' + table).fetchone()[0]
              for table in TABLES}
    invalid_bronze = connection.execute(
        "SELECT count(*) FROM bronze.pncp_responses WHERE encode(sha256(body),'hex')<>body_sha256"
    ).fetchone()[0]
    if invalid_bronze:
        raise ValueError('Bronze body hash mismatch')
    # The digest covers stable identities and content, not database-local row order.
    fingerprints = {}
    for name, query in {
        'bronze': 'SELECT id::text,body_sha256 FROM bronze.pncp_responses ORDER BY id',
        'identities': 'SELECT id::text,source,source_id FROM procurement.procurements ORDER BY id',
        'versions': 'SELECT id::text,procurement_id::text,content_hash,raw_response_id::text '
                    'FROM procurement.procurement_versions ORDER BY id',
        'observations': 'SELECT id::text,version_id::text,raw_response_id::text '
                        'FROM procurement.version_observations ORDER BY id',
    }.items():
        hasher = hashlib.sha256()
        for row in connection.execute(query):
            hasher.update(json.dumps(row, separators=(',', ':')).encode())
            hasher.update(b'\n')
        fingerprints[name] = hasher.hexdigest()
    return {'revision': revision, 'counts': counts, 'fingerprints': fingerprints,
            'invalid_bronze_hashes': invalid_bronze}


def run_checked(args):
    result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(args[0] + ' failed; stderr withheld to protect data')


def main():
    parser = argparse.ArgumentParser(description='Back up and rehearse restore of the isolated C2 target')
    parser.add_argument('--label', default='s06',
                        help='Unique lowercase label for a new evidence pair; never overwrites')
    parser.add_argument('--allow-growth', action='store_true',
                        help='Verify live-history growth against the earlier restored baseline')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,39}', args.label):
        raise ValueError('Invalid backup evidence label')
    identity = json.loads((ROOT / 'target-credentials.json').read_text())
    baseline = json.loads(BASELINE.read_text())
    target = identity['database']
    if (os.environ.get('PGDATABASE') != 'motor_decisao'
            or not re.fullmatch(r'test_\d{8}_[a-f0-9]{12}', target)
            or not re.fullmatch(r'radar_test_[a-f0-9]{12}', identity['role'])):
        raise ValueError('Protected database identity is invalid')
    dump = ROOT / f'{args.label}-current-0012.dump'
    report_path = ROOT / f'{args.label}-restore-report.json'
    if dump.exists() or report_path.exists():
        raise FileExistsError('S06 evidence exists; inspect it before retrying')
    admin = psycopg.connect(autocommit=True)
    rehearsal = None
    rehearsal_oid = None
    restored = False
    try:
        server = admin.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0]
        target_oid = admin.execute('SELECT oid FROM pg_database WHERE datname=%s', (target,)).fetchone()
        role_oid = admin.execute('SELECT oid FROM pg_roles WHERE rolname=%s', (identity['role'],)).fetchone()
        principal_revision = admin.execute('SELECT version_num FROM alembic_version').fetchone()[0]
        principal_has_procurement = admin.execute(
            "SELECT to_regnamespace('procurement') IS NOT NULL").fetchone()[0]
        if (server != baseline['server'] or target_oid != (identity['database_oid'],)
                or role_oid != (baseline['role_oid'],)
                or principal_revision != '20260817_0009' or principal_has_procurement):
            raise ValueError('Principal, C2 target, role or server drift')
        with psycopg.connect(dbname=target) as source:
            before = snapshot(source)
        if before['revision'] != '20261027_0012':
            raise ValueError('C2 revision drift')
        if args.allow_growth:
            prior = json.loads((ROOT / 'history-20261029-pre-restore-report.json').read_text())
            if (prior.get('restored_equal') is not True
                    or any(before['counts'][name] < count
                           for name, count in prior['counts'].items())
                    or before['counts']['control.source_attempts']
                    != before['counts']['control.source_attempt_outcomes']):
                raise ValueError('Live-history counts or attempt outcomes drift')
        elif (before['counts']['control.ingestion_jobs'] != 3
                or before['counts']['bronze.pncp_responses'] != 4
                or any(before['counts'][name] != 10 for name in (
                    'procurement.procurements', 'procurement.procurement_versions',
                    'procurement.version_observations'))):
            raise ValueError('C2 protected counts or revision drift')
        run_checked(['pg_dump', '--format=custom', '--no-owner', '--file', str(dump),
                     '--dbname', target])
        if dump.stat().st_size < 1000:
            raise ValueError('Empty or implausibly small S06 backup')
        toc = subprocess.run(['pg_restore', '--list', str(dump)],
                             capture_output=True, check=True).stdout.decode()
        if 'TABLE DATA' not in toc or 'alembic_version' not in toc:
            raise ValueError('Backup TOC lacks table data or revision')
        rehearsal = 'rehearsal_' + datetime.now(timezone.utc).strftime('%Y%m%d') + '_' + secrets.token_hex(6)
        if admin.execute('SELECT 1 FROM pg_database WHERE datname=%s', (rehearsal,)).fetchone():
            raise ValueError('Rehearsal database collision')
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(rehearsal)))
        rehearsal_oid = admin.execute('SELECT oid FROM pg_database WHERE datname=%s',
                                     (rehearsal,)).fetchone()[0]
        run_checked(['pg_restore', '--exit-on-error', '--single-transaction', '--no-owner',
                     '--dbname', rehearsal, str(dump)])
        with psycopg.connect(dbname=rehearsal) as copy:
            after = snapshot(copy)
        if after != before:
            raise ValueError('Restored C2 revision, counts or history fingerprints differ')
        if (admin.execute('SELECT version_num FROM alembic_version').fetchone()[0]
                != principal_revision or admin.execute(
                    "SELECT to_regnamespace('procurement') IS NOT NULL").fetchone()[0]):
            raise ValueError('Principal changed during S06 restore rehearsal')
        restored = True
        report = {
            'created_utc': datetime.now(timezone.utc).isoformat(),
            'target': target, 'target_oid': target_oid[0],
            'principal_revision': principal_revision,
            'principal_procurement_schema_absent': True,
            'revision': before['revision'], 'counts': before['counts'],
            'fingerprints': before['fingerprints'],
            'invalid_bronze_hashes': before['invalid_bronze_hashes'],
            'backup_name': dump.name, 'backup_bytes': dump.stat().st_size,
            'backup_sha256': digest(dump), 'pg_restore_list_verified': True,
            'full_restore_exit_code': 0, 'restored_equal': True,
            'rehearsal_cleaned': False,
            'growth_baseline_report': 'history-20261029-pre-restore-report.json' if args.allow_growth else None,
        }
    finally:
        if rehearsal is not None and rehearsal_oid is not None:
            actual = admin.execute('SELECT oid FROM pg_database WHERE datname=%s',
                                   (rehearsal,)).fetchone()
            if actual != (rehearsal_oid,):
                raise ValueError('Rehearsal OID changed; refusing cleanup')
            admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(rehearsal)))
        admin.close()
    if not restored:
        raise RuntimeError('S06 restore did not complete')
    report['rehearsal_cleaned'] = True
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    report_path.chmod(0o600)
    print(json.dumps(report, sort_keys=True))


if __name__ == '__main__':
    main()
