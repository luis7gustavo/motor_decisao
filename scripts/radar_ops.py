"""Compose-only foundation runner. Never migrate or clean the principal.

The privileged coordinator owns ephemeral targets; pytest runs as nobody with
only a temporary nonsuperuser credential, no .env, and an outbound socket guard.
Reports contain identifiers/counts, never restored rows or connection strings.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

import psycopg
from psycopg import sql
from alembic.config import Config
from alembic.script import ScriptDirectory


def validate_target(name, principal, created):
    if name == principal or not re.fullmatch(r"(?:rehearsal|test)_\d{8}_[a-f0-9]{12}", name) or name not in created:
        raise ValueError("Target is not a temporary database created by this execution")


def validate_revision_path(origin, target, parents):
    if target in {"head", "heads", "base", "-1", origin}:
        raise ValueError("An explicit new descendant revision is required")
    result = []
    node = target
    while node != origin:
        if node in result or node not in parents or not isinstance(parents[node], str):
            raise ValueError("Revision path is divergent, merged or older than origin")
        result.append(node)
        node = parents[node]
    return list(reversed(result))


def digest(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def dump_tables(toc):
    # TABLE DATA/ATTACH are distinct TOC object types, not schemas named DATA.
    return {schema + "." + table for schema, table in re.findall(
        r"^\d+; \d+ \d+ TABLE (?!DATA |ATTACH )(\S+) (\S+) ", toc, re.MULTILINE)}


class Runner:
    def __init__(self, mode):
        self.principal = os.environ["PGDATABASE"]
        self.admin = psycopg.connect(autocommit=True)
        self.server = self.admin.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0]
        self.created = {}
        self.roles = {}
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + secrets.token_hex(4)
        self.report = {"run_id": self.run_id, "mode": mode, "principal": self.principal,
                       "server": self.server, "created": {}, "cleaned": [], "steps": []}
        self.report_path = Path("/evidence") / (self.run_id + ".json")
        self.save()

    def save(self):
        self.report_path.write_text(json.dumps(self.report, indent=2) + "\n")
        self.report_path.chmod(0o600)

    def check_server(self):
        actual = self.admin.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0]
        if actual != self.server:
            raise ValueError("Server identity changed")

    def check(self, name):
        validate_target(name, self.principal, self.created)
        self.check_server()
        actual = self.admin.execute("SELECT oid FROM pg_database WHERE datname=%s", (name,)).fetchone()
        if not actual or actual[0] != self.created[name]:
            raise ValueError("Database identity changed")

    def create(self, prefix):
        self.check_server()
        name = prefix + "_" + datetime.now(timezone.utc).strftime("%Y%m%d") + "_" + secrets.token_hex(6)
        if self.admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
            raise ValueError("Target already exists")
        self.admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
        self.created[name] = self.admin.execute("SELECT oid FROM pg_database WHERE datname=%s", (name,)).fetchone()[0]
        self.report["created"][name] = self.created[name]
        self.save()
        self.check(name)
        return name

    def connection(self, name):
        self.check(name)
        conn = psycopg.connect(dbname=name, autocommit=True)
        actual = conn.execute("SELECT current_database(), (SELECT system_identifier::text FROM pg_control_system())").fetchone()
        if actual != (name, self.server):
            conn.close()
            raise ValueError("Connection identity mismatch")
        return conn

    def command(self, args, *, name=None, env=None):
        if name:
            self.check(name)
        result = subprocess.run(args, env=env, capture_output=True)
        # Never expose pg_restore errors: errors can include row values.
        self.report["steps"].append({"command": args[:2], "exit_code": result.returncode})
        self.save()
        if result.returncode:
            raise RuntimeError(f"{args[0]} failed with code {result.returncode}; raw output suppressed")
        return result.stdout

    def inventory(self, name, tables=None):
        with self.connection(name) as conn, conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            revision = conn.execute("SELECT version_num FROM alembic_version ORDER BY version_num").fetchall()
            if len(revision) != 1:
                raise ValueError("Exactly one applied revision is required")
            if tables is None:
                tables = conn.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1,2").fetchall()
            counts = {}
            structures = {}
            for schema, table in tables:
                counts[schema + "." + table] = conn.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))).fetchone()[0]
                qualified = sql.Identifier(schema, table).as_string(conn)
                columns = conn.execute("SELECT attname, format_type(atttypid,atttypmod), attnotnull, pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", (qualified,)).fetchall()
                constraints = conn.execute("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass ORDER BY conname", (qualified,)).fetchall()
                indexes = conn.execute("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s ORDER BY indexname", (schema, table)).fetchall()
                structures[schema + "." + table] = hashlib.sha256(json.dumps([columns, constraints, indexes]).encode()).hexdigest()
            return {"revision": revision[0][0], "counts": counts, "structures": structures}

    def migrate(self, name, direction, revision, location="alembic"):
        self.check(name)
        env = os.environ.copy()
        from sqlalchemy.engine import URL
        env["DATABASE_URL"] = URL.create("postgresql+psycopg", username=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"], host=os.environ["PGHOST"], port=int(os.environ["PGPORT"]), database=name).render_as_string(hide_password=False)
        # Copy config; env.py reads only the explicitly set DATABASE_URL.
        with tempfile.TemporaryDirectory() as directory:
            cfg = Path(directory) / "alembic.ini"
            cfg.write_text(Path("alembic.ini").read_text().replace("script_location = alembic", "script_location = " + location))
            self.command([sys.executable, "-m", "alembic", "-c", str(cfg), direction, revision], name=name, env=env)

    def rehearse(self, dump, origin, target):
        dump = Path(dump).resolve()
        if dump.parent != Path("/evidence") or not dump.is_file() or dump.stat().st_size == 0:
            raise ValueError("A nonempty dump directly under /evidence is required")
        before = digest(dump)
        toc = self.command(["pg_restore", "--list", str(dump)]).decode()
        expected_tables = dump_tables(toc)
        if not expected_tables:
            raise ValueError("Dump contains no table inventory")
        self.report["backup"] = {"name": dump.name, "bytes": dump.stat().st_size, "sha256": before}
        main_revision = self.admin.execute("SELECT version_num FROM alembic_version").fetchall()
        if main_revision != [(origin,)]:
            raise ValueError("Principal revision differs from explicit origin")
        graph = ScriptDirectory.from_config(Config("alembic.ini"))
        if graph.get_heads() != [origin] and target == "synthetic":
            raise ValueError("Synthetic foundation requires code head to match origin")
        name = self.create("rehearsal")
        self.command(["pg_restore", "--exit-on-error", "--single-transaction", "--no-owner", "--dbname", name, str(dump)], name=name)
        baseline = self.inventory(name)
        if baseline["revision"] != origin:
            raise ValueError("Restored revision differs from origin")
        if set(baseline["counts"]) != expected_tables:
            raise ValueError("Restored tables differ from dump inventory")
        synthetic = target == "synthetic"
        self.report["expected_table_count"] = len(expected_tables)
        self.report["baseline"] = baseline
        self.report["count_source"] = "restored dump; never independent queries against active principal"
        self.save()
        tables = [tuple(key.split(".", 1)) for key in baseline["counts"]]
        with tempfile.TemporaryDirectory() as directory:
            location = str(Path(directory) / "alembic")
            shutil.copytree("alembic", location)
            if target == "synthetic":
                target = "c0_empty_" + secrets.token_hex(5)
                (Path(location) / "versions" / (target + ".py")).write_text(
                    f"revision = {target!r}\ndown_revision = {origin!r}\nbranch_labels = None\ndepends_on = None\ndef upgrade():\n    pass\ndef downgrade():\n    pass\n")
            cfg = Config("alembic.ini")
            cfg.set_main_option("script_location", location)
            revisions = ScriptDirectory.from_config(cfg)
            parents = {r.revision: r.down_revision for r in revisions.walk_revisions()}
            self.report["new_revisions"] = validate_revision_path(origin, target, parents)
            self.report["migration_hashes"] = {Path(r.path).name: digest(r.path) for r in revisions.walk_revisions()}
            for direction, destination in [("upgrade", target), ("downgrade", origin), ("upgrade", target)]:
                self.migrate(name, direction, destination, location)
                observed = self.inventory(name, tables)
                if observed["revision"] != destination or observed["counts"] != baseline["counts"]:
                    raise ValueError("Revision or legacy row counts changed unexpectedly")
                if synthetic and observed["structures"] != baseline["structures"]:
                    raise ValueError("Synthetic rehearsal changed legacy structure")
                self.report["steps"].append({"migration": direction, "revision": destination, "legacy_counts_equal": True, "synthetic_structure_equal": synthetic})
                self.save()
        if digest(dump) != before:
            raise ValueError("Dump changed during rehearsal")
        self.report["restoration_verified"] = True

    def tests(self, origin, baseline_only):
        graph = ScriptDirectory.from_config(Config("alembic.ini"))
        if origin not in {r.revision for r in graph.walk_revisions()}:
            raise ValueError("Tests require an explicit known revision")
        name = self.create("test")
        self.migrate(name, "upgrade", origin)
        # Restricted role owns only the synthetic database's objects.
        role = "radar_test_" + secrets.token_hex(6)
        password = secrets.token_hex(24)
        self.admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION").format(sql.Identifier(role), sql.Literal(password)))
        self.roles[role] = self.admin.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (role,)).fetchone()[0]
        self.report["test_role"] = role
        self.save()
        # Grants are made only inside the disposable synthetic database.
        with self.connection(name) as conn:
            schemas = conn.execute("SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'").fetchall()
            for (schema,) in schemas:
                conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(role)))
                conn.execute(sql.SQL("GRANT ALL ON ALL TABLES IN SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(role)))
                conn.execute(sql.SQL("GRANT ALL ON ALL SEQUENCES IN SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(role)))
        forbidden = self.admin.execute("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND c.relkind IN ('r','p') AND has_table_privilege(%s,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')", (role,)).fetchone()[0]
        if forbidden:
            raise ValueError("Test credential has privileges on operational tables")
        self.report["principal_table_privileges_for_test_role"] = 0
        self.check(name)
        from sqlalchemy.engine import URL
        url = URL.create("postgresql+psycopg", username=role, password=password,
                         host=os.environ["PGHOST"], port=int(os.environ["PGPORT"]), database=name)
        env = {"PATH": os.environ["PATH"], "PYTHONPATH": "/app", "PYTHONDONTWRITEBYTECODE": "1",
               "DATABASE_URL": url.render_as_string(hide_password=False), "RADAR_TEST_DB": name,
               "RADAR_TEST_ROLE": role, "RADAR_TEST_OID": str(self.created[name]),
               "HOME": "/tmp", "PREFECT_SERVER_ANALYTICS_ENABLED": "false"}
        args = [sys.executable, "-m", "scripts.radar_test_entry"]
        if baseline_only:
            args.append("--baseline")
        result = subprocess.run(args, env=env, capture_output=True, user=65534, group=65534, extra_groups=[])
        output = (result.stdout + result.stderr).decode(errors="replace")
        output = output.replace(password, "[REDACTED]").replace(env["DATABASE_URL"], "[REDACTED]")
        log = self.report_path.with_suffix(".tests.txt")
        log.write_text(output)
        log.chmod(0o600)
        self.report["pytest_exit_code"] = result.returncode
        self.report["pytest_log"] = log.name
        self.save()
        print(output, flush=True)
        if result.returncode:
            raise RuntimeError("Test suite failed; see sanitized report")

    def cleanup(self):
        errors = []
        for name in list(self.created):
            try:
                self.check(name)
                # Only our recorded OID on the recorded server; never terminate
                # sessions in any other database. No FORCE/drop of unknown DBs.
                self.admin.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datid=%s AND pid<>pg_backend_pid()", (self.created[name],))
                self.admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
                if self.admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
                    raise RuntimeError("Database removal not confirmed")
                self.report["cleaned"].append(name)
                del self.created[name]
            except Exception as exc:
                errors.append({"resource": name, "error_type": type(exc).__name__})
        for role, oid in self.roles.items():
            try:
                self.check_server()
                if self.admin.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (role,)).fetchone() != (oid,):
                    raise ValueError("Role identity changed")
                self.admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
                self.report["cleaned"].append(role)
            except Exception as exc:
                errors.append({"resource": role, "error_type": type(exc).__name__})
        self.report["cleanup_errors"] = errors
        self.report["finished_utc"] = datetime.now(timezone.utc).isoformat()
        self.save()
        self.admin.close()
        if errors:
            raise RuntimeError("Cleanup failed; original failure remains in report")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["tests", "rehearse"])
    parser.add_argument("--origin", required=True)
    parser.add_argument("--dump")
    parser.add_argument("--target", default="synthetic")
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    def interrupted(signum, _frame):
        raise SystemExit(f"Interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    runner = Runner(args.mode)
    failed = False
    try:
        if args.mode == "tests":
            runner.tests(args.origin, args.baseline)
        else:
            runner.rehearse(args.dump, args.origin, args.target)
        runner.report["operation_passed"] = True
    except BaseException as exc:
        failed = True
        runner.report["operation_passed"] = False
        runner.report["failure_type"] = type(exc).__name__
        # Only our own validation/process messages are safe to persist. Database
        # exceptions may contain connection strings or restored row contents.
        if type(exc) in (ValueError, RuntimeError, SystemExit):
            runner.report["failure_reason"] = str(exc)
        print("Operation failed:", type(exc).__name__, flush=True)
    finally:
        try:
            runner.cleanup()
        except Exception:
            failed = True
            print("Cleanup failed; inspect report", flush=True)
    print("Report:", runner.report_path.name, flush=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
