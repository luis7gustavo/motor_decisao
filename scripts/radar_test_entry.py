"""Unprivileged offline pytest entry point (RT-012/015)."""
import os
import socket
import sys

import psycopg
from sqlalchemy.engine import make_url


def main():
    url = make_url(os.environ["DATABASE_URL"])
    if url.database != os.environ["RADAR_TEST_DB"] or not url.database.startswith("test_") or url.username != os.environ["RADAR_TEST_ROLE"]:
        raise ValueError("Test identity mismatch")
    if os.geteuid() == 0 or os.path.exists(".env"):
        raise ValueError("Tests require unprivileged execution without .env")
    address = socket.gethostbyname(url.host)
    with psycopg.connect(host=address, port=url.port, user=url.username, password=url.password, dbname=url.database) as conn:
        observed = conn.execute("SELECT current_database(), current_user, (SELECT oid FROM pg_database WHERE datname=current_database()), (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)").fetchone()
        if observed != (url.database, url.username, int(os.environ["RADAR_TEST_OID"]), False):
            raise ValueError("Connected test identity mismatch")

    def audit(event, args):
        if event == "socket.connect":
            sock, endpoint = args
            if sock.family in (socket.AF_INET, socket.AF_INET6) and endpoint[:2] != (address, url.port):
                raise RuntimeError("Offline tests: network destination blocked")
        if event in ("subprocess.Popen", "os.system", "os.exec", "os.posix_spawn"):
            # ctypes discovers libpthread while importing Trio; this read-only
            # loader inventory cannot make a network request.
            if event == "subprocess.Popen" and args[0] == "/sbin/ldconfig" and args[1] == ["/sbin/ldconfig", "-p"]:
                return
            raise RuntimeError("Offline tests: external processes blocked")

    sys.addaudithook(audit)
    import pytest
    args = ["tests", "-o", "addopts=", "-q", "-ra", "-p", "no:cacheprovider"]
    if "--baseline" in sys.argv:
        args += ["--ignore=tests/test_radar_ops.py"]
    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
