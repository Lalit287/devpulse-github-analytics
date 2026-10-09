"""Manage only the project-owned loopback PostgreSQL cluster."""
import argparse
import json
import os
import secrets
import shutil
import socket
import subprocess
import uuid
from pathlib import Path

import psycopg
from psycopg import sql

from config.settings import ROOT
from exploration.io import write_json

RUNTIME = ROOT / ".runtime/postgres"
DATA = RUNTIME / "data"
PORT = 15432
DBNAME = "devpulse"
ROLES = {"admin": "devpulse_owner", "writer": "devpulse_writer", "reader": "devpulse_reader"}


def executable(name):
    value = shutil.which(name)
    if not value:
        # Homebrew keg-only PostgreSQL is often absent from a fresh Terminal PATH.
        for base in ['/opt/homebrew/opt/postgresql@14/bin', '/usr/local/opt/postgresql@14/bin']:
            candidate = Path(base) / name
            if candidate.is_file() and os.access(candidate, os.X_OK):
                value = str(candidate)
                break
    if not value:
        raise RuntimeError(f"PostgreSQL executable unavailable: {name}")
    return value


def private_json(path, value):
    write_json(path, value)
    path.chmod(0o600)


def credentials():
    path = RUNTIME / "credentials.json"
    if not path.exists():
        raise RuntimeError("Start the project database first: python -m database.cluster start")
    return json.loads(path.read_text())


def connect(role="reader", *, dbname=DBNAME, autocommit=False):
    if role not in ROLES:
        raise ValueError("Unknown project database role")
    secret = credentials()
    connection = psycopg.connect(host="127.0.0.1", port=PORT, dbname=dbname, user=ROLES[role],
                                 password=secret[role], connect_timeout=5, autocommit=autocommit,
                                 application_name=f"DevPulse-{role}")
    connection.execute("SET TIME ZONE 'UTC'")
    connection.execute("SET statement_timeout = '15s'" if role == "reader" else "SET statement_timeout = '20min'")
    if not autocommit:
        connection.commit()
    return connection


def guard():
    marker = RUNTIME / "cluster.json"
    if not marker.exists() or json.loads(marker.read_text()).get("data_directory") != str(DATA):
        raise RuntimeError("Missing project cluster identity; refusing to manage another database")


def running():
    if not (DATA / "PG_VERSION").exists():
        return False
    guard()
    return subprocess.run([executable("pg_ctl"), "-D", str(DATA), "status"], capture_output=True).returncode == 0


def initialize():
    RUNTIME.mkdir(parents=True, exist_ok=True)
    RUNTIME.chmod(0o700)
    if (DATA / "PG_VERSION").exists():
        guard()
        credentials()
        return
    if DATA.exists() and any(DATA.iterdir()):
        raise RuntimeError("Nonempty PostgreSQL directory has no cluster identity; preserving its files")
    secret = {role: secrets.token_urlsafe(32) for role in ROLES}
    private_json(RUNTIME / "credentials.json", secret)
    password_file = RUNTIME / "initial_admin_password"
    password_file.write_text(secret["admin"] + "\n")
    password_file.chmod(0o600)
    try:
        result = subprocess.run([executable("initdb"), "-D", str(DATA), "-U", ROLES["admin"],
                                 "--pwfile", str(password_file), "--auth-local=trust", "--auth-host=scram-sha-256",
                                 "--encoding=UTF8", "--locale=C"], capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError(f"Project initdb failed: {result.stderr.strip()}")
    finally:
        password_file.unlink(missing_ok=True)
    sock = RUNTIME / "socket"
    sock.mkdir(exist_ok=True)
    sock.chmod(0o700)
    with (DATA / "postgresql.conf").open("a") as handle:
        handle.write(f"\n# DevPulse isolated local cluster\nlisten_addresses = '127.0.0.1'\nport = {PORT}\n"
                     f"unix_socket_directories = '{sock}'\ntimezone = 'UTC'\nshared_buffers = '128MB'\n"
                     "work_mem = '16MB'\nmax_connections = 20\nmax_wal_size = '512MB'\n"
                     "log_statement = 'none'\n")
    private_json(RUNTIME / "cluster.json", {"cluster_id": uuid.uuid4().hex, "data_directory": str(DATA), "port": PORT})


def bootstrap():
    secret = credentials()
    with connect("admin", dbname="postgres", autocommit=True) as connection:
        for role in ("writer", "reader"):
            if not connection.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (ROLES[role],)).fetchone():
                connection.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(ROLES[role]), sql.Literal(secret[role])))
        if not connection.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DBNAME,)).fetchone():
            connection.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(DBNAME), sql.Identifier(ROLES["writer"])))
        connection.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(sql.Identifier(ROLES["reader"])))
    with connect("admin", autocommit=True) as connection:
        connection.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        connection.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(DBNAME)))
        connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}, {}").format(sql.Identifier(DBNAME), sql.Identifier(ROLES["writer"]), sql.Identifier(ROLES["reader"])))


def start():
    initialize()
    if not running():
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", PORT)) == 0:
                raise RuntimeError(f"Port {PORT} is occupied; no existing service was changed")
        result = subprocess.run([executable("pg_ctl"), "-D", str(DATA), "-l", str(RUNTIME / "postgres.log"),
                                 "-w", "-t", "20", "start"], capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError(f"Project PostgreSQL start failed: {result.stderr.strip()}; see .runtime/postgres/postgres.log")
    bootstrap()
    return status()


def status():
    if not running():
        return {"status": "stopped", "host": "127.0.0.1", "port": PORT}
    with connect("admin", dbname="postgres", autocommit=True) as connection:
        version = connection.execute("SHOW server_version").fetchone()[0]
    return {"status": "healthy", "host": "127.0.0.1", "port": PORT, "database": DBNAME, "server_version": version}


def stop():
    guard()
    if running():
        subprocess.run([executable("pg_ctl"), "-D", str(DATA), "-m", "fast", "-w", "-t", "20", "stop"],
                       check=True, capture_output=True, timeout=30)
    return {"status": "stopped", "port": PORT}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "status", "stop"])
    args = parser.parse_args()
    print(json.dumps({"start": start, "status": status, "stop": stop}[args.action](), indent=2))


if __name__ == "__main__":
    main()
