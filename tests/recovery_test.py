"""End-to-end test for primary disaster recovery via POST /api/recover.

Run with the project venv:  .venv/bin/python -I tests/recovery_test.py

Scenarios:
 1. primary volume loss -> restarted primary is the empty serial-0 zone
    while the backup still serves the old complete zone;
 2. operator triggers /api/recover: backup zone is fetched and validated,
    primary is restored with a serial strictly above the backup serial;
 3. the backup catches the restored version up via its normal sync loop;
 4. backup unreachable -> 502, primary stays empty;
 5. backup serves an invalid zone (bad IP / wrong zone name) -> 502,
    primary stays empty;
 6. a normal publish racing a slow recovery wins: recovery gets 409 and
    never overwrites the freshly published zone;
 7. recovery on a non-empty primary is refused (409) without any pull;
 8. the endpoint is primary-only (403 on the backup);
 9. an empty serial-0 backup has nothing recoverable -> 502.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
from dnslib import DNSRecord, RCODE

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

ZONE = "internal.local"
PRIMARY_API = "http://127.0.0.1:18020"
BACKUP_API = "http://127.0.0.1:18021"
PRIMARY_DNS = ("127.0.0.1", 15060)
BACKUP_DNS = ("127.0.0.1", 15061)
BLACKHOLE = "http://127.0.0.1:19998"
FAILURES = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name +
          (" " + detail if not cond else ""))
    if not cond:
        FAILURES.append(name)


def wait_http(url, timeout=15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.5).status_code == 200:
                return
        except Exception as exc:
            last = exc
        time.sleep(0.3)
    raise RuntimeError(f"not ready: {url} ({last})")


def dns_query(server, name, qtype="A"):
    q = DNSRecord.question(name, qtype)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(3)
    sock.sendto(q.pack(), server)
    data, _ = sock.recvfrom(65535)
    sock.close()
    return DNSRecord.parse(data)


def start(role, api_port, dns_port, db, upstream, backup_url=None,
          sync_interval="1"):
    env = os.environ.copy()
    env.update({
        "DNS_ROLE": role, "DNS_ZONE": ZONE, "DB_PATH": db,
        "DNS_HOST": "127.0.0.1", "DNS_PORT": str(dns_port),
        "API_HOST": "127.0.0.1", "API_PORT": str(api_port),
        "SYNC_INTERVAL": sync_interval, "HTTP_TIMEOUT": "5",
        "PYTHONUNBUFFERED": "1",
    })
    if upstream:
        env["PRIMARY_API_URL"] = upstream
    if backup_url:
        env["BACKUP_API_URL"] = backup_url
    log = open(DATA / f"{role}-recover.log", "w")
    return subprocess.Popen(
        [PY, "-I", "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(api_port)],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT)


def stop(p):
    if p is None or p.poll() is not None:
        return
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def remove_db(path):
    for suffix in ("", "-wal", "-shm"):
        p = Path(path + suffix)
        if p.exists():
            p.unlink()


def serial(api):
    return httpx.get(f"{api}/api/version", timeout=3).json()["serial"]


class SlowBackupHandler(BaseHTTPRequestHandler):
    """Fake backup that holds /api/zone open, letting a publish race."""

    delay = 3.0
    body = json.dumps({
        "zone": ZONE,
        "serial": 2,
        "records": [{"name": f"db.{ZONE}", "ip": "10.0.0.12"}],
        "published_at": "2026-11-16T00:00:00Z",
    }).encode()

    def do_GET(self):
        if self.path != "/api/zone":
            self.send_response(404)
            self.end_headers()
            return
        time.sleep(self.delay)
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *a):
        pass


class StaticHandler(BaseHTTPRequestHandler):
    payload = b"{}"
    status = 200

    def do_GET(self):
        self.send_response(self.status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(self.payload)))
        self.end_headers()
        self.wfile.write(self.payload)

    def log_message(self, *a):
        pass


def serve_static(payload: dict | bytes, status=200):
    if isinstance(payload, dict):
        body = json.dumps(payload).encode()
    else:
        body = payload

    class H(StaticHandler):
        pass

    H.payload = body
    H.status = status
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def main():
    primary_db = str(DATA / "primary-recover.db")
    backup_db = str(DATA / "backup-recover.db")
    remove_db(primary_db)
    remove_db(backup_db)

    print("[1] seed a healthy pair, then lose the primary volume")
    primary = start("primary", 18020, 15060, primary_db, None,
                    backup_url=BACKUP_API)
    backup = start("backup", 18021, 15061, backup_db, PRIMARY_API)
    try:
        wait_http(f"{PRIMARY_API}/health")
        wait_http(f"{BACKUP_API}/health")
        records = [
            {"name": f"db.{ZONE}", "ip": "10.0.0.11"},
            {"name": f"web.{ZONE}", "ip": "10.0.0.21"},
        ]
        r = httpx.post(f"{PRIMARY_API}/api/zone",
                       json={"records": records}, timeout=5)
        check("publish serial 1", r.status_code == 201
              and r.json()["serial"] == 1, r.text)
        deadline = time.time() + 12
        while time.time() < deadline and serial(BACKUP_API) < 1:
            time.sleep(0.3)
        check("backup holds serial 1", serial(BACKUP_API) == 1)
        r = dns_query(BACKUP_DNS, f"db.{ZONE}")
        check("backup still resolves the old zone",
              [str(x.rdata) for x in r.rr] == ["10.0.0.11"])

        # Simulate the deleted/re-initialised primary data volume.
        stop(primary)
        remove_db(primary_db)
        primary = start("primary", 18020, 15060, primary_db, None,
                        backup_url=BACKUP_API)
        wait_http(f"{PRIMARY_API}/health")
        check("primary re-initialised at empty serial 0",
              serial(PRIMARY_API) == 0)
        r = dns_query(PRIMARY_DNS, f"db.{ZONE}")
        check("empty primary answers NXDOMAIN",
              r.header.rcode == RCODE.NXDOMAIN)
        check("backup serial untouched by primary loss",
              serial(BACKUP_API) == 1)

        print("[2] operator triggers recovery from the backup")
        r = httpx.post(f"{PRIMARY_API}/api/recover", timeout=10)
        body = r.json()
        check("recovery 200 with serial above backup",
              r.status_code == 200 and body["status"] == "recovered"
              and body["backup_serial"] == 1 and body["serial"] > 1
              and body["record_count"] == 2, json.dumps(body))
        restored_serial = body["serial"]
        zone = httpx.get(f"{PRIMARY_API}/api/zone", timeout=3).json()
        check("primary zone restored with the backup's A records",
              zone["serial"] == restored_serial
              and zone["records"] == [
                  {"name": f"db.{ZONE}", "ip": "10.0.0.11"},
                  {"name": f"web.{ZONE}", "ip": "10.0.0.21"},
              ], json.dumps(zone))
        r = dns_query(PRIMARY_DNS, f"web.{ZONE}")
        check("primary UDP resolves restored zone",
              r.header.rcode == RCODE.NOERROR
              and [str(x.rdata) for x in r.rr] == ["10.0.0.21"])

        print("[3] backup catches the restored version via normal sync")
        deadline = time.time() + 12
        caught = False
        while time.time() < deadline:
            if serial(BACKUP_API) == restored_serial:
                caught = True
                break
            time.sleep(0.3)
        check("backup auto-syncs to the restored serial", caught)
        r = dns_query(BACKUP_DNS, f"db.{ZONE}")
        check("backup UDP answers the restored record",
              [str(x.rdata) for x in r.rr] == ["10.0.0.11"])

        print("[4] second recovery on a non-empty primary is refused")
        r = httpx.post(f"{PRIMARY_API}/api/recover", timeout=10)
        check("non-empty primary -> 409, state unchanged",
              r.status_code == 409 and serial(PRIMARY_API) == restored_serial,
              r.text)

        print("[5] endpoint is primary-only")
        r = httpx.post(f"{BACKUP_API}/api/recover", timeout=5)
        check("backup /api/recover -> 403", r.status_code == 403, r.text)

        print("[6] backup unreachable: primary state untouched, reason given")
        stop(primary)
        remove_db(primary_db)
        primary = start("primary", 18020, 15060, primary_db, None,
                        backup_url=BLACKHOLE)
        wait_http(f"{PRIMARY_API}/health")
        r = httpx.post(f"{PRIMARY_API}/api/recover", timeout=10)
        check("unreachable backup -> 502 with reason",
              r.status_code == 502 and "unreachable" in
              r.json()["detail"]["reason"], r.text)
        check("primary stays empty after failed recovery",
              serial(PRIMARY_API) == 0)

        print("[7] invalid backup data: bad A record and wrong zone")
        bad_ip_srv, bad_ip_url = serve_static({
            "zone": ZONE, "serial": 3,
            "records": [{"name": f"db.{ZONE}", "ip": "not-an-ip"}],
            "published_at": "2026-11-16T00:00:00Z",
        })
        stop(primary)
        primary = start("primary", 18020, 15060, primary_db, None,
                        backup_url=bad_ip_url)
        wait_http(f"{PRIMARY_API}/health")
        r = httpx.post(f"{PRIMARY_API}/api/recover", timeout=10)
        check("invalid A record -> 502 with validation reason",
              r.status_code == 502 and "validation" in
              r.json()["detail"]["reason"], r.text)
        check("primary still empty after invalid-data recovery",
              serial(PRIMARY_API) == 0)
        bad_ip_srv.shutdown()

        bad_zone_srv, bad_zone_url = serve_static({
            "zone": "other.example", "serial": 3,
            "records": [{"name": "db.other.example", "ip": "10.0.0.9"}],
            "published_at": "2026-11-16T00:00:00Z",
        })
        stop(primary)
        primary = start("primary", 18020, 15060, primary_db, None,
                        backup_url=bad_zone_url)
        wait_http(f"{PRIMARY_API}/health")
        r = httpx.post(f"{PRIMARY_API}/api/recover", timeout=10)
        check("zone-name mismatch -> 502 with zone reason",
              r.status_code == 502 and "zone" in
              r.json()["detail"]["reason"], r.text)
        check("primary still empty after zone mismatch",
              serial(PRIMARY_API) == 0)
        bad_zone_srv.shutdown()

        empty_srv, empty_url = serve_static({
            "zone": ZONE, "serial": 0, "records": [],
            "published_at": "2026-11-16T00:00:00Z",
        })
        stop(primary)
        primary = start("primary", 18020, 15060, primary_db, None,
                        backup_url=empty_url)
        wait_http(f"{PRIMARY_API}/health")
        r = httpx.post(f"{PRIMARY_API}/api/recover", timeout=10)
        check("empty backup serial 0 -> 502, nothing to recover",
              r.status_code == 502 and serial(PRIMARY_API) == 0, r.text)
        empty_srv.shutdown()

        print("[8] racing publish during recovery must not be overwritten")
        # Slow fake backup: the /api/zone pull takes ~3s. A normal publish
        # lands while the pull is in flight; recovery must then abort.
        slow_srv = ThreadingHTTPServer(("127.0.0.1", 18097), SlowBackupHandler)
        threading.Thread(target=slow_srv.serve_forever, daemon=True).start()
        stop(primary)
        primary = start("primary", 18020, 15060, primary_db, None,
                        backup_url="http://127.0.0.1:18097")
        wait_http(f"{PRIMARY_API}/health")

        def call_recover():
            call_recover.response = httpx.post(
                f"{PRIMARY_API}/api/recover", timeout=15)

        t = threading.Thread(target=call_recover)
        t.start()
        time.sleep(0.8)  # let the slow backup pull start
        pr = httpx.post(f"{PRIMARY_API}/api/zone", json={"records": [
            {"name": f"new.{ZONE}", "ip": "172.16.0.1"}]}, timeout=5)
        check("racing publish succeeds at serial 1",
              pr.status_code == 201 and pr.json()["serial"] == 1, pr.text)
        t.join()
        rr = call_recover.response
        check("racing recovery -> 409 conflict, no overwrite",
              rr.status_code == 409 and "empty" in
              rr.json()["detail"]["reason"], rr.text)
        check("primary keeps the raced publish, not the backup content",
              serial(PRIMARY_API) == 1
              and [x["name"] for x in
                   httpx.get(f"{PRIMARY_API}/api/zone").json()["records"]]
              == [f"new.{ZONE}"])
        r = dns_query(PRIMARY_DNS, f"new.{ZONE}")
        check("UDP serves the raced publish",
              [str(x.rdata) for x in r.rr] == ["172.16.0.1"])
        slow_srv.shutdown()
    finally:
        stop(primary)
        stop(backup)

    print(f"\n==== recovery checks: {len(FAILURES)} failures ====")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
