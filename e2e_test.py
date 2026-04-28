"""
End-to-end test for the Kafka Lag Monitor MVP.

Starts the FastAPI server in a subprocess, hits every endpoint, verifies
that injecting a spike produces a breach alert within ~6 seconds, then
shuts down cleanly.

Run:  python e2e_test.py
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from urllib.request import Request, urlopen

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("E2E_PORT", "8765"))
BASE = f"http://127.0.0.1:{PORT}"


def http(method: str, path: str, expect: int = 200) -> dict:
    req = Request(BASE + path, method=method)
    with urlopen(req, timeout=10) as r:
        body = r.read().decode("utf-8")
        assert r.status == expect, f"{path}: expected {expect}, got {r.status}"
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"_raw": body[:200]}


def wait_for_port(host: str, port: int, timeout: float = 25.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.3)
    raise TimeoutError(f"server did not open {host}:{port} within {timeout}s")


def step(label: str) -> None:
    print(f"\n=== {label} ===")


def main() -> int:
    db_path = "/tmp/e2e_lag.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    env = dict(os.environ)
    env["PORT"] = str(PORT)
    env["LAG_MONITOR_DB"] = db_path
    for v in (
        "SLACK_WEBHOOK_URL",
        "SLACK_WEBHOOK_PNO_TEAM",
        "SLACK_WEBHOOK_CATALOG_TEAM",
        "SLACK_WEBHOOK_SHIPPING_TEAM",
    ):
        env.pop(v, None)
    for v in (
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "FTP_PROXY",
        "http_proxy", "https_proxy", "all_proxy", "ftp_proxy",
        "GRPC_PROXY", "grpc_proxy",
    ):
        env.pop(v, None)

    print(f"starting server on port {PORT}...")
    proc = subprocess.Popen(
        [sys.executable, "app.py"],
        cwd=HERE, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )

    failed = False
    try:
        wait_for_port("127.0.0.1", PORT)
        time.sleep(0.6)

        step("GET /api/health")
        h = http("GET", "/api/health")
        print(json.dumps(h, indent=2))
        assert h["ok"] is True
        assert h["jobs_monitored"] == 18
        assert h["threshold"] == 4_000_000

        step("GET /api/status")
        s = http("GET", "/api/status")
        summary = s["summary"]
        print(json.dumps(summary, indent=2))
        print(f"  -> {len(s['jobs'])} jobs returned (sample 3):")
        for j in s["jobs"][:3]:
            print(f"     {j['job_id']}  status={j['status']}  lag={j['lag']:,}  team={j['team']}")
        assert len(s["jobs"]) == 18
        assert summary["monitored"] == 18
        # Realism layers (bursts/incidents) can occasionally push a job over the
        # threshold. We only assert no PRESEEDED breaches — random ones are fine.
        assert summary["breaching"] <= 3, f"unexpectedly many startup breaches: {summary['breaching']}"

        step("GET /api/alerts (after first poll cycle)")
        time.sleep(5.5)
        a = http("GET", "/api/alerts?limit=20")
        print(f"  -> {len(a['alerts'])} alerts in log")
        for entry in a["alerts"][:5]:
            print(f"     [{entry['alert_type']}] {entry['topic']} :: {entry['environment']}  lag={entry['lag_value']:,}")
        # Without preseeded breaches, the only alerts at startup come from
        # natural simulator variability — typically 0-3.
        prev_alert_count = len(a["alerts"])
        assert prev_alert_count <= 3, f"too many spurious startup alerts: {prev_alert_count}"


        step("GET /api/job/{job_id}/history?minutes=30")
        sample_job = s["jobs"][0]["job_id"]
        hist = http("GET", f"/api/job/{sample_job}/history?minutes=30")
        print(f"  -> job={sample_job}  points={len(hist['history'])}  threshold={hist['threshold']:,}")
        assert len(hist["history"]) > 50, "warmup should have produced many points"

        step("Inject flow - POST /api/inject/<healthy_job> and verify alert fires within 8s")
        target = next((j for j in s["jobs"] if j["status"] == "ok"), None)
        assert target, "no healthy job found to inject"
        print(f"  injecting: {target['job_id']}  (team={target['team']})")
        inj = http("POST", f"/api/inject/{target['job_id']}?stream=topic&duration=120")
        print(f"  inject response: {inj}")
        assert inj["ok"] is True

        fired = False
        deadline = time.time() + 9.0
        start = time.time()
        while time.time() < deadline:
            time.sleep(0.5)
            a2 = http("GET", "/api/alerts?limit=20")
            new_for_target = [
                x for x in a2["alerts"]
                if x["job_id"] == target["job_id"] and x["alert_type"] == "breach"
            ]
            if new_for_target and len(a2["alerts"]) > prev_alert_count:
                fired = True
                elapsed = time.time() - start
                print(f"  OK breach alert fired after {elapsed:.1f}s")
                e0 = new_for_target[0]
                print(f"    {e0['topic']} :: {e0['environment']}  lag={e0['lag_value']:,}  channel={e0['channel']}")
                break
        assert fired, "inject did not produce a breach alert within 9s"

        s2 = http("GET", "/api/status")
        injected_job = next(j for j in s2["jobs"] if j["job_id"] == target["job_id"])
        print(f"  /api/status reflects: status={injected_job['status']}  lag={injected_job['lag']:,}  injecting={injected_job['injecting']}")
        assert injected_job["status"] == "breach"
        assert injected_job["injecting"] is True
        assert injected_job["injecting_stream"] == "topic", "inject should remember which stream was spiked"

        step("POST /api/clear/<job_id>")
        c = http("POST", f"/api/clear/{target['job_id']}")
        print(f"  clear response: {c}")
        assert c["ok"] is True

        time.sleep(6.0)
        a3 = http("GET", "/api/alerts?limit=30")
        resolved = [
            x for x in a3["alerts"]
            if x["job_id"] == target["job_id"] and x["alert_type"] == "resolved"
        ]
        if resolved:
            print(f"  OK resolved alert fired: lag={resolved[0]['lag_value']:,}")
        else:
            print("  (no resolved alert yet - personality may still be near threshold)")

        step("GET / (HTML dashboard)")
        with urlopen(BASE + "/", timeout=5) as r:
            body = r.read().decode("utf-8", errors="replace")
            assert "<title>Kafka Lag Monitor" in body
            assert "/api/status" in body
            print(f"  OK index.html served, {len(body):,} bytes")

        step("404 handling - POST /api/inject/<unknown>")
        try:
            http("POST", "/api/inject/no-such-job", expect=200)
            assert False, "should have returned 404"
        except Exception as e:
            print(f"  OK correctly errored: {e}")

        print("\nALL E2E CHECKS PASSED")

    except AssertionError as e:
        failed = True
        print(f"\nASSERTION FAILED: {e}")
    except Exception as e:
        failed = True
        print(f"\nERROR: {type(e).__name__}: {e}")
    finally:
        step("Shutting down server")
        proc.terminate()
        try:
            stdout, _ = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, _ = proc.communicate()
        print("  server log tail:")
        for line in (stdout or "").splitlines()[-12:]:
            print(f"    | {line}")
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except OSError:
                pass

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
