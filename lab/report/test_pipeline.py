#!/usr/bin/env python3
"""End-to-end test of the report pipeline (process -> html -> validate).

Builds a minimal synthetic dataset in a temp directory, runs the real
pipeline scripts against it, and verifies the outputs. This is a pipeline
test, not a benchmark: the synthetic data is only used to exercise the
processing code paths.

Run: python3 -m unittest report.test_pipeline
"""
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

REPORT_DIR = os.path.dirname(os.path.abspath(__file__))
LAB_DIR = os.path.dirname(REPORT_DIR)


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(data)


def make_dataset(root):
    """Create a minimal but valid dataset: 2 runs (1 success, 1 failed)."""
    raw = os.path.join(root, "raw")
    ok_run = os.path.join(raw, "workload-raft-r3-w50-c32-1")
    fail_run = os.path.join(raw, "scaling-epaxos-r7-w100-c32-1")

    # Successful run: metadata + requests + resources + events + phase.
    write(os.path.join(ok_run, "metadata.json"), json.dumps({
        "run_id": "workload-raft-r3-w50-c32-1",
        "experiment": "workload",
        "status": "success",
        "config": {"protocol": "raft", "replicas": 3, "read_pct": 50,
                   "write_pct": 50, "concurrency": 32,
                   "failure": {"mode": "none"}},
        "versions": {"go": "go1.26", "raft": "v1.7.3"},
        "host": {"os": "linux", "arch": "arm64", "cpus": 12},
        "started_at": "2026-09-16T00:00:00Z",
        "duration_s": 20,
    }))
    req = os.path.join(ok_run, "requests.csv")
    with open(req, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "protocol", "replicas", "read_pct", "write_pct",
                    "concurrency", "conflict_pct", "worker", "seq", "request_id",
                    "op", "key", "hot", "target", "start_ns", "end_ns",
                    "latency_ns", "ok", "error"])
        for i in range(100):
            w.writerow(["workload-raft-r3-w50-c32-1", "raft", 3, 50, 50, 32, 0,
                        0, i, i, "PUT", i, 0, 0, 1000 + i, 2000 + i, 1000, 1, ""])
    res = os.path.join(ok_run, "resources.csv")
    with open(res, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "protocol", "role", "replica_id", "container",
                    "ts_ns", "cpu_usage_usec", "cpu_user_usec", "cpu_system_usec",
                    "net_rx_bytes", "net_tx_bytes", "rss_bytes"])
        for rid in range(3):
            w.writerow(["workload-raft-r3-w50-c32-1", "raft", "replica", rid,
                        f"r{rid}", 1000, 1000, 500, 500, 10, 20, 30])
            w.writerow(["workload-raft-r3-w50-c32-1", "raft", "replica", rid,
                        f"r{rid}", 2000, 2000, 1000, 1000, 20, 40, 30])
    write(os.path.join(ok_run, "events.csv"),
          "run_id,ts_ns,event,detail\n"
          "workload-raft-r3-w50-c32-1,1000,run_started,\n"
          "workload-raft-r3-w50-c32-1,2000,phase_started,\n")
    write(os.path.join(ok_run, "phase.json"),
          json.dumps({"phase_started_ns": 1000}))

    # Failed run: metadata with status=failed, no requests.
    write(os.path.join(fail_run, "metadata.json"), json.dumps({
        "run_id": "scaling-epaxos-r7-w100-c32-1",
        "experiment": "scaling",
        "status": "failed",
        "failure_reason": "all requests failed",
        "config": {"protocol": "epaxos", "replicas": 7, "read_pct": 0,
                   "write_pct": 100, "concurrency": 32,
                   "failure": {"mode": "none"}},
        "started_at": "2026-09-16T00:01:00Z",
        "duration_s": 20,
    }))
    write(os.path.join(fail_run, "requests.csv"),
          "run_id,protocol,replicas,read_pct,write_pct,concurrency,conflict_pct,"
          "worker,seq,request_id,op,key,hot,target,start_ns,end_ns,latency_ns,ok,error\n")
    write(os.path.join(fail_run, "resources.csv"),
          "run_id,protocol,role,replica_id,container,ts_ns,cpu_usage_usec,"
          "cpu_user_usec,cpu_system_usec,net_rx_bytes,net_tx_bytes,rss_bytes\n")
    write(os.path.join(fail_run, "events.csv"),
          "run_id,ts_ns,event,detail\n"
          "scaling-epaxos-r7-w100-c32-1,1000,run_started,\n")
    write(os.path.join(fail_run, "phase.json"),
          json.dumps({"phase_started_ns": 1000}))

    # Election run: exercises the decoupled recovery metric. The isolation
    # window is 2s (target 1 x 2000ms election timeout); the first success
    # after the isolation end is at base+5000ms, so recovery_from_isolation_end
    # must be 5000 - (2000 + 2000) = 1000ms.
    elec_run = os.path.join(raw, "election-raft-r3-e1-1")
    base = 1_700_000_000_000_000_000  # plausible UnixNano epoch
    write(os.path.join(elec_run, "metadata.json"), json.dumps({
        "run_id": "election-raft-r3-e1-1",
        "experiment": "election",
        "status": "success",
        "config": {"protocol": "raft", "replicas": 3, "read_pct": 0,
                   "write_pct": 100, "concurrency": 32,
                   "failure": {"mode": "election", "at_s": 1,
                               "failed_elections": 1},
                   "raft_election_ms": 2000},
        "started_at": "2026-09-16T00:02:00Z",
        "duration_s": 20,
    }))
    req = os.path.join(elec_run, "requests.csv")
    with open(req, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "protocol", "replicas", "read_pct", "write_pct",
                    "concurrency", "conflict_pct", "worker", "seq", "request_id",
                    "op", "key", "hot", "target", "start_ns", "end_ns",
                    "latency_ns", "ok", "error"])
        # Successes before the kill (t<base+2000ms), one in-flight success
        # just after the kill (base+2010ms), failures during the outage, then
        # successes after recovery (t>=base+5000ms). The isolation window is
        # base+2000..base+4000 (target 1 x 2000ms), so the first success
        # after the isolation end is at base+5000ms ->
        # recovery_from_isolation_end = 1.0s.
        for i, (off_ms, ok) in enumerate([(1000, 1), (1500, 1), (2010, 1),
                                          (2500, 0), (3000, 0), (5000, 1),
                                          (6000, 1)]):
            end = base + off_ms * 1_000_000
            w.writerow(["election-raft-r3-e1-1", "raft", 3, 0, 100, 32, 0,
                        0, i, i, "PUT", i, 0, 0, end - 100_000, end, 100_000,
                        ok, ""])
    res = os.path.join(elec_run, "resources.csv")
    with open(res, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "protocol", "role", "replica_id", "container",
                    "ts_ns", "cpu_usage_usec", "cpu_user_usec", "cpu_system_usec",
                    "net_rx_bytes", "net_tx_bytes", "rss_bytes"])
        for rid in range(3):
            w.writerow(["election-raft-r3-e1-1", "raft", "replica", rid,
                        f"r{rid}", base, 1000, 500, 500, 10, 20, 30])
            w.writerow(["election-raft-r3-e1-1", "raft", "replica", rid,
                        f"r{rid}", base + 1_000_000_000, 2000, 1000, 1000, 20, 40, 30])
    write(os.path.join(elec_run, "events.csv"),
          "run_id,ts_ns,event,detail\n"
          f"election-raft-r3-e1-1,{base},run_started,\n"
          f"election-raft-r3-e1-1,{base},phase_started,\n"
          f"election-raft-r3-e1-1,{base + 2_000_000_000},failure_injected,container\n"
          f"election-raft-r3-e1-1,{base + 2_000_000_000},election_isolation_confirmed,replica1\n"
          f"election-raft-r3-e1-1,{base + 2_000_000_000},election_isolation_confirmed,replica2\n")
    write(os.path.join(elec_run, "phase.json"),
          json.dumps({"phase_started_ns": base}))
    write(os.path.join(elec_run, "stats.json"), json.dumps([
        {"replica": 0, "error": "dial refused"},
        {"replica": 1, "dropped_pre_votes": 2, "dropped_votes": 0},
        {"replica": 2, "dropped_pre_votes": 2, "dropped_votes": 0},
    ]))

    # A figure so the report has something to embed.
    fig = os.path.join(root, "figures", "workload")
    os.makedirs(fig, exist_ok=True)
    with open(os.path.join(fig, "throughput-50r50w.png"), "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n" + b"0" * 64)

    return raw


class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="labtest-")
        self.raw = make_dataset(self.tmp)
        self.processed = os.path.join(self.tmp, "processed")
        self.figures = os.path.join(self.tmp, "figures")
        self.report = os.path.join(self.tmp, "report")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_script(self, name, *args):
        return subprocess.run(
            [sys.executable, os.path.join(REPORT_DIR, name), *args],
            capture_output=True, text=True, cwd=LAB_DIR)

    def test_process(self):
        r = self.run_script("process.py", "--raw", self.raw,
                            "--processed", self.processed)
        self.assertEqual(r.returncode, 0, r.stderr)
        # 2 valid runs processed (workload + election); the failed run is
        # skipped but recorded.
        self.assertIn("processed 2/3 runs", r.stdout)
        self.assertTrue(os.path.exists(os.path.join(self.processed, "metrics.csv")))
        self.assertTrue(os.path.exists(os.path.join(self.processed, "run-index.json")))
        with open(os.path.join(self.processed, "run-index.json")) as f:
            idx = json.load(f)
        self.assertEqual(len(idx), 3)
        by_id = {e["run_id"]: e for e in idx}
        self.assertTrue(by_id["workload-raft-r3-w50-c32-1"]["valid"])
        self.assertFalse(by_id["scaling-epaxos-r7-w100-c32-1"]["valid"])
        self.assertTrue(by_id["election-raft-r3-e1-1"]["valid"])

    def test_experiment_families_are_not_conflated(self):
        """workload, concurrency, conflict (at conflict_pct 0) and scaling-r3
        share every numeric parameter, so a configuration key without the
        experiment family merges four different experiments into one
        aggregate -- and the run-selection rule then silently drops runs.
        """
        sys.path.insert(0, REPORT_DIR)
        import process as P

        base = dict(protocol="raft", replicas=3, read_pct=0, write_pct=100,
                    concurrency=32, conflict_pct=0, failure_mode="none",
                    failed_elections_target=0)
        families = ("workload", "concurrency", "conflict", "scaling")
        self.assertEqual(len({P.config_key(dict(base, experiment=f)) for f in families}),
                         len(families))

        rows = [dict(base, experiment=f, run_id=f"{f}-raft-r3-w100-c32-1",
                     repetition=1, attempt=1, contaminated=False)
                for f in families]
        out, excluded = P.mark_included(rows)
        self.assertEqual(excluded, 0)
        self.assertTrue(all(r["included"] == 1 for r in out))

    def test_election_metric(self):
        """The decoupled recovery metric must be computed from the isolation
        end, not from the kill: first success after isolation end minus the
        isolation expiry."""
        self.run_script("process.py", "--raw", self.raw,
                        "--processed", self.processed)
        with open(os.path.join(self.processed, "failures.json")) as f:
            failures = json.load(f)
        elec = [x for x in failures if "election" in x.get("run_id", "")]
        self.assertEqual(len(elec), 1)
        f = elec[0]
        # Isolation end = max(confirmed) + 1 x 2000ms = 2000 + 2000 = 4000.
        # First success at/after 4000 is at 5000 -> recovery = 1.0s.
        self.assertEqual(f["recovery_from_isolation_end_s"], 1.0)
        # Measured elections = (2+2+0+0) / (3-1) = 2.
        self.assertEqual(f["measured_elections"], 2)
        # The availability gap (largest no-success interval with the earlier
        # success at/after the kill) is 2010..5000 = 2.99s.
        self.assertAlmostEqual(f["write_availability_gap_s_approx"], 2.99)

    def test_html(self):
        self.run_script("process.py", "--raw", self.raw,
                        "--processed", self.processed)
        r = self.run_script("html.py", "--processed", self.processed,
                            "--raw", self.raw, "--figures", self.figures,
                            "--config", os.path.join(LAB_DIR, "configs", "matrix.json"),
                            "--out", self.report)
        self.assertEqual(r.returncode, 0, r.stderr)
        html = open(os.path.join(self.report, "index.html")).read()
        # Summary must be derived from the dataset, not hardcoded.
        self.assertIn("Total runs", html)
        self.assertIn(">2<", html)  # 2 runs
        self.assertIn(">1<", html)  # 1 successful
        # The failed run must appear explicitly.
        self.assertIn("scaling-epaxos-r7-w100-c32-1", html)
        self.assertIn("FAILED", html)
        # Figure reference must resolve.
        self.assertIn("throughput-50r50w.png", html)

    def test_validate(self):
        self.run_script("process.py", "--raw", self.raw,
                        "--processed", self.processed)
        self.run_script("html.py", "--processed", self.processed,
                        "--raw", self.raw, "--figures", self.figures,
                        "--config", os.path.join(LAB_DIR, "configs", "matrix.json"),
                        "--out", self.report)
        r = self.run_script("validate_html.py", "--report",
                            os.path.join(self.report, "index.html"),
                            "--processed", self.processed,
                            "--raw", self.raw, "--figures", self.figures)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("PASS", r.stdout)


if __name__ == "__main__":
    unittest.main()