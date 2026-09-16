#!/usr/bin/env python3
"""Process raw experiment results into aggregated metrics.

Pipeline: raw -> validation -> processing -> aggregated metrics -> figures.

Raw data is never modified. Every run directory under results/raw/ is
validated; failed runs are recorded in the index and excluded from
aggregation (but never deleted). Aggregated metrics are written to
results/processed/ as CSV/JSON, and figures are generated from those metrics
by figures.py.

Usage:
    python3 report/process.py --raw results/raw --processed results/processed
"""
import argparse
import csv
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone


def load_metadata(run_dir):
    path = os.path.join(run_dir, "metadata.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    idx = int(p * (len(sorted_vals) - 1))
    return sorted_vals[idx]


def validate_run(run_dir, run_id):
    """Return (ok, reason). A run is valid if it has metadata, requests with
    at least one success, and resource samples."""
    meta = load_metadata(run_dir)
    if meta is None:
        return False, "metadata.json missing"
    if meta.get("status") != "success":
        return False, f"status={meta.get('status')} {meta.get('failure_reason') or ''}"
    req_path = os.path.join(run_dir, "requests.csv")
    if not os.path.exists(req_path):
        return False, "requests.csv missing"
    rows = read_csv(req_path)
    if not rows:
        return False, "requests.csv empty"
    ok = sum(1 for r in rows if r.get("ok") == "1")
    if ok == 0:
        return False, "no successful requests"
    res_path = os.path.join(run_dir, "resources.csv")
    if not os.path.exists(res_path) or os.path.getsize(res_path) == 0:
        return False, "resources.csv missing/empty"
    return True, ""


def process_run(run_dir, run_id):
    """Compute aggregated metrics for one valid run."""
    meta = load_metadata(run_dir)
    cfg = meta["config"]
    rows = read_csv(os.path.join(run_dir, "requests.csv"))

    lats = sorted(int(r["latency_ns"]) for r in rows)
    ok_rows = [r for r in rows if r.get("ok") == "1"]
    ok_lats = sorted(int(r["latency_ns"]) for r in ok_rows)

    # Throughput over the measured phase (from phase.json if present).
    phase_path = os.path.join(run_dir, "phase.json")
    phase_s = None
    if os.path.exists(phase_path):
        with open(phase_path) as f:
            phase = json.load(f)
        start = phase.get("phase_started_ns")
        if start and rows:
            end = max(int(r["end_ns"]) for r in rows)
            phase_s = (end - start) / 1e9

    total = len(rows)
    ok = len(ok_rows)
    duration = phase_s or (cfg.get("duration_s") or 0)
    throughput = ok / duration if duration and duration > 0 else None

    def pct(vals, p):
        return percentile(vals, p)

    metrics = {
        "run_id": run_id,
        "protocol": cfg["protocol"],
        "replicas": cfg["replicas"],
        "read_pct": cfg["read_pct"],
        "write_pct": cfg["write_pct"],
        "concurrency": cfg["concurrency"],
        "conflict_pct": cfg.get("conflict_pct", 0),
        "failure_mode": cfg["failure"]["mode"],
        "status": "success",
        "requests_total": total,
        "requests_ok": ok,
        "requests_failed": total - ok,
        "success_rate": ok / total if total else None,
        "throughput_req_s": throughput,
        "latency_ns_p50": pct(ok_lats, 0.50),
        "latency_ns_p95": pct(ok_lats, 0.95),
        "latency_ns_p99": pct(ok_lats, 0.99),
        "latency_ns_mean": statistics.mean(ok_lats) if ok_lats else None,
        "latency_ns_p50_all": pct(lats, 0.50),
        "latency_ns_p95_all": pct(lats, 0.95),
        "latency_ns_p99_all": pct(lats, 0.99),
        "phase_duration_s": duration,
    }
    return metrics


def process_resources(run_dir, run_id):
    """Aggregate per-replica resource samples (raw samples are preserved in
    results/raw; this only summarizes them)."""
    meta = load_metadata(run_dir)
    cfg = meta["config"]
    path = os.path.join(run_dir, "resources.csv")
    rows = read_csv(path)
    by_replica = defaultdict(list)
    for r in rows:
        if r.get("role") == "replica":
            by_replica[r["replica_id"]].append(r)

    out = []
    for rid, samples in sorted(by_replica.items(), key=lambda kv: int(kv[0])):
        samples.sort(key=lambda s: int(s["ts_ns"]))
        if len(samples) < 2:
            continue
        t0 = int(samples[0]["ts_ns"])
        t1 = int(samples[-1]["ts_ns"])
        span = (t1 - t0) / 1e9
        cpu0 = int(samples[0]["cpu_usage_usec"])
        cpu1 = int(samples[-1]["cpu_usage_usec"])
        rx0 = int(samples[0]["net_rx_bytes"])
        rx1 = int(samples[-1]["net_rx_bytes"])
        tx0 = int(samples[0]["net_tx_bytes"])
        tx1 = int(samples[-1]["net_tx_bytes"])
        rss = max(int(s["rss_bytes"]) for s in samples)
        out.append({
            "run_id": run_id,
            "protocol": cfg["protocol"],
            "replicas": cfg["replicas"],
            "replica_id": int(rid),
            "samples": len(samples),
            "span_s": span,
            "cpu_usage_usec_total": cpu1 - cpu0,
            "cpu_util_pct": (cpu1 - cpu0) / 1e6 / span * 100 if span > 0 else None,
            "net_rx_bytes_total": rx1 - rx0,
            "net_tx_bytes_total": tx1 - tx0,
            "net_rx_bps": (rx1 - rx0) / span if span > 0 else None,
            "net_tx_bps": (tx1 - tx0) / span if span > 0 else None,
            "rss_bytes_max": rss,
        })
    return out


def process_failure(run_dir, run_id):
    """Extract the failure/recovery timeline from events.csv and requests.csv."""
    ev_path = os.path.join(run_dir, "events.csv")
    if not os.path.exists(ev_path):
        return None
    events = read_csv(ev_path)
    if not any(e["event"] == "failure_injected" for e in events):
        return None

    phase_path = os.path.join(run_dir, "phase.json")
    t0 = None
    if os.path.exists(phase_path):
        with open(phase_path) as f:
            t0 = json.load(f).get("phase_started_ns")

    timeline = {}
    for e in events:
        ts = int(e["ts_ns"])
        rel = (ts - t0) / 1e9 if t0 else None
        timeline[e["event"]] = {"ts_ns": ts, "rel_s": rel, "detail": e["detail"]}

    rows = read_csv(os.path.join(run_dir, "requests.csv"))
    fail_ts = timeline.get("failure_injected", {}).get("ts_ns")

    # Availability gap: the longest interval with NO successful completion,
    # starting at or after the failure. In-flight requests that complete just
    # after the kill are genuine successes and do not end the gap; the gap is
    # the quiet period between the last success before it and the first
    # success after it.
    gap_s = None
    gap_start_rel = None
    gap_end_rel = None
    if fail_ts and t0:
        success_ends = sorted(
            int(r["end_ns"]) for r in rows if r.get("ok") == "1"
        )
        best = 0
        best_pair = None
        for i in range(1, len(success_ends)):
            prev, nxt = success_ends[i - 1], success_ends[i]
            if prev >= fail_ts and (nxt - prev) > best:
                best = nxt - prev
                best_pair = (prev, nxt)
        # If no successes at all after the failure, the gap runs from the
        # failure to the end of the measured phase.
        if best_pair is None and not any(e >= fail_ts for e in success_ends):
            last_before = max((e for e in success_ends if e < fail_ts), default=fail_ts)
            end = max(int(r["end_ns"]) for r in rows)
            best = end - last_before
            best_pair = (last_before, end)
        if best_pair:
            gap_s = best / 1e9
            gap_start_rel = (best_pair[0] - t0) / 1e9
            gap_end_rel = (best_pair[1] - t0) / 1e9

    # Degradation onset: first failed request at or after the failure.
    degrade_rel = None
    for r in sorted(rows, key=lambda r: int(r["end_ns"])):
        if r.get("ok") != "1" and int(r["end_ns"]) >= (fail_ts or 0):
            degrade_rel = (int(r["end_ns"]) - t0) / 1e9 if t0 else None
            break

    return {
        "run_id": run_id,
        "timeline": timeline,
        "write_availability_gap_s_approx": gap_s,
        "gap_start_rel_s": gap_start_rel,
        "gap_end_rel_s": gap_end_rel,
        "degradation_onset_rel_s": degrade_rel,
        "gap_note": (
            "approximate: the largest interval with no successful completion "
            "at or after failure_injected; resolution is limited by request "
            "timing and the ~200ms resource monitor"
        ),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="results/raw")
    ap.add_argument("--processed", default="results/processed")
    args = ap.parse_args()

    os.makedirs(args.processed, exist_ok=True)

    run_dirs = sorted(
        d for d in os.listdir(args.raw)
        if os.path.isdir(os.path.join(args.raw, d))
    )

    all_metrics = []
    all_resources = []
    all_failures = []
    index = []
    valid = 0
    for run_id in run_dirs:
        run_dir = os.path.join(args.raw, run_id)
        ok, reason = validate_run(run_dir, run_id)
        index.append({"run_id": run_id, "valid": ok, "reason": reason})
        if not ok:
            print(f"  skip {run_id}: {reason}")
            continue
        valid += 1
        m = process_run(run_dir, run_id)
        all_metrics.append(m)
        all_resources.extend(process_resources(run_dir, run_id))
        f = process_failure(run_dir, run_id)
        if f:
            all_failures.append(f)

    # Write aggregated metrics.
    with open(os.path.join(args.processed, "metrics.csv"), "w", newline="") as f:
        if all_metrics:
            w = csv.DictWriter(f, fieldnames=list(all_metrics[0].keys()))
            w.writeheader()
            w.writerows(all_metrics)
    with open(os.path.join(args.processed, "resources.csv"), "w", newline="") as f:
        if all_resources:
            w = csv.DictWriter(f, fieldnames=list(all_resources[0].keys()))
            w.writeheader()
            w.writerows(all_resources)
    with open(os.path.join(args.processed, "failures.json"), "w") as f:
        json.dump(all_failures, f, indent=2)
    with open(os.path.join(args.processed, "run-index.json"), "w") as f:
        json.dump(index, f, indent=2)

    print(f"processed {valid}/{len(run_dirs)} runs")
    print(f"  metrics: {os.path.join(args.processed, 'metrics.csv')}")
    print(f"  resources: {os.path.join(args.processed, 'resources.csv')}")
    print(f"  failures: {os.path.join(args.processed, 'failures.json')}")


if __name__ == "__main__":
    sys.exit(main())