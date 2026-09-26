#!/usr/bin/env python3
"""Extract the exact numbers the paper's Evaluation section reports, from the
processed dataset. Run after the full matrix completes:

    .venv/bin/python report/paper_numbers.py --processed results/processed

Outputs a markdown table with every number that appears in
paper/sections/04-evaluation.tex, computed from the raw per-run observations.
No value is hardcoded; if the dataset changes, the numbers change.
"""
import argparse
import csv
import os
import statistics
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stats as labstats


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def mean(vals):
    vals = [v for v in vals if v is not None]
    return statistics.mean(vals) if vals else None


def median(vals):
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None


def minmax(vals):
    vals = [v for v in vals if v is not None]
    return (min(vals), max(vals)) if vals else (None, None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="results/processed")
    args = ap.parse_args()

    metrics = read_csv(os.path.join(args.processed, "metrics.csv"))
    # Same selection rule as the figures (process.py: mark_included): a paper
    # number must never be computed from a run whose host telemetry was
    # contaminated, or from an attempt superseded by a replacement.
    metrics = [m for m in metrics if str(m.get("included")) == "1"]
    failures = []
    fp = os.path.join(args.processed, "failures.json")
    if os.path.exists(fp):
        import json
        with open(fp) as f:
            failures = json.load(f)

    out = []
    def line(s=""):
        out.append(s)

    line("# Paper numbers (from processed dataset)")
    line("")
    line(f"Generated from {len(metrics)} valid runs. "
         f"All values computed from raw per-run observations.")
    line("")

    # --- Scaling table (Table 1) ---
    line("## Table 1: Throughput by replica count (100% write, c32) [mean of the ten repetitions]")
    line("")
    line("The paper reports the median of the same values; compare with")
    line("`results/processed/config-summary.json` before flagging a mismatch.")
    line("| Replicas | Raft mean | EPaxos mean | EPaxos/Raft |")
    line("|---|---|---|---|")
    for n in ("3", "5", "7", "9"):
        raft = [fnum(m["throughput_req_s"]) for m in metrics
                if m["protocol"] == "raft" and m["replicas"] == n
                and m["failure_mode"] == "none" and m["concurrency"] == "32"
                and m["experiment"] == "scaling"]
        epax = [fnum(m["throughput_req_s"]) for m in metrics
                if m["protocol"] == "epaxos" and m["replicas"] == n
                and m["failure_mode"] == "none" and m["concurrency"] == "32"
                and m["experiment"] == "scaling"]
        mr, me = mean(raft), mean(epax)
        ratio = me / mr if mr and me else None
        line(f"| {n} | {mr:,.0f} | {me:,.0f} | {ratio:.2f}x |")
    line("")

    # --- Latency table (Table 2) ---
    line("## Table 2: p50 latency by replica count (100% write, c32) [mean of the ten per-run p50 values]")
    line("")
    line("The paper reports the median of the same per-run p50 values.")
    line("| Replicas | Raft p50 (ms) | EPaxos p50 (ms) |")
    line("|---|---|---|")
    for n in ("3", "5", "7", "9"):
        raft = [fnum(m["latency_ns_p50"]) / 1e6 for m in metrics
                if m["protocol"] == "raft" and m["replicas"] == n
                and m["failure_mode"] == "none" and m["concurrency"] == "32"
                and m["experiment"] == "scaling"]
        epax = [fnum(m["latency_ns_p50"]) / 1e6 for m in metrics
                if m["protocol"] == "epaxos" and m["replicas"] == n
                and m["failure_mode"] == "none" and m["concurrency"] == "32"
                and m["experiment"] == "scaling"]
        line(f"| {n} | {mean(raft):.1f} | {mean(epax):.1f} |")
    line("")

    # --- Concurrency sweep ---
    line("## Concurrency sweep (3 replicas, 100% write)")
    line("")
    for c in ("1", "32", "64", "256"):
        raft = [fnum(m["throughput_req_s"]) for m in metrics
                if m["protocol"] == "raft" and m["concurrency"] == c
                and m["failure_mode"] == "none" and m["replicas"] == "3"
                and m["experiment"] == "concurrency"]
        epax = [fnum(m["throughput_req_s"]) for m in metrics
                if m["protocol"] == "epaxos" and m["concurrency"] == c
                and m["failure_mode"] == "none" and m["replicas"] == "3"
                and m["experiment"] == "concurrency"]
        raft_lat = [fnum(m["latency_ns_p50"]) / 1e6 for m in metrics
                    if m["protocol"] == "raft" and m["concurrency"] == c
                    and m["failure_mode"] == "none" and m["replicas"] == "3"
                    and m["experiment"] == "concurrency"]
        epax_lat = [fnum(m["latency_ns_p50"]) / 1e6 for m in metrics
                    if m["protocol"] == "epaxos" and m["concurrency"] == c
                    and m["failure_mode"] == "none" and m["replicas"] == "3"
                    and m["experiment"] == "concurrency"]
        line(f"- c{c}: Raft {mean(raft):,.0f} req/s (p50 {mean(raft_lat):.1f} ms), "
             f"EPaxos {mean(epax):,.0f} req/s (p50 {mean(epax_lat):.1f} ms)")
    # Max per-run values at c256.
    for proto in ("raft", "epaxos"):
        vals = [fnum(m["throughput_req_s"]) for m in metrics
                if m["protocol"] == proto and m["concurrency"] == "256"
                and m["failure_mode"] == "none" and m["replicas"] == "3"
                and m["experiment"] == "concurrency"]
        line(f"- c256 max per-run {proto}: {max(vals):,.0f} req/s")
    line("")

    # --- Write-ratio sweep ---
    line("## Write-ratio sweep (3 replicas, c32)")
    line("")
    for proto in ("raft", "epaxos"):
        vals = [fnum(m["throughput_req_s"]) for m in metrics
                if m["protocol"] == proto and m["concurrency"] == "32"
                and m["failure_mode"] == "none" and m["replicas"] == "3"
                and m["experiment"] == "workload"]
        lo, hi = minmax(vals)
        line(f"- {proto}: {lo:,.0f} to {hi:,.0f} req/s across write ratios")
    line("")

    # --- Conflict sweep ---
    line("## Conflict sweep (3 replicas, 100% write, c32)")
    line("")
    for proto in ("raft", "epaxos"):
        vals = [fnum(m["throughput_req_s"]) for m in metrics
                if m["protocol"] == proto and m["failure_mode"] == "none"
                and m["experiment"] == "conflict"]
        lo, hi = minmax(vals)
        line(f"- {proto}: {lo:,.0f} to {hi:,.0f} req/s across conflict ratios")
    line("")

    # --- Resource utilisation ---
    line("## Resource utilisation (all runs)")
    line("")
    resources = read_csv(os.path.join(args.processed, "resources.csv"))
    for proto in ("raft", "epaxos"):
        cpu = [fnum(r["cpu_util_pct"]) for r in resources if r["protocol"] == proto]
        rss = [fnum(r["rss_bytes_max"]) / 1e6 for r in resources if r["protocol"] == proto]
        rx = [fnum(r["net_rx_bps"]) / 1e3 for r in resources if r["protocol"] == proto]
        tx = [fnum(r["net_tx_bps"]) / 1e3 for r in resources if r["protocol"] == proto]
        line(f"- {proto}: CPU {mean(cpu):.1f}%, RSS {mean(rss):.1f} MB, "
             f"RX {mean(rx):,.0f} kB/s, TX {mean(tx):,.0f} kB/s")
    line("")

    # --- Per-replica CPU in 9-replica scaling runs ---
    line("## Per-replica CPU, 9-replica scaling runs")
    line("")
    for proto in ("raft", "epaxos"):
        vals = [fnum(r["cpu_util_pct"]) for r in resources
                if r["protocol"] == proto and r["replicas"] == "9"
                and r["experiment"] == "scaling"]
        per_replica = defaultdict(list)
        for r in resources:
            if (r["protocol"] == proto and r["replicas"] == "9"
                    and r["experiment"] == "scaling"):
                v = fnum(r["cpu_util_pct"])
                if v is not None:
                    per_replica[r["replica_id"]].append(v)
        means = {rid: statistics.mean(v) for rid, v in per_replica.items()}
        lo, hi = minmax(vals)
        mlo, mhi = minmax(means.values())
        line(f"- {proto}: per-replica means {mlo:.1f}% to {mhi:.1f}%; "
             f"individual observations {lo:.1f}% to {hi:.1f}%")
    line("")

    # --- Failure table (Table 3) ---
    line("## Table 3: Failure behaviour (3 replicas, 100% write, c32)")
    line("")
    line("| Failure | Protocol | Gap min (s) | Gap max (s) | Failed req min | Failed req max |")
    line("|---|---|---|---|---|---|")
    for mode, proto in (("leader", "raft"), ("follower", "raft"), ("replica", "epaxos")):
        gaps = [fnum(f["write_availability_gap_s_approx"]) for f in failures
                if f.get("failure_mode") == mode and str(f.get("included")) == "1"]
        failed = [int(m["requests_failed"]) for m in metrics
                  if m["failure_mode"] == mode and m["protocol"] == proto]
        glo, ghi = minmax(gaps)
        flo, fhi = minmax(failed)
        line(f"| {mode} kill | {proto} | {glo:.2f} | {ghi:.2f} | {flo:,} | {fhi:,} |")
    line("")

    # --- Election correlations ---
    line("## Election experiment")
    line("")
    elec = [f for f in failures
            if f.get("failure_mode") == "election" and str(f.get("included")) == "1"]
    xs = [f.get("measured_elections") for f in elec]
    yg = [fnum(f.get("write_availability_gap_s_approx")) for f in elec]
    yr = [fnum(f.get("recovery_from_isolation_end_s")) for f in elec]
    rg = labstats.pearson(xs, yg)
    pg = labstats.pearson_p(rg, len(xs)) if rg is not None else None
    rr = labstats.pearson(xs, yr)
    pr = labstats.pearson_p(rr, len(xs)) if rr is not None else None
    line(f"- n={len(xs)}")
    line(f"- Gap vs measured elections: r={rg:.3f}, p={pg:.4f}")
    line(f"- Recovery-from-isolation-end vs measured elections: r={rr:.3f}, p={pr:.4f}")
    line("")

    print("\n".join(out))


if __name__ == "__main__":
    sys.exit(main())