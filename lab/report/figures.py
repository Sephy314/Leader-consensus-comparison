#!/usr/bin/env python3
"""Generate figures from processed metrics.

Every figure is generated programmatically from measured data in
results/processed/. A figure is only produced if the underlying experiment
produced valid data. No values are entered manually.

Usage:
    python3 report/figures.py --processed results/processed --figures results/figures
"""
import argparse
import csv
import json
import os
import sys

# report/html.py shadows the stdlib `html` package for any script run from
# this directory (sys.path[0] = report/). matplotlib's pyparsing dependency
# imports html.entities, so drop the script directory from sys.path before
# importing matplotlib to let the stdlib html resolve.
_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path = [p for p in sys.path if os.path.abspath(p) != _script_dir]

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"figure.dpi": 150, "font.size": 9})


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_metrics(processed):
    path = os.path.join(processed, "metrics.csv")
    if not os.path.exists(path):
        return []
    return read_csv(path)


def load_resources(processed):
    path = os.path.join(processed, "resources.csv")
    if not os.path.exists(path):
        return []
    return read_csv(path)


def load_failures(processed):
    path = os.path.join(processed, "failures.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def save(fig, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {path}")


def workload_figures(metrics, outdir):
    """Throughput and latency vs concurrency, per protocol and R/W mix."""
    rows = [m for m in metrics if m["failure_mode"] == "none" and m["replicas"] == "3"]
    if not rows:
        return
    mixes = sorted({(m["read_pct"], m["write_pct"]) for m in rows})
    for rp, wp in mixes:
        sub = [m for m in rows if m["read_pct"] == rp]
        protos = sorted({m["protocol"] for m in sub})
        if len(protos) < 2:
            continue
        # Throughput vs concurrency.
        fig, ax = plt.subplots()
        for p in protos:
            pts = sorted(
                ((int(m["concurrency"]), fnum(m["throughput_req_s"])) for m in sub if m["protocol"] == p),
                key=lambda kv: kv[0],
            )
            pts = [(c, t) for c, t in pts if t is not None]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=p)
        ax.set_xlabel("concurrency")
        ax.set_ylabel("throughput (req/s)")
        ax.set_title(f"throughput vs concurrency — {rp}R/{wp}W, 3 replicas")
        ax.legend()
        ax.set_xscale("log", base=2)
        save(fig, os.path.join(outdir, "workload", f"throughput-{rp}r{wp}w.png"))

        # Latency vs concurrency (p50/p95/p99).
        for pct in ("p50", "p95", "p99"):
            fig, ax = plt.subplots()
            for p in protos:
                pts = sorted(
                    (
                        (int(m["concurrency"]), fnum(m[f"latency_ns_{pct}"]))
                        for m in sub
                        if m["protocol"] == p
                    ),
                    key=lambda kv: kv[0],
                )
                pts = [(c, t / 1e6) for c, t in pts if t is not None]
                if pts:
                    ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=p)
            ax.set_xlabel("concurrency")
            ax.set_ylabel(f"latency {pct} (ms)")
            ax.set_title(f"latency {pct} vs concurrency — {rp}R/{wp}W, 3 replicas")
            ax.legend()
            ax.set_xscale("log", base=2)
            save(fig, os.path.join(outdir, "workload", f"latency-{pct}-{rp}r{wp}w.png"))


def scaling_figures(metrics, outdir):
    """Throughput and latency vs replica count (100% write)."""
    rows = [
        m
        for m in metrics
        if m["failure_mode"] == "none" and m["write_pct"] == "100" and m["concurrency"] == "32"
    ]
    if not rows:
        return
    protos = sorted({m["protocol"] for m in rows})
    for metric, ylabel, fname in (
        ("throughput_req_s", "throughput (req/s)", "throughput"),
        ("latency_ns_p50", "latency p50 (ms)", "latency-p50"),
        ("latency_ns_p95", "latency p95 (ms)", "latency-p95"),
    ):
        fig, ax = plt.subplots()
        for p in protos:
            pts = sorted(
                ((int(m["replicas"]), fnum(m[metric])) for m in rows if m["protocol"] == p),
                key=lambda kv: kv[0],
            )
            pts = [(n, v / 1e6 if "latency" in metric else v) for n, v in pts if v is not None]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=p)
        ax.set_xlabel("replicas")
        ax.set_ylabel(ylabel)
        ax.set_title(f"{fname} vs replica count — 100% write, concurrency 32")
        ax.legend()
        save(fig, os.path.join(outdir, "scaling", f"{fname}.png"))


def resource_figures(resources, outdir):
    """Per-replica CPU and network distribution."""
    if not resources:
        return
    # Group by (protocol, replicas, run_id) — use the first run per config.
    seen = set()
    for r in resources:
        key = (r["protocol"], r["replicas"], r["run_id"])
        if key in seen:
            continue
        seen.add(key)
        run_res = [x for x in resources if x["run_id"] == r["run_id"]]
        run_res.sort(key=lambda x: int(x["replica_id"]))
        ids = [int(x["replica_id"]) for x in run_res]
        cpu = [fnum(x["cpu_util_pct"]) or 0 for x in run_res]
        rx = [fnum(x["net_rx_bps"]) or 0 for x in run_res]
        tx = [fnum(x["net_tx_bps"]) or 0 for x in run_res]

        fig, axes = plt.subplots(1, 3, figsize=(12, 3.5))
        axes[0].bar(ids, cpu)
        axes[0].set_title("CPU util %")
        axes[0].set_xlabel("replica")
        axes[1].bar(ids, [v / 1e3 for v in rx])
        axes[1].set_title("net RX (kB/s)")
        axes[1].set_xlabel("replica")
        axes[2].bar(ids, [v / 1e3 for v in tx])
        axes[2].set_title("net TX (kB/s)")
        axes[2].set_xlabel("replica")
        fig.suptitle(f"per-replica resources — {r['protocol']} r{r['replicas']} ({r['run_id']})")
        save(fig, os.path.join(outdir, "resources", f"{r['run_id']}.png"))


def failure_figures(failures, processed, outdir):
    """Failure timeline: request success/failure, throughput, latency over time."""
    if not failures:
        return
    for f in failures:
        run_id = f["run_id"]
        # Reconstruct per-second aggregates from raw requests.
        req_path = os.path.join(os.path.dirname(processed), "raw", run_id, "requests.csv")
        if not os.path.exists(req_path):
            continue
        rows = read_csv(req_path)
        if not rows:
            continue
        t0 = None
        phase_path = os.path.join(os.path.dirname(processed), "raw", run_id, "phase.json")
        if os.path.exists(phase_path):
            with open(phase_path) as fh:
                t0 = json.load(fh).get("phase_started_ns")
        if t0 is None:
            continue

        buckets = {}
        for r in rows:
            sec = int((int(r["end_ns"]) - t0) / 1e9)
            b = buckets.setdefault(sec, {"ok": 0, "fail": 0, "lats": []})
            if r["ok"] == "1":
                b["ok"] += 1
                b["lats"].append(int(r["latency_ns"]))
            else:
                b["fail"] += 1
        secs = sorted(buckets)
        ok = [buckets[s]["ok"] for s in secs]
        fail = [buckets[s]["fail"] for s in secs]
        total = [ok[i] + fail[i] for i in range(len(secs))]
        p95 = [
            sorted(buckets[s]["lats"])[int(0.95 * (len(buckets[s]["lats"]) - 1))] / 1e6
            if buckets[s]["lats"]
            else 0
            for s in secs
        ]

        fig, axes = plt.subplots(3, 1, figsize=(9, 9), sharex=True)
        axes[0].bar(secs, ok, label="ok", color="tab:green")
        axes[0].bar(secs, fail, bottom=ok, label="failed", color="tab:red")
        axes[0].set_ylabel("requests/s")
        axes[0].legend()
        axes[0].set_title(f"failure timeline — {run_id}")
        axes[1].plot(secs, total, marker=".", label="throughput")
        axes[1].set_ylabel("throughput (req/s)")
        axes[1].legend()
        axes[2].plot(secs, p95, marker=".", label="p95 latency")
        axes[2].set_ylabel("p95 latency (ms)")
        axes[2].set_xlabel("seconds since phase start")
        axes[2].legend()

        # Mark failure injection and recovery events.
        tl = f.get("timeline", {})
        for ev, color in (("failure_injected", "red"), ("restart_confirmed", "blue")):
            if ev in tl and tl[ev].get("rel_s") is not None:
                for ax in axes:
                    ax.axvline(tl[ev]["rel_s"], color=color, linestyle="--", alpha=0.7)
        save(fig, os.path.join(outdir, "failure", f"{run_id}.png"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="results/processed")
    ap.add_argument("--figures", default="results/figures")
    args = ap.parse_args()

    metrics = load_metrics(args.processed)
    resources = load_resources(args.processed)
    failures = load_failures(args.processed)

    print("generating figures from processed data ...")
    workload_figures(metrics, args.figures)
    scaling_figures(metrics, args.figures)
    resource_figures(resources, args.figures)
    failure_figures(failures, args.processed, args.figures)
    print("done")


if __name__ == "__main__":
    main()