#!/usr/bin/env python3
"""Merged null-result figure: throughput vs write ratio and vs conflict ratio.

Both sweeps are at 3 replicas, concurrency 32, and both are null results,
so they share one stacked single-column figure (the same pattern as
fig:delay-merged). Both labels live in the single float, so no text
reference changes.

Usage:
    .venv/bin/python report/ratio_merged_figure.py \
        --processed results/processed --out results/figures
"""
import argparse
import csv
import os
import statistics
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

sys.path.insert(0, _script_dir)
from figures import fnum, select_included, save_pub  # noqa: E402


def load(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def plot_panel(ax, rows, xkey, xlabel):
    groups = {}
    for r in rows:
        p = r["protocol"]
        x = xkey(r)
        v = fnum(r["throughput_req_s"])
        if v is None:
            continue
        groups.setdefault(p, {}).setdefault(x, []).append(v)
    for p in ("raft", "epaxos"):
        if p not in groups:
            continue
        xs = sorted(groups[p])
        meds = [statistics.median(groups[p][x]) for x in xs]
        color = "#1f77b4" if p == "raft" else "#d62728"
        marker = "o" if p == "raft" else "s"
        label = "Raft" if p == "raft" else "EPaxos"
        ax.plot(xs, meds, marker=marker, color=color, label=label, linestyle="-")
        for x in xs:
            ax.scatter([x] * len(groups[p][x]), groups[p][x], color=color,
                       s=10, alpha=0.45, zorder=3)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Throughput (req/s)")
    ax.legend(fontsize=7)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="results/processed")
    ap.add_argument("--out", default="results/figures")
    args = ap.parse_args()

    metrics = select_included(load(os.path.join(args.processed, "metrics.csv")))

    wr = [m for m in metrics if m["experiment"] == "workload"
          and m["concurrency"] == "32" and m["failure_mode"] == "none"]
    cr = [m for m in metrics if m["experiment"] == "conflict"
          and m["failure_mode"] == "none"]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(3.4, 4.6), sharex=False)
    plot_panel(ax1, wr, lambda m: int(m["write_pct"]), "Write ratio (%)")
    plot_panel(ax2, cr, lambda m: int(m["conflict_pct"]), "Conflict ratio (%)")
    fig.tight_layout()
    save_pub(fig, os.path.join(args.out, "throughput-ratios"))


if __name__ == "__main__":
    main()