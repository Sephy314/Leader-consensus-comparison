#!/usr/bin/env python3
"""Publication figure for the Raft high-concurrency scaling follow-up.

Merges the recorded c32 scaling family (results/processed) with the
scaling-conc family (results/scaling-conc/processed): median Raft throughput
vs replica count, one series per concurrency level (32, 128, 256).

Usage:
    .venv/bin/python report/scaling_conc_figure.py \
        --primary results/processed --new results/scaling-conc/processed \
        --out results/scaling-conc/figures
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

COLORS = {"32": "#1f77b4", "128": "#d62728", "256": "#2ca02c"}
MARKERS = {"32": "o", "128": "s", "256": "^"}
LABELS = {"32": "c32", "128": "c128", "256": "c256"}


def load(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", default="results/processed")
    ap.add_argument("--new", default="results/scaling-conc/processed")
    ap.add_argument("--out", default="results/scaling-conc/figures")
    args = ap.parse_args()

    primary = select_included(load(os.path.join(args.primary, "metrics.csv")))
    new = select_included(load(os.path.join(args.new, "metrics.csv")))

    # c32 from the recorded scaling family; c128/c256 from the follow-up.
    rows = {c: [] for c in ("32", "128", "256")}
    for m in primary:
        if m["experiment"] == "scaling" and m["protocol"] == "raft" \
                and m["concurrency"] == "32":
            rows["32"].append(m)
    for m in new:
        if m["experiment"] == "scaling-conc" and m["protocol"] == "raft":
            rows[m["concurrency"]].append(m)

    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    for c in ("32", "128", "256"):
        xs = sorted({int(m["replicas"]) for m in rows[c]})
        meds = [statistics.median(fnum(m["throughput_req_s"])
                                  for m in rows[c] if int(m["replicas"]) == x)
                for x in xs]
        ax.plot(xs, meds, marker=MARKERS[c], color=COLORS[c],
                label=LABELS[c], linestyle="-")
        for x in xs:
            vals = [fnum(m["throughput_req_s"]) for m in rows[c]
                    if int(m["replicas"]) == x]
            ax.scatter([x] * len(vals), vals, color=COLORS[c], s=10,
                       alpha=0.45, zorder=3)
    ax.set_xticks([3, 5, 7, 9])
    ax.set_xlabel("Replicas")
    ax.set_ylabel("Throughput (req/s)")
    ax.legend(title="Concurrency")
    fig.tight_layout()
    save_pub(fig, os.path.join(args.out, "throughput-scaling-conc"))


if __name__ == "__main__":
    main()