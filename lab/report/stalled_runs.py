#!/usr/bin/env python3
"""List repetitions that must be replaced because they stalled.

A small number of repetitions complete far fewer requests than their own
configuration's median without failing a request and without tripping the
host-telemetry contamination rule: the run is slow, but the host load average
stays below the starvation threshold, no telemetry gap exceeds the pause
threshold and the host was never suspended. The recorded suite describes the
same signature ("slow from start to finish with no failed requests").

The selection rule is fixed here, in one place, and applied uniformly to every
condition of a dataset:

    an included repetition whose measured throughput is below `--below` times
    the median of its own configuration (over the included repetitions of that
    configuration) is listed for replacement.

The list is consumed by `runner matrix --rerun-ids`, which re-runs every listed
run as a new attempt. The original attempt is never deleted or edited, the
report keeps the attempt with the highest number whatever its value, so no
observation is selected on its outcome (a replacement that stalls again stays
in the dataset). Nothing about this rule depends on which condition is being
looked at.

Usage:
    python3 report/stalled_runs.py \
        --processed results/conflict_validation/processed --below 0.7 \
        --out results/conflict_validation/rerun-ids.txt
"""
import argparse
import csv
import os
import statistics as st
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import process as labprocess


def load_metrics(processed):
    with open(os.path.join(processed, "metrics.csv"), newline="") as f:
        return list(csv.DictReader(f))


def newest_run_mtime(raw):
    """mtime of the most recently written run directory, or None."""
    if not os.path.isdir(raw):
        return None
    times = [os.path.getmtime(os.path.join(raw, d))
             for d in os.listdir(raw) if os.path.isdir(os.path.join(raw, d))]
    return max(times) if times else None


def check_fresh(processed, raw):
    """Refuse to list from stale processed data.

    A replacement attempt is a run directory like any other, so a list built
    from data that predates the last replacement names the attempt that is no
    longer included and would produce a third attempt for the same repetition
    instead of replacing the current one.
    """
    m = os.path.getmtime(os.path.join(processed, "metrics.csv"))
    newest = newest_run_mtime(raw)
    if newest is None:
        return
    if m < newest:
        sys.exit(f"{processed}/metrics.csv is older than the newest run under {raw};\n"
                 f"re-process the dataset before deriving the replacement list "
                 f"(e.g. make <family>-report)")


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def stalled_runs(rows, metric, below):
    """[(ratio, run_id, value, median, n_reps)] for every included repetition
    below `below` times its configuration's median, worst first."""
    groups = defaultdict(list)
    for r in rows:
        if r["included"] == "1":
            groups[labprocess.config_key(r)].append(r)
    out = []
    for key in groups:
        reps = groups[key]
        vals = [v for v in (fnum(r[metric]) for r in reps) if v is not None]
        if len(vals) < 4:                      # too few to define a median
            continue
        med = st.median(vals)
        if med <= 0:
            continue
        for r in reps:
            v = fnum(r[metric])
            if v is not None and v < below * med:
                out.append((v / med, r["run_id"], v, med, len(reps)))
    out.sort()
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--processed", required=True, help="processed dataset directory")
    ap.add_argument("--raw", default="", help="raw dataset directory, checked for staleness (default: sibling raw/)")
    ap.add_argument("--below", type=float, default=0.7,
                    help="list repetitions below this fraction of their condition median (default 0.7)")
    ap.add_argument("--metric", default="throughput_req_s", help="metric to test (default throughput_req_s)")
    ap.add_argument("--out", default="", help="write the run IDs here (one per line, tab then reason)")
    args = ap.parse_args()
    if not 0 < args.below < 1:
        sys.exit("--below must be between 0 and 1")

    raw = args.raw or os.path.join(os.path.dirname(os.path.abspath(args.processed)), "raw")
    check_fresh(args.processed, raw)

    rows = load_metrics(args.processed)
    found = stalled_runs(rows, args.metric, args.below)
    included = sum(1 for r in rows if r["included"] == "1")
    print(f"{args.processed}: {len(found)} of {included} included repetitions "
          f"below {args.below:.2f} of their condition median")
    for ratio, run_id, val, med, n in found:
        print(f"  {100 * ratio:5.0f}%  {run_id}  ({val:,.0f} vs {med:,.0f} req/s over {n} repetitions)")

    if args.out:
        # Sorted by run ID so the file is stable between invocations of the
        # same dataset; the reason travels with the ID into the replacement
        # run's metadata (rerun_reason).
        lines = [f"{run_id}\tstalled: {val:,.0f} req/s = {100 * ratio:.0f}% of the "
                 f"condition median {med:,.0f} req/s, below {args.below:.2f} "
                 f"(declared by report/stalled_runs.py)"
                 for ratio, run_id, val, med, _ in sorted(found, key=lambda t: t[1])]
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as f:
            f.write("\n".join(lines) + "\n" if lines else "")
        print(f"wrote {len(lines)} run ID(s) to {args.out}")


if __name__ == "__main__":
    main()
