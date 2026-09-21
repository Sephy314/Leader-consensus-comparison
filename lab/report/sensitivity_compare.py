#!/usr/bin/env python3
"""Compare two implementation-sensitivity runs.

Reads two `sensitivity.json` files (the group-level statistics produced by
report/sensitivity.py) and reports, per (protocol, implementation, replicas):

  - mean throughput, SD, 95% CI and p50 latency in each run
  - absolute and percentage change between the runs
  - whether the two runs' 95% intervals overlap (i.e. whether the difference
    is separable at all)

It then checks the three specific claims the sensitivity experiment is meant
to reproduce, in both runs:

  1. at 3 replicas EPaxos sustains higher throughput than Raft
  2. at 5 replicas the two EPaxos implementations differ substantially
  3. the primary Raft implementation re-run differs from the primary dataset

Run 1 is the earlier run (archived); run 2 is the new run. The primary
dataset's processed summary is read only for the baseline means.

Usage:
    python3 report/sensitivity_compare.py \
        --run1 results/archive/sensitivity-run1-2026-09-20/sensitivity.json \
        --run2 results/sensitivity/sensitivity.json \
        --primary-processed results/processed \
        --out results/sensitivity/run1-vs-run2.md
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stats as labstats


def load(path):
    with open(path) as f:
        return json.load(f)


def fnum(v, digits=0):
    return "--" if v is None else f"{v:,.{digits}f}"


def sep(a, b):
    """True when two summaries' 95% intervals do not overlap."""
    if not a or not b:
        return None
    if a.get("ci95_lo") is None or b.get("ci95_hi") is None:
        return None
    return a["ci95_lo"] > b["ci95_hi"] or b["ci95_lo"] > a["ci95_hi"]


def pct(old, new):
    if old in (None, 0) or new is None:
        return None
    return (new - old) / old * 100.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run1", required=True)
    ap.add_argument("--run2", required=True)
    ap.add_argument("--primary-processed", default="results/processed")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    r1, r2 = load(args.run1), load(args.run2)
    s1, s2 = r1["stats"], r2["stats"]

    L = []
    w = L.append
    w("# Sensitivity re-run: run 1 vs run 2")
    w("")
    w("| | Run 1 (archived) | Run 2 (new) |")
    w("|---|---|---|")
    w(f"| Valid runs aggregated | {r1.get('included')} | {r2.get('included')} |")
    w(f"| Excluded | {r1.get('excluded')} | {r2.get('excluded')} |")
    w(f"| Failed/invalid attempts | {len(r1.get('failed') or [])} | {len(r2.get('failed') or [])} |")
    w(f"| Classification | {r1.get('classification')} | {r2.get('classification')} |")
    w("")

    w("## Per-configuration comparison")
    w("")
    w("| Protocol | Impl | R | Run1 mean | Run2 mean | Δ | Δ % | Run1 SD | Run2 SD | "
      "Run1 95% CI | Run2 95% CI | CIs overlap | Run1 p50 | Run2 p50 | Δ p50 % |")
    w("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    rows = {}
    for key in sorted(set(s1) | set(s2)):
        a, b = s1.get(key), s2.get(key)
        if not a or not b:
            continue
        ta, tb = a["throughput"], b["throughput"]
        la, lb = a["latency_p50_ms"], b["latency_p50_ms"]
        d = pct(ta["mean"], tb["mean"])
        dp = pct(la["mean"], lb["mean"])
        ov = sep(ta, tb)
        rows[key] = {
            "run1": ta, "run2": tb,
            "run1_p50": la, "run2_p50": lb,
            "delta_pct": d, "delta_p50_pct": dp, "separated": ov,
        }
        proto, impl, rep = key.split("/")
        ci_a = f"{fnum(ta['ci95_lo'])}–{fnum(ta['ci95_hi'])}" if ta["ci95_lo"] is not None else "--"
        ci_b = f"{fnum(tb['ci95_lo'])}–{fnum(tb['ci95_hi'])}" if tb["ci95_lo"] is not None else "--"
        w(f"| {proto} | {impl} | {rep} | {fnum(ta['mean'])} | {fnum(tb['mean'])} | "
          f"{fnum((tb['mean'] or 0) - (ta['mean'] or 0))} | "
          f"{'--' if d is None else f'{d:+.1f}%'} | {fnum(ta['sd'])} | {fnum(tb['sd'])} | "
          f"{ci_a} | {ci_b} | {'no' if ov else 'YES'} | "
          f"{fnum(la['mean'], 2)} | {fnum(lb['mean'], 2)} | "
          f"{'--' if dp is None else f'{dp:+.1f}%'} |")
    w("")

    # ---- the three specific claims ----
    def get(stats, proto, impl, rep):
        return (stats.get(f"{proto}/{impl}/{rep}") or {}).get("throughput")

    w("## Reproduction checks")
    w("")

    # 1. r3 EPaxos > Raft
    w("### 1. At 3 replicas: EPaxos above Raft")
    w("")
    w("| Pair | Run | Raft mean | EPaxos mean | Higher | CIs separated |")
    w("|---|---|---|---|---|---|")
    for label, rf, ep in [("A (HashiCorp vs original)", "hashicorp", "original"),
                          ("B (etcd vs nvb)", "etcd", "nvb")]:
        for name, st in (("run1", s1), ("run2", s2)):
            a = get(st, "raft", rf, "r3")
            b = get(st, "epaxos", ep, "r3")
            if not a or not b:
                continue
            higher = "EPaxos" if b["mean"] > a["mean"] else "Raft"
            w(f"| {label} | {name} | {fnum(a['mean'])} | {fnum(b['mean'])} | {higher} | "
              f"{'yes' if sep(a, b) else 'NO'} |")
    w("")

    # 2. r5 EPaxos A vs B
    w("### 2. At 5 replicas: the two EPaxos implementations")
    w("")
    w("| Run | original (A) | nvb (B) | Δ | Δ % | CIs separated |")
    w("|---|---|---|---|---|---|")
    for name, st in (("run1", s1), ("run2", s2)):
        a = get(st, "epaxos", "original", "r5")
        b = get(st, "epaxos", "nvb", "r5")
        if not a or not b:
            continue
        d = pct(a["mean"], b["mean"])
        w(f"| {name} | {fnum(a['mean'])} | {fnum(b['mean'])} | "
          f"{fnum(b['mean'] - a['mean'])} | {'--' if d is None else f'{d:+.1f}%'} | "
          f"{'yes' if sep(a, b) else 'NO'} |")
    w("")

    # 3. primary vs sensitivity re-run
    prim = {}
    p = os.path.join(args.primary_processed, "config-summary.json")
    if os.path.exists(p):
        for rec in json.load(open(p)):
            if rec["experiment"] == "scaling":
                prim[(rec["protocol"], rec["replicas"])] = rec.get("throughput") or {}

    w("### 3. Primary Raft implementation: primary dataset vs each re-run")
    w("")
    w("| Replicas | Primary mean | Run1 mean | Run1 Δ % | Run2 mean | Run2 Δ % | Run2 vs Run1 Δ % |")
    w("|---|---|---|---|---|---|---|")
    for rep in ("r3", "r5"):
        n = rep[1:]
        pm = prim.get(("raft", int(n)), {})
        a = get(s1, "raft", "hashicorp", rep)
        b = get(s2, "raft", "hashicorp", rep)
        if not pm.get("mean") or not a or not b:
            continue
        w(f"| {n} | {fnum(pm['mean'])} | {fnum(a['mean'])} | {pct(pm['mean'], a['mean']):+.1f}% | "
          f"{fnum(b['mean'])} | {pct(pm['mean'], b['mean']):+.1f}% | "
          f"{pct(a['mean'], b['mean']):+.1f}% |")
    w("")

    # ---- summary statistics of the run-to-run delta ----
    deltas = [v["delta_pct"] for v in rows.values() if v["delta_pct"] is not None]
    if deltas:
        w("## Run-to-run change summary")
        w("")
        w(f"- configurations compared: {len(deltas)}")
        w(f"- mean change: {labstats.mean(deltas):+.1f}%")
        w(f"- SD of change: {labstats.stdev(deltas):.1f}%")
        w(f"- largest increase: {max(deltas):+.1f}%")
        w(f"- largest decrease: {min(deltas):+.1f}%")
        n_overlap = sum(1 for v in rows.values() if not v["separated"])
        w(f"- configurations whose 95% intervals overlap between runs: {n_overlap}/{len(rows)}")
        w("")

    report = "\n".join(L)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as f:
            f.write(report + "\n")
        print(f"wrote {args.out}")
    else:
        print(report)


if __name__ == "__main__":
    main()
