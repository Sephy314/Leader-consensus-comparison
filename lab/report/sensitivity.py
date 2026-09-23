#!/usr/bin/env python3
"""Implementation-sensitivity analysis.

Reads the sensitivity dataset (results/sensitivity/raw), computes per-run
throughput and latency with the SAME definitions as the primary pipeline, and
reports per-implementation statistics with the same statistical procedure.

Reuse rather than reimplementation:
  - per-run throughput, percentiles and resource aggregation come from
    report/process.py (process_run, process_resources, request_series,
    validate_run), so the definitions are literally the primary ones;
  - mean/median/sd/95% t-interval come from report/stats.py, the same module
    the primary pipeline uses.

The only difference from the primary pipeline is the aggregation key: the
implementation is part of it, because the sensitivity dataset deliberately
contains several implementations of the same protocol at the same
configuration, which the primary key (see process.config_key) does not
distinguish.

The primary dataset is read only for comparison (its processed
config-summary.json), never modified.

Usage:
    python3 report/sensitivity.py \
        --raw results/sensitivity/raw \
        --primary-processed results/processed \
        --out results/sensitivity/sensitivity-report.md \
        --json results/sensitivity/sensitivity.json
"""
import argparse
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import process as labprocess
import stats as labstats

PROTOCOLS = ["raft", "epaxos"]
PRIMARY_IMPL = {"raft": "hashicorp", "epaxos": "original"}
IMPLS = {
    "raft": ["hashicorp", "etcd", "etcd-core"],
    "epaxos": ["original", "nvb"],
}

# The two implementation pairs being compared: pair A is the pair already in
# the paper (primary implementations), pair B is the pair added by the
# sensitivity experiment.
PAIRS = {
    "A": {"raft": "hashicorp", "epaxos": "original"},
    "B": {"raft": "etcd", "epaxos": "nvb"},
}


def effective_impl(cfg):
    return cfg.get("implementation") or PRIMARY_IMPL.get(cfg.get("protocol", ""), "")


def collect(raw_root):
    """Extract one metrics row per run directory, using the primary pipeline's
    per-run definitions."""
    rows, resources, failed = [], [], []
    for name in sorted(os.listdir(raw_root)):
        run_dir = os.path.join(raw_root, name)
        if not os.path.isdir(run_dir):
            continue
        ok, reason, series = labprocess.validate_run(run_dir, name)
        if not ok:
            failed.append({"run_id": name, "reason": reason})
            continue
        m = labprocess.process_run(run_dir, name, series)
        meta = labprocess.load_metadata(run_dir)
        m["implementation"] = effective_impl(meta["config"])
        m["keyspace"] = meta["config"].get("keyspace")
        m["duration_s"] = meta["config"].get("duration_s")
        rows.append(m)
        for r in labprocess.process_resources(run_dir, name, series):
            r["implementation"] = m["implementation"]
            resources.append(r)
    return rows, resources, failed


def mark_included(rows):
    """The primary dataset's documented selection rule, with the
    implementation added to the configuration key.

    process.mark_included keys on process.config_key, which does not include
    the implementation. Using it unchanged would merge the primary and
    independent implementations of the same protocol at the same
    configuration into one condition and silently drop half the runs. The
    rule itself is unchanged: drop contaminated attempts, and keep the
    highest attempt number among the remainder.
    """
    best = {}
    for r in rows:
        if r["contaminated"]:
            continue
        key = (labprocess.config_key(r), r["implementation"], r["repetition"])
        cur = best.get(key)
        if cur is None or r["attempt"] > cur["attempt"]:
            best[key] = r
    chosen = {id(r) for r in best.values()}
    excluded = 0
    for r in rows:
        r["included"] = 1 if id(r) in chosen else 0
        if not r["included"]:
            excluded += 1
    return rows, excluded


def summarize(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return {"n": 0, "mean": None, "sd": None, "ci95_lo": None, "ci95_hi": None}
    lo, hi = labstats.ci95(vals)
    return {
        "n": len(vals),
        "mean": labstats.mean(vals),
        "sd": labstats.stdev(vals),
        "ci95_lo": lo,
        "ci95_hi": hi,
        "min": min(vals),
        "max": max(vals),
    }


def group_stats(rows, keys):
    """Group included rows by `keys` and summarize throughput and p50 latency."""
    groups = defaultdict(list)
    for r in rows:
        if not r["included"]:
            continue
        groups[tuple(r[k] for k in keys)].append(r)
    out = {}
    for k, rs in groups.items():
        out[k] = {
            "n": len(rs),
            "throughput": summarize([r["throughput_req_s"] for r in rs]),
            "latency_p50_ms": summarize(
                [r["latency_ns_p50"] / 1e6 if r["latency_ns_p50"] is not None else None for r in rs]
            ),
            "success_rate": labstats.mean([r["success_rate"] for r in rs]),
            "run_ids": sorted(r["run_id"] for r in rs),
        }
    return out


def resource_stats(resources):
    """Per (protocol, implementation, replicas): mean per-replica CPU
    utilisation, peak RSS, and network rates, averaged over runs."""
    per_run = defaultdict(lambda: {"cpu": 0.0, "rss": 0.0, "rx": 0.0, "tx": 0.0, "n": 0})
    for r in resources:
        key = (r["protocol"], r["implementation"], r["replicas"], r["run_id"])
        agg = per_run[key]
        agg["cpu"] += r["cpu_util_pct"] or 0.0
        agg["rss"] += r["rss_bytes_max"] or 0
        agg["rx"] += r["net_rx_bps"] or 0.0
        agg["tx"] += r["net_tx_bps"] or 0.0
        agg["n"] += 1

    by_cond = defaultdict(list)
    for (proto, impl, replicas, _), agg in per_run.items():
        by_cond[(proto, impl, replicas)].append(agg)

    out = {}
    for k, aggs in by_cond.items():
        out[k] = {
            "runs": len(aggs),
            "cpu_total_pct_mean": labstats.mean([a["cpu"] for a in aggs]),
            "cpu_per_replica_pct_mean": labstats.mean(
                [a["cpu"] / a["n"] if a["n"] else None for a in aggs]
            ),
            "rss_mb_per_replica_mean": labstats.mean(
                [a["rss"] / a["n"] / 1e6 if a["n"] else None for a in aggs]
            ),
            "net_rx_kbps_mean": labstats.mean([a["rx"] / 1024 for a in aggs]),
            "net_tx_kbps_mean": labstats.mean([a["tx"] / 1024 for a in aggs]),
        }
    return out


def primary_means(primary_processed):
    """The primary dataset's per-configuration means, for comparison only."""
    path = os.path.join(primary_processed, "config-summary.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        data = json.load(f)
    out = {}
    for rec in data:
        key = (rec["experiment"], rec["protocol"], rec["replicas"], rec["write_pct"],
               rec["concurrency"])
        out[key] = {
            "throughput": rec.get("throughput") or {},
            "latency_p50": rec.get("latency_p50") or {},
        }
    return out


def pct_diff(a, b):
    if a in (None, 0) or b is None:
        return None
    return (b - a) / a * 100.0


def classify(want, got):
    """Map the evidence to exactly one of the four required classifications.

    want: {replicas: "raft"|"epaxos"} --- the direction the PRIMARY dataset
          separates at that replica count.
    got:  {(pair, replicas): "raft"|"epaxos"} --- the direction the
          sensitivity data separates, per implementation pair. A replica
          count absent from `got` had overlapping intervals, i.e. no
          direction could be read.

    The mapping is fixed and stated in full so a reader can disagree with the
    mapping rather than with the measurements:

      not reproduced        a pair reverses at EVERY replica count at which it
                            separates from the primary direction
      reproduced            both pairs agree at every replica count at which
                            they separate
      inconclusive          neither pair separates at any replica count
      partially reproduced  everything else
    """
    per_pair = defaultdict(lambda: {"agree": 0, "reverse": 0, "overlap": 0})
    for (pair, replicas), d in got.items():
        w = want.get(replicas)
        if w is None:
            continue
        if d == w:
            per_pair[pair]["agree"] += 1
        else:
            per_pair[pair]["reverse"] += 1
    for pair in PAIRS:
        for replicas, w in want.items():
            if (pair, replicas) not in got:
                per_pair[pair]["overlap"] += 1

    if all(v["reverse"] and not v["agree"] for v in per_pair.values()):
        return "trend not reproduced", per_pair
    if all(v["agree"] == len(want) and not v["reverse"] for v in per_pair.values()):
        return "trend reproduced", per_pair
    if all(v["agree"] == 0 and v["reverse"] == 0 for v in per_pair.values()):
        return "trend inconclusive", per_pair
    return "trend partially reproduced", per_pair


def fmt(v, digits=0):
    if v is None:
        return "--"
    return f"{v:,.{digits}f}"


def build_report(rows, resources, failed, primary, args):
    lines = []
    w = lines.append

    included = [r for r in rows if r["included"]]
    excluded = [r for r in rows if not r["included"]]
    w("# Implementation-sensitivity report")
    w("")
    w(f"Raw root: `{args.raw}`  ")
    w(f"Runs found: {len(rows)} valid, {len(failed)} failed/invalid  ")
    w(f"Included in aggregation: {len(included)}  ")
    w(f"Excluded (contaminated host telemetry, or superseded attempt): {len(excluded)}")
    w("")

    # ---- per-implementation table (F) ----
    st = group_stats(rows, ["protocol", "implementation", "replicas"])
    w("## Per-implementation results")
    w("")
    w("| Protocol | Implementation | Replicas | n | Mean throughput (req/s) | SD | 95% CI | p50 latency (ms) | p50 95% CI |")
    w("|---|---|---|---|---|---|---|---|---|")
    for proto in PROTOCOLS:
        for impl in IMPLS[proto]:
            for replicas in sorted({r["replicas"] for r in rows}):
                key = (proto, impl, replicas)
                if key not in st:
                    continue
                s = st[key]
                t = s["throughput"]
                l = s["latency_p50_ms"]
                ci = (f"{fmt(t['ci95_lo'])}–{fmt(t['ci95_hi'])}"
                      if t["ci95_lo"] is not None else "--")
                lci = (f"{l['ci95_lo']:.2f}–{l['ci95_hi']:.2f}"
                       if l["ci95_lo"] is not None else "--")
                w(f"| {proto} | {impl} | {replicas} | {s['n']} | {fmt(t['mean'])} | "
                  f"{fmt(t['sd'])} | {ci} | {fmt(l['mean'], 2)} | {lci} |")
    w("")

    # `etcd-core` is not an alternative implementation of Raft in the sense the
    # other names are: it is the same go.etcd.io/raft/v3 core run with
    # in-memory storage only (no write-ahead log, no fsync). It exists to
    # separate the core's cost from the adapter's durability path, so it is
    # reported beside the durable implementations but never read as one.
    if any(r["implementation"] == "etcd-core" for r in rows):
        w("`etcd-core` is an analysis mode, not a durable implementation: it")
        w("runs the same etcd/raft core with in-memory storage only, so it")
        w("performs no write-ahead logging and no fsync. The gap between")
        w("`etcd` and `etcd-core` is the cost of the adapter's durability")
        w("path, not a property of the raft algorithm; `etcd-core` must not be")
        w("quoted as an implementation of Raft alongside the others.")
        w("")

    # ---- resources ----
    rs = resource_stats(resources)
    if rs:
        w("## Resource use (per-replica means over included runs)")
        w("")
        w("| Protocol | Implementation | Replicas | Runs | CPU %/replica | RSS MB/replica | RX KiB/s | TX KiB/s |")
        w("|---|---|---|---|---|---|---|---|")
        for key in sorted(rs):
            proto, impl, replicas = key
            v = rs[key]
            w(f"| {proto} | {impl} | {replicas} | {v['runs']} | "
              f"{fmt(v['cpu_per_replica_pct_mean'], 1)} | {fmt(v['rss_mb_per_replica_mean'], 1)} | "
              f"{fmt(v['net_rx_kbps_mean'], 1)} | {fmt(v['net_tx_kbps_mean'], 1)} |")
        w("")

    # ---- direction, per implementation ----
    def direction(proto, replicas):
        """'raft' / 'epaxos' when the intervals separate, else None."""
        vals = {}
        for p in PROTOCOLS:
            for impl in IMPLS[p]:
                s = st.get((p, impl, replicas))
                if s and s["throughput"]["mean"] is not None:
                    vals[impl] = s["throughput"]
        pairs = []
        if proto == "raft":
            pair = ("hashicorp", "etcd")
        else:
            pair = ("original", "nvb")
        a, b = vals.get(pair[0]), vals.get(pair[1])
        return a, b, pair

    w("## Implementation-to-implementation variation")
    w("")
    w("| Protocol | Replicas | A mean | B mean | Δ (B vs A) | Δ as % of A | A 95% CI | B 95% CI |")
    w("|---|---|---|---|---|---|---|---|")
    variation = {}
    for proto in PROTOCOLS:
        for replicas in sorted({r["replicas"] for r in rows}):
            a, b, pair = direction(proto, replicas)
            if not a or not b:
                continue
            d = pct_diff(a["mean"], b["mean"])
            variation[(proto, replicas)] = d
            w(f"| {proto} | {replicas} | {fmt(a['mean'])} | {fmt(b['mean'])} | "
              f"{fmt((b['mean'] or 0) - (a['mean'] or 0))} | "
              f"{'--' if d is None else f'{d:+.1f}%'} | "
              f"{fmt(a['ci95_lo'])}–{fmt(a['ci95_hi'])} | {fmt(b['ci95_lo'])}–{fmt(b['ci95_hi'])} |")
    w("")

    # ---- comparison with the primary dataset ----
    w("## Comparison with the primary dataset (same configuration)")
    w("")
    w("The sensitivity dataset re-runs the primary implementations too, so the")
    w("difference between primary and sensitivity means for the same")
    w("implementation is a direct estimate of run-to-run reproducibility.")
    w("")
    w("| Protocol | Implementation | Replicas | Sensitivity mean | Primary mean | Δ | Primary n |")
    w("|---|---|---|---|---|---|---|")
    repro = {}
    for proto in PROTOCOLS:
        for impl in IMPLS[proto]:
            base = PRIMARY_IMPL[proto]
            if impl != base:
                continue
            for replicas in sorted({r["replicas"] for r in rows}):
                s = st.get((proto, impl, replicas))
                p = primary.get(("scaling", proto, replicas, 100, 32))
                if not s or not p or not p["throughput"].get("mean"):
                    continue
                sens_mean = s["throughput"]["mean"]
                prim_mean = p["throughput"]["mean"]
                d = pct_diff(prim_mean, sens_mean)
                repro[(proto, replicas)] = d
                w(f"| {proto} | {impl} | {replicas} | {fmt(sens_mean)} | {fmt(prim_mean)} | "
                  f"{'--' if d is None else f'{d:+.1f}%'} | {p['throughput'].get('n')} |")
    w("")

    # ---- Raft-vs-EPaxos direction ----
    w("## Raft vs EPaxos direction")
    w("")
    w("`--` means the two 95% intervals overlap and the comparison is read as")
    w("inconclusive. `want` is the direction the primary dataset separates at")
    w("that replica count; the two right-hand columns are the sensitivity data")
    w("for the primary pair (A) and the independent pair (B).")
    w("")
    w("| Replicas | Primary (want) | Pair A | Pair B | A matches | B matches |")
    w("|---|---|---|---|---|---|")

    def sep_winner(a, b):
        if not a or not b or a["mean"] is None or b["mean"] is None:
            return None
        if a["ci95_lo"] is None or b["ci95_lo"] is None:
            return None
        if a["ci95_lo"] > b["ci95_hi"]:
            return "raft"
        if b["ci95_lo"] > a["ci95_hi"]:
            return "epaxos"
        return None

    want, got = {}, {}
    for replicas in sorted({r["replicas"] for r in rows}):
        prim_r = primary.get(("scaling", "raft", replicas, 100, 32), {}).get("throughput", {})
        prim_e = primary.get(("scaling", "epaxos", replicas, 100, 32), {}).get("throughput", {})
        want[replicas] = sep_winner(prim_r, prim_e)
        for pair, members in PAIRS.items():
            d = sep_winner(st.get(("raft", members["raft"], replicas), {}).get("throughput"),
                           st.get(("epaxos", members["epaxos"], replicas), {}).get("throughput"))
            if d:
                got[(pair, replicas)] = d

        def match(pair):
            wv = want[replicas]
            if wv is None:
                return "--"
            gv = got.get((pair, replicas))
            if gv is None:
                return "overlap"
            return "yes" if gv == wv else "**NO**"

        def show(v):
            return v or "--"

        w(f"| {replicas} | {show(want[replicas])} | {show(got.get(('A', replicas)))} | "
          f"{show(got.get(('B', replicas)))} | {match('A')} | {match('B')} |")
    w("")

    classification, per_pair = classify(want, got)
    w(f"**Classification: {classification}**")
    w("")
    w("| Pair | Agrees | Reverses | Intervals overlap |")
    w("|---|---|---|---|")
    for pair in sorted(per_pair):
        v = per_pair[pair]
        w(f"| {pair} | {v['agree']} | {v['reverse']} | {v['overlap']} |")
    w("")
    w("Mapping, stated so it can be audited rather than trusted: *not")
    w("reproduced* if a pair reverses at every replica count at which it")
    w("separates; *reproduced* if both pairs agree at every such count;")
    w("*inconclusive* if neither pair separates at all; *partially reproduced*")
    w("otherwise.")
    w("")

    if failed:
        w("## Failed or invalid runs (recorded, never dropped)")
        w("")
        w(f"{len(failed)} runs were not measured successfully. They remain in")
        w("`results/sensitivity/raw/` and are listed in the JSON output.")
        w("")
        reasons = defaultdict(int)
        for f in failed:
            reasons[f["reason"].split(":")[0]] += 1
        w("| Reason | Count |")
        w("|---|---|")
        for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
            w(f"| `{reason}` | {count} |")
        w("")

    if excluded:
        w("## Excluded runs")
        w("")
        w("| Run | Reason |")
        w("|---|---|")
        for r in excluded:
            why = "; ".join(r["anomalies"]) if r["anomalies"] else "superseded attempt"
            w(f"| {r['run_id']} | {why} |")
        w("")

    return "\n".join(lines), {
        "included": len(included),
        "excluded": len(excluded),
        "failed": failed,
        "stats": {f"{k[0]}/{k[1]}/r{k[2]}": v for k, v in st.items()},
        "variation_pct": {f"{k[0]}/r{k[1]}": v for k, v in variation.items()},
        "reproducibility_pct": {f"{k[0]}/r{k[1]}": v for k, v in repro.items()},
        "classification": classification,
        "direction_primary": want,
        "direction_sensitivity": {f"{k[0]}/r{k[1]}": v for k, v in got.items()},
        "per_pair": {k: dict(v) for k, v in per_pair.items()},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True, help="sensitivity raw results root")
    ap.add_argument("--primary-processed", default="results/processed",
                    help="primary dataset processed dir (read-only, for comparison)")
    ap.add_argument("--out", default=None, help="markdown report path")
    ap.add_argument("--json", default=None, help="JSON output path")
    args = ap.parse_args()

    rows, resources, failed = collect(args.raw)
    rows, _ = mark_included(rows)
    primary = primary_means(args.primary_processed)
    report, data = build_report(rows, resources, failed, primary, args)

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as f:
            f.write(report + "\n")
        print(f"wrote {args.out}")
    else:
        print(report)
    if args.json:
        os.makedirs(os.path.dirname(os.path.abspath(args.json)), exist_ok=True)
        with open(args.json, "w") as f:
            json.dump(data, f, indent=2)
        print(f"wrote {args.json}")


if __name__ == "__main__":
    main()
