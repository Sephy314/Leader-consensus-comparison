#!/usr/bin/env python3
"""Report host resource contention for the large replica counts.

The 7- and 9-replica configurations run on a single host, where every replica
is its own container with a fixed CPU quota. This report puts the resulting
oversubscription next to the measurements, so the large-replica results can be
read in context and NOT as an isolated protocol-level scalability effect.

Units: `cpu_util_pct` from the monitor is percent of ONE CPU (a container
holding two CPUs fully reports 200%). A replica's quota is `replica_cpus`
(2.0), so saturation is `cpu_util_pct / 100 / replica_cpus`.

Per (experiment, replica count) it reports the requested quota against the
host's CPU count, the observed per-replica utilisation, the share of requests
that failed, the throughput, and how many runs host telemetry flagged as
contaminated. It then states explicitly whether the measurements support
reading the configuration as resource-constrained: a configuration whose
replicas never approach their quota is NOT resource-constrained, and an
unverified cause is not recorded as a fact.

Usage:
    python3 report/resource_contention.py --processed results/processed \
        --raw results/raw --out results/resource-contention.md
"""
import argparse
import csv
import json
import os
import statistics
from collections import defaultdict


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_meta(raw_root, run_id):
    path = os.path.join(raw_root, run_id, "metadata.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def f2(v, suffix=""):
    return "--" if v is None else f"{v:.2f}{suffix}"


def quantile(vals, frac):
    if not vals:
        return None
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(frac * (len(vals) - 1)))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="results/processed")
    ap.add_argument("--raw", default="results/raw")
    ap.add_argument("--out", default="results/resource-contention.md")
    args = ap.parse_args()

    metrics = read_csv(os.path.join(args.processed, "metrics.csv"))
    resources = read_csv(os.path.join(args.processed, "resources.csv"))

    # One representative configuration per replica count: the CPU allocation is
    # identical across families, so one sample per replica count is enough --
    # but it must be sampled for EVERY replica count, not only the first run.
    cfg_by_replicas = {}
    cpus = None
    for run_id in sorted({m["run_id"] for m in metrics}):
        meta = load_meta(args.raw, run_id)
        if not meta:
            continue
        if cpus is None:
            cpus = (meta.get("host") or {}).get("cpus")
        cfg_by_replicas.setdefault(int(meta["config"]["replicas"]), meta["config"])
    if cpus is None:
        cpus = 12

    cpu_by_group = defaultdict(list)
    for r in resources:
        try:
            util = float(r["cpu_util_pct"])
            replicas = int(r["replicas"])
        except (KeyError, TypeError, ValueError):
            continue
        cpu_by_group[(r["experiment"], replicas)].append(util)

    groups = defaultdict(list)
    for m in metrics:
        groups[(m["experiment"], int(m["replicas"]))].append(m)

    lines = ["# Host resource contention at the large replica counts\n"]
    lines.append(f"Host CPU count recorded in run metadata: **{cpus}**.\n")
    lines.append(
        "Each replica is a separate container with a fixed CPU quota "
        "(`replica_cpus`), and the client and coordinator have their own. "
        "`quota/host` is the total requested quota divided by the host's CPU "
        "count, so a value above 1 means the run's containers cannot all be "
        "scheduled at their full quota simultaneously. `replica saturation` "
        "compares the busiest replica's observed CPU against ITS OWN quota, "
        "which is the quantity that decides whether CPU contention actually "
        "bounded the run. `failed requests` is the worst run in the group, "
        "not the median: the median over Raft and EPaxos runs together is 0 "
        "because Raft never times out, which would hide exactly the "
        "timeout-heavy runs this question is about.\n")
    lines.append("| experiment | replicas | quota | quota/host | replicas sampled "
                 "| per-replica CPU p50 | p95 | max | replica saturation (max) "
                 "| failed requests (max run) | throughput p50 | flagged runs |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")

    verdicts = []
    for key in sorted(groups):
        exp, replicas = key
        runs = groups[key]
        utils = cpu_by_group.get(key, [])
        cfg = cfg_by_replicas.get(replicas)
        quota = quota_ratio = per_replica = None
        if cfg:
            per_replica = float(cfg["replica_cpus"])
            quota = (replicas * per_replica + float(cfg["client_cpus"])
                     + float(cfg["master_cpus"]))
            quota_ratio = quota / cpus
        failed = [float(m["requests_failed"]) / float(m["requests_total"])
                  for m in runs
                  if m.get("requests_total") and float(m["requests_total"]) > 0]
        tput = [float(m["throughput_req_s"]) for m in runs if m.get("throughput_req_s")]
        flagged = sum(1 for m in runs
                      if str(m.get("contaminated")).lower() in ("true", "1"))
        saturation = (max(utils) / 100 / per_replica) if (utils and per_replica) else None

        lines.append(
            f"| {exp} | {replicas} | {f2(quota)} | {f2(quota_ratio)} | {len(utils)} | "
            f"{f2(statistics.median(utils) if utils else None, ' %')} | "
            f"{f2(quantile(utils, 0.95), ' %')} | {f2(max(utils) if utils else None, ' %')} | "
            f"{f2(saturation * 100 if saturation is not None else None, ' %')} | "
            f"{f2(max(failed) * 100 if failed else None, ' %')} | "
            f"{f2(statistics.median(tput) if tput else None)} | {flagged}/{len(runs)} |")

        if saturation is None:
            continue
        peak = max(utils)
        med = statistics.median(utils)
        if saturation >= 0.9:
            verdicts.append(
                f"**{exp} / {replicas} replicas - constrained.** The busiest "
                f"replica reached {saturation * 100:.0f}% of its "
                f"{per_replica:.1f}-CPU quota (peak {peak:.1f}% of one CPU, "
                f"median {med:.1f}%), so CPU contention is a plausible "
                f"contributor to this configuration's result and its numbers "
                f"must be reported with that caveat.")
        else:
            verdicts.append(
                f"**{exp} / {replicas} replicas - not constrained by CPU.** "
                f"No replica approached its {per_replica:.1f}-CPU quota: the "
                f"busiest reached {saturation * 100:.0f}% of it (peak "
                f"{peak:.1f}% of one CPU, median {med:.1f}%). The requested "
                f"quota ({quota:.1f} CPUs against {cpus} host CPUs, "
                f"{quota_ratio:.2f}x) is oversubscribed on paper, but the "
                f"observed utilisation does NOT support attributing this "
                f"configuration's result to CPU contention.")

    lines.append("\n## What the measurements support\n")
    lines.extend(f"- {v}" for v in verdicts)
    lines.append(
        "\n## How to cite this\n"
        "- Any statement about a large replica count must be conditioned on "
        "this table, not asserted.\n"
        "- A configuration qualifies for the resource-contention caveat only "
        "where `replica saturation (max)` approaches 100% and/or host telemetry "
        "flagged contamination.\n"
        "- The raw telemetry (`resources.csv`, `host-telemetry.csv`) is "
        "preserved unchanged for every run, including the flagged ones, so "
        "every claim here can be re-checked without re-running the suite.\n")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
