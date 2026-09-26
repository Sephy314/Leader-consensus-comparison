#!/usr/bin/env python3
"""Leader vs follower CPU in the recorded Raft scaling runs.

The master assigns node ids in registration order, so the node id in the
client's "raft leader is replica N" line is *not* the container index that
`resources.csv` uses (`replica1` can be node 2). Each container prints
"registered with master: id=N", which is the mapping this script uses; taking
the node id as the container index inverts the leader and follower roles and
produced the wrong sign in an earlier analysis.

Usage: python3 report/leader_cpu.py [results/raw]
"""
import csv, glob, re, statistics as st, sys
from collections import defaultdict

root = sys.argv[1] if len(sys.argv) > 1 else "results/raw"


def cpu_by_container(run_dir):
    """container name (replicaN-1) -> mean CPU utilisation, % of one core."""
    series = defaultdict(list)
    for row in csv.DictReader(open(f"{run_dir}/resources.csv", newline="")):
        if row.get("role") == "replica":
            key = "-".join(row["container"].split("-")[-2:])
            series[key].append((int(row["ts_ns"]), int(row["cpu_usage_usec"])))
    out = {}
    for name, samples in series.items():
        samples.sort()
        span = (samples[-1][0] - samples[0][0]) / 1e9
        if span > 0:
            out[name] = (samples[-1][1] - samples[0][1]) / 1e6 / span * 100.0
    return out


def leader_container(run_dir):
    log = open(f"{run_dir}/container-logs.txt", errors="replace").read()
    idmap = {m.group(2): m.group(1) for m in re.finditer(
        r"^(replica\d+-1)\s+\|\s+\d{4}/\d{2}/\d{2} [\d:]+ registered with master: id=(\d+)", log, re.M)}
    m = re.search(r"raft leader is replica (\d+)", log)
    return idmap.get(m.group(1)) if m else None


by_count = defaultdict(list)
for run_dir in sorted(glob.glob(f"{root}/scaling-raft-*")):
    cpu = cpu_by_container(run_dir)
    lead = leader_container(run_dir)
    if lead is None or lead not in cpu or len(cpu) < 3:
        continue
    ratio = cpu[lead] / st.mean([v for k, v in cpu.items() if k != lead])
    n = int(re.search(r"scaling-raft-r(\d+)-", run_dir).group(1))
    by_count[n].append(ratio)

for n in sorted(by_count):
    r = by_count[n]
    print(f"  r{n}: {len(r)} runs  median leader/follower CPU {st.median(r):.2f}  "
          f"range {min(r):.2f}--{max(r):.2f}")
allr = [x for v in by_count.values() for x in v]
if allr:
    print(f"  all: {len(allr)} runs  median {st.median(allr):.2f}")
