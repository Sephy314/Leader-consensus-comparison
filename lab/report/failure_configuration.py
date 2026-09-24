#!/usr/bin/env python3
"""Document the failure experiment's configuration and measured outcome.

A measured availability gap is a property of the run's configured failure
detection, not a generic constant: the gap contains the time the survivors need
to notice the failure and elect a new leader, and it ends only when a request
succeeds again. This report prints the failure-detection parameters next to the
measured timings for every failure run, so a number like "1.81-4.74 s" is always
attached to the configuration it was observed under.

Only recorded values are printed. The heartbeat and election timeouts come from
the run's metadata (and are marked n/a for runs that are not Raft, where those
parameters are not part of the failure detection at all), and the timings come
from the run's own events/requests timeline. A field the run did not record is
printed as "not recorded" rather than inferred.

Usage:
    python3 report/failure_configuration.py --processed results/processed \
        --raw results/raw --out results/failure-configuration.md
"""
import argparse
import csv
import json
import os
import statistics


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


CONFIGURATION_HELP = (
    "Raft failure detection in this lab is configured by two parameters, both "
    "recorded in every run's metadata.json:\n"
    "- `raft_heartbeat_ms` (`-heartbeat-ms`): how often the leader sends "
    "AppendEntries; HashiCorp Raft's `HeartbeatTimeout`.\n"
    "- `raft_election_ms` (`-election-ms`): the election timeout, i.e. how long "
    "a follower waits without hearing from the leader before starting an "
    "election; HashiCorp Raft's `ElectionTimeout`. HashiCorp Raft randomizes "
    "the effective timeout over `[ElectionTimeout, 2 x ElectionTimeout)`.\n"
    "Both Raft implementations are launched with the same values, so they fail "
    "over under the same detection budget. EPaxos runs have no leader and "
    "therefore no such parameters: their availability gap is bounded by the "
    "client timeout (`timeout_ms`) and by how quickly the surviving replicas "
    "resume quorum, so the two protocol families' gaps are not the same "
    "quantity."
)


def load_meta(raw_root, run_id):
    path = os.path.join(raw_root, run_id, "metadata.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def sha(value, fmt="{:.3f}"):
    """Format a recorded value, or say plainly that it was not recorded."""
    if value is None:
        return "not recorded"
    return fmt.format(value)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="results/processed")
    ap.add_argument("--raw", default="results/raw")
    ap.add_argument("--out", default="results/failure-configuration.md")
    args = ap.parse_args()

    failures = json.load(open(os.path.join(args.processed, "failures.json")))
    included = [f for f in failures if f.get("included")]

    # Failed-request counts live in the per-run metrics, not in the failure
    # records; join them so the timeline can be read against the request loss.
    failed_reqs = {}
    metrics_path = os.path.join(args.processed, "metrics.csv")
    if os.path.exists(metrics_path):
        for m in read_csv(metrics_path):
            failed_reqs[m["run_id"]] = m.get("requests_failed")

    lines = ["# Failure experiment: configuration and measured outcome\n"]
    lines.append("## Configuration\n")
    lines.append(CONFIGURATION_HELP + "\n")

    # The configured detection budget, taken from the runs themselves.
    seen = {}
    for f in failures:
        meta = load_meta(args.raw, f["run_id"])
        if not meta:
            continue
        cfg = meta["config"]
        if cfg["protocol"] == "raft":
            budget = (f"{cfg['raft_heartbeat_ms']} ms", f"{cfg['raft_election_ms']} ms")
        else:
            budget = ("n/a (not a Raft run)", f"n/a; client timeout {cfg['timeout_ms']} ms")
        seen.setdefault((cfg["protocol"], cfg["replicas"]) + budget, []).append(f["run_id"])
    lines.append("| protocol | replicas | heartbeat | election timeout | runs |")
    lines.append("|---|---|---|---|---|")
    for (proto, replicas, hb, elec), run_ids in sorted(seen.items()):
        lines.append(f"| {proto} | {replicas} | {hb} | {elec} | {len(run_ids)} |")
    lines.append("")

    lines.append("## Per-run timeline\n")
    lines.append(
        "`availability gap` is the longest interval with no successful "
        "completion starting at or after the injected failure. "
        "`recovery from isolation end` is measured from the expiry of the "
        "injected isolation window, so it is decoupled from the injection "
        "duration (which is `failed_elections_target x election_timeout` by "
        "construction). Both are bounded by the ~200 ms resource-monitor "
        "resolution and by request timing.\n")
    lines.append("| run | failure | target | protocol | heartbeat | election "
                 "| kill (rel s) | gap start (rel s) | gap end = first success "
                 "(rel s) | availability gap (s) | recovery from isolation end (s) "
                 "| failed elections (measured) | failed requests |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")

    grouped = {}
    for f in sorted(included, key=lambda x: (x.get("failure_mode", ""), x["run_id"])):
        run_id = f["run_id"]
        meta = load_meta(args.raw, run_id)
        cfg = (meta or {}).get("config", {})
        timeline = f.get("timeline") or {}
        target = (timeline.get("failure_target") or {}).get("detail", "")

        def rel(event):
            entry = timeline.get(event)
            return None if not entry else entry.get("rel_s")

        if cfg.get("protocol") == "raft":
            hb = str(cfg.get("raft_heartbeat_ms", "not recorded"))
            elec = str(cfg.get("raft_election_ms", "not recorded"))
        else:
            hb, elec = "n/a", "n/a"

        gap = f.get("write_availability_gap_s_approx")
        grouped.setdefault((cfg.get("protocol", ""), f.get("failure_mode", "")), []).append(gap)

        lines.append(
            f"| {run_id} | {f.get('failure_mode', '')} | {target or '--'} "
            f"| {cfg.get('protocol', '')} | {hb} | {elec} "
            f"| {sha(rel('failure_injected'))} | {sha(f.get('gap_start_rel_s'))} "
            f"| {sha(f.get('gap_end_rel_s'))} | {sha(gap)} "
            f"| {sha(f.get('recovery_from_isolation_end_s'))} "
            f"| {f.get('measured_elections', 'not recorded')} "
            f"| {failed_reqs.get(run_id, 'not recorded')} |")

    lines.append("\n## How these numbers may be described\n")
    for (proto, mode), vals in sorted(grouped.items()):
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        lines.append(
            f"- **{proto} / {mode}** (n={len(vals)}, included runs): observed "
            f"availability gap {min(vals):.2f}-{max(vals):.2f} s, "
            f"median {statistics.median(vals):.2f} s.")
    lines.append(
        "\nEvery gap above is *conditioned on the configured failure-detection "
        "and election parameters in the first table*. They must be reported as "
        "the availability gap observed under that configuration, not as a "
        "generic Raft recovery time: a different heartbeat/election "
        "configuration would move the election component of the gap, and for "
        "the election runs the injected isolation window "
        "(`failed_elections_target x election_timeout`) is a proportional part "
        "of it by construction. The `recovery from isolation end` column is the "
        "metric that removes that proportionality.\n")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {args.out} ({len(included)} included failure runs)")


if __name__ == "__main__":
    main()
