#!/usr/bin/env python3
"""Process raw experiment results into aggregated metrics.

Pipeline: raw -> validation -> processing -> aggregated metrics -> figures.

Raw data is never modified. Every run directory under results/raw/ is
validated; failed runs are recorded in the index and excluded from
aggregation (but never deleted). Aggregated metrics are written to
results/processed/ as CSV/JSON, and figures are generated from those metrics
by figures.py.

Methodology notes (see docs/audit-2026-09-19.md):
- Resource samples are filtered to the measured phase (phase.json start to
  the last request end), so warm-up and teardown are excluded.
- Host-level anomalies (suspend, CPU starvation, clock jumps) are detected
  from each run's host-telemetry.csv and recorded; contaminated runs are
  marked, never silently deleted.
- For election runs, the ACTUAL number of failed elections is derived from
  the Raft adapters' measured counters (stats.json dropped_votes), never
  from the configured target.
- Per-configuration statistics (n, mean, median, SD, 95% CI) are written to
  config-summary.csv from the raw per-run observations.

Usage:
    python3 report/process.py --raw results/raw --processed results/processed
"""
import argparse
import array
import csv
import json
import os
import sys
from collections import defaultdict

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stats as labstats


def load_metadata(run_dir):
    path = os.path.join(run_dir, "metadata.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def request_series(run_dir):
    """Stream requests.csv once and return (latency_ns, ok_mask, last_end_ns).

    The high-concurrency conditions write ~1e7 request rows per run (the
    dataset totals ~9e7 requests, 12 GB), so the file is read once, with a
    column-indexed csv.reader into flat arrays. csv.DictReader (a dict per
    row) and sorting multi-million-element Python lists for percentiles
    dominated the pipeline's runtime; numpy does both.
    """
    path = os.path.join(run_dir, "requests.csv")
    if not os.path.exists(path):
        return None
    lat = array.array("q")
    ok = array.array("b")
    last_end = None
    with open(path, newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return None  # empty file: no header, no data
        i_lat = header.index("latency_ns")
        i_ok = header.index("ok")
        i_end = header.index("end_ns")
        for row in reader:
            lat.append(int(row[i_lat]))
            ok.append(1 if row[i_ok] == "1" else 0)
            end = int(row[i_end])
            if last_end is None or end > last_end:
                last_end = end
    return (np.frombuffer(lat, dtype=np.int64),
            np.frombuffer(ok, dtype=np.int8).astype(bool),
            last_end)


def pct_of(arr, p):
    """Percentile of a numpy array, 'lower' method (same convention as the
    previous sorted-list implementation). None for an empty sample."""
    if arr is None or arr.size == 0:
        return None
    return int(np.percentile(arr, p * 100, method="lower"))


def detect_anomalies(run_dir):
    """Detect host-level contamination from host-telemetry.csv.

    Returns a list of anomaly strings. Anomalies are recorded and reported;
    they are never silently deleted. Exclusion is a separate, predefined
    decision made by the report.

    The CPU-starvation threshold is relative to the host's CPU count (0.9 ×
    NumCPU): the benchmark itself legitimately uses ~9 cores, so a fixed
    absolute threshold would flag the benchmark's own load as starvation.
    """
    path = os.path.join(run_dir, "host-telemetry.csv")
    if not os.path.exists(path):
        return []
    rows = read_csv(path)
    if not rows:
        return []
    # Host CPU count from metadata (fallback: 12).
    meta = load_metadata(run_dir) or {}
    host_cpus = (meta.get("host") or {}).get("cpus", 12)
    load_threshold = 0.9 * host_cpus
    anomalies = []
    prev_ts = None
    for r in rows:
        try:
            ts = int(r["ts_ns"])
        except (KeyError, ValueError):
            continue
        if prev_ts is not None:
            dt = ts - prev_ts
            if dt < 0:
                anomalies.append(f"clock_jump: telemetry went backwards by {-dt/1e6:.0f}ms")
            elif dt > 5e9:
                anomalies.append(f"host_pause: telemetry gap of {dt/1e9:.1f}s")
        prev_ts = ts
        try:
            load1 = float(r["load1"])
        except (KeyError, ValueError):
            continue
        if load1 > load_threshold:
            anomalies.append(f"cpu_starvation: host load1={load1:.1f} exceeds threshold {load_threshold:.1f}")
    # Deduplicate.
    return list(dict.fromkeys(anomalies))


def measured_elections(run_dir, replicas=3):
    """Actual number of failed elections, from the Raft adapters' measured
    counters.

    HashiCorp Raft v1.7.3 enables pre-vote by default. A failed election
    attempt is either (a) a pre-vote round that receives no response (the
    candidate sends RequestPreVote to every peer, all dropped while the
    transport is isolated), or (b) a real vote round whose pre-vote
    succeeded but whose RequestVote was dropped. Each failed attempt
    therefore produces (replicas-1) dropped messages of one kind. The sum of
    dropped pre-votes and dropped votes across replicas, divided by
    (replicas-1), is the number of failed election attempts. None if not an
    election run or the counters are unavailable.
    """
    path = os.path.join(run_dir, "stats.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        try:
            stats = json.load(f)
        except json.JSONDecodeError:
            return None
    total_pre = 0
    total_votes = 0
    seen = False
    for s in stats:
        if "dropped_pre_votes" in s:
            seen = True
            total_pre += int(s.get("dropped_pre_votes", 0))
            total_votes += int(s.get("dropped_votes", 0))
    if not seen:
        return None
    denom = max(1, replicas - 1)
    return round((total_pre + total_votes) / denom)


def validate_run(run_dir, run_id):
    """Return (ok, reason, series). A run is valid if it has metadata,
    requests with at least one success, and resource samples. `series` is the
    single parsed pass over requests.csv, handed to the processing steps so
    the file is never read twice."""
    meta = load_metadata(run_dir)
    if meta is None:
        return False, "metadata.json missing", None
    if meta.get("status") != "success":
        return False, f"status={meta.get('status')} {meta.get('failure_reason') or ''}", None
    res_path = os.path.join(run_dir, "resources.csv")
    if not os.path.exists(res_path) or os.path.getsize(res_path) == 0:
        return False, "resources.csv missing/empty", None
    series = request_series(run_dir)
    if series is None or series[1].size == 0:
        return False, "requests.csv empty", None
    if not series[1].any():
        return False, "no successful requests", None
    return True, "", series


def measured_phase_bounds(run_dir, last_request_end_ns):
    """(start_ns, end_ns) of the measured phase: phase.json start to the last
    request end. Returns (None, None) if unknown."""
    phase_path = os.path.join(run_dir, "phase.json")
    start = None
    if os.path.exists(phase_path):
        with open(phase_path) as f:
            phase = json.load(f)
        start = phase.get("phase_started_ns")
    return start, last_request_end_ns


def split_attempt(run_id):
    """(identity, repetition, attempt) of a run from its run ID.

    A run ID is `<identity>-<repetition>`; a replacement run created by
    `runner matrix --rerun-contaminated` appends its attempt number
    (`<identity>-<repetition>-2`). The identity never ends in a bare number,
    so the split is unambiguous."""
    parts = run_id.split("-")
    if len(parts) >= 2 and parts[-1].isdigit() and parts[-2].isdigit():
        return "-".join(parts[:-2]), int(parts[-2]), int(parts[-1])
    if parts and parts[-1].isdigit():
        return "-".join(parts[:-1]), int(parts[-1]), 1
    return run_id, 0, 1


def mark_included(rows):
    """Apply the documented run-selection rule and set `included` on every
    row. Returns (rows, n_excluded).

    Rule (fixed in advance, applied uniformly to every condition):
      1. A run whose host telemetry was flagged contaminated (host suspend /
         pause, CPU starvation, clock jump) is excluded from aggregation.
         Contaminated runs are never deleted: they stay in metrics.csv and
         run-index.json with included=0 and their anomaly list.
      2. When several attempts exist for the same (config, repetition) -- a
         replacement run created after a contaminated or failed attempt --
         the non-contaminated attempt with the highest attempt number is
         used. Every condition therefore keeps exactly one observation per
         planned repetition.
    """
    best = {}
    for r in rows:
        if r["contaminated"]:
            continue
        key = (config_key(r), r["repetition"])
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


def process_run(run_dir, run_id, series):
    """Compute aggregated metrics for one valid run from its parsed request
    series (see request_series)."""
    meta = load_metadata(run_dir)
    cfg = meta["config"]
    lat, ok_mask, last_end = series
    identity, repetition, attempt = split_attempt(run_id)
    anomalies = detect_anomalies(run_dir)

    ok_lats = lat[ok_mask]

    # Throughput over the measured phase (from phase.json if present).
    phase_path = os.path.join(run_dir, "phase.json")
    phase_s = None
    if os.path.exists(phase_path):
        with open(phase_path) as f:
            phase = json.load(f)
        start = phase.get("phase_started_ns")
        if start and last_end:
            phase_s = (last_end - start) / 1e9

    total = int(lat.size)
    ok = int(ok_mask.sum())
    duration = phase_s or (cfg.get("duration_s") or 0)
    throughput = ok / duration if duration and duration > 0 else None

    metrics = {
        "run_id": run_id,
        "experiment": meta.get("experiment", ""),
        "protocol": cfg["protocol"],
        "replicas": cfg["replicas"],
        "read_pct": cfg["read_pct"],
        "write_pct": cfg["write_pct"],
        "concurrency": cfg["concurrency"],
        "conflict_pct": cfg.get("conflict_pct", 0),
        "failure_mode": cfg["failure"]["mode"],
        "failed_elections_target": cfg.get("failure", {}).get("failed_elections", 0),
        "measured_elections": measured_elections(run_dir, cfg.get("replicas", 3)),
        "status": "success",
        "requests_total": total,
        "requests_ok": ok,
        "requests_failed": total - ok,
        "success_rate": ok / total if total else None,
        "throughput_req_s": throughput,
        "latency_ns_p50": pct_of(ok_lats, 0.50),
        "latency_ns_p95": pct_of(ok_lats, 0.95),
        "latency_ns_p99": pct_of(ok_lats, 0.99),
        "latency_ns_mean": float(ok_lats.mean()) if ok_lats.size else None,
        "latency_ns_p50_all": pct_of(lat, 0.50),
        "latency_ns_p95_all": pct_of(lat, 0.95),
        "latency_ns_p99_all": pct_of(lat, 0.99),
        "phase_duration_s": duration,
        "anomalies": anomalies,
        "contaminated": bool(anomalies),
        "repetition": repetition,
        "attempt": attempt,
        "identity": identity,
    }
    return metrics


def process_resources(run_dir, run_id, series):
    """Aggregate per-replica resource samples (raw samples are preserved in
    results/raw; this only summarizes them). Samples are filtered to the
    measured phase so warm-up and teardown are excluded."""
    meta = load_metadata(run_dir)
    cfg = meta["config"]
    path = os.path.join(run_dir, "resources.csv")
    rows = read_csv(path)
    start_ns, end_ns = measured_phase_bounds(run_dir, series[2])

    by_replica = defaultdict(list)
    for r in rows:
        if r.get("role") != "replica":
            continue
        try:
            ts = int(r["ts_ns"])
        except (KeyError, ValueError):
            continue
        if start_ns is not None and ts < start_ns:
            continue
        if end_ns is not None and ts > end_ns:
            continue
        by_replica[r["replica_id"]].append(r)

    out = []
    for rid, samples in sorted(by_replica.items(), key=lambda kv: int(kv[0])):
        samples.sort(key=lambda s: int(s["ts_ns"]))
        if len(samples) < 2:
            continue
        t0 = int(samples[0]["ts_ns"])
        t1 = int(samples[-1]["ts_ns"])
        span = (t1 - t0) / 1e9
        cpu0 = int(samples[0]["cpu_usage_usec"])
        cpu1 = int(samples[-1]["cpu_usage_usec"])
        rx0 = int(samples[0]["net_rx_bytes"])
        rx1 = int(samples[-1]["net_rx_bytes"])
        tx0 = int(samples[0]["net_tx_bytes"])
        tx1 = int(samples[-1]["net_tx_bytes"])
        rss = max(int(s["rss_bytes"]) for s in samples)
        out.append({
            "run_id": run_id,
            "experiment": meta.get("experiment", ""),
            "protocol": cfg["protocol"],
            "replicas": cfg["replicas"],
            "replica_id": int(rid),
            "samples": len(samples),
            "span_s": span,
            "cpu_usage_usec_total": cpu1 - cpu0,
            "cpu_util_pct": (cpu1 - cpu0) / 1e6 / span * 100 if span > 0 else None,
            "net_rx_bytes_total": rx1 - rx0,
            "net_tx_bytes_total": tx1 - tx0,
            "net_rx_bps": (rx1 - rx0) / span if span > 0 else None,
            "net_tx_bps": (tx1 - tx0) / span if span > 0 else None,
            "rss_bytes_max": rss,
        })
    return out


def process_failure(run_dir, run_id):
    """Extract the failure/recovery timeline from events.csv and requests.csv."""
    ev_path = os.path.join(run_dir, "events.csv")
    if not os.path.exists(ev_path):
        return None
    events = read_csv(ev_path)
    if not any(e["event"] == "failure_injected" for e in events):
        return None

    phase_path = os.path.join(run_dir, "phase.json")
    t0 = None
    if os.path.exists(phase_path):
        with open(phase_path) as f:
            t0 = json.load(f).get("phase_started_ns")

    timeline = {}
    for e in events:
        ts = int(e["ts_ns"])
        rel = (ts - t0) / 1e9 if t0 else None
        timeline[e["event"]] = {"ts_ns": ts, "rel_s": rel, "detail": e["detail"]}

    rows = read_csv(os.path.join(run_dir, "requests.csv"))
    fail_ts = timeline.get("failure_injected", {}).get("ts_ns")

    # Availability gap: the longest interval with NO successful completion,
    # starting at or after the failure. In-flight requests that complete just
    # after the kill are genuine successes and do not end the gap; the gap is
    # the quiet period between the last success before it and the first
    # success after it.
    gap_s = None
    gap_start_rel = None
    gap_end_rel = None
    if fail_ts and t0:
        success_ends = sorted(
            int(r["end_ns"]) for r in rows if r.get("ok") == "1"
        )
        best = 0
        best_pair = None
        for i in range(1, len(success_ends)):
            prev, nxt = success_ends[i - 1], success_ends[i]
            if prev >= fail_ts and (nxt - prev) > best:
                best = nxt - prev
                best_pair = (prev, nxt)
        # If no successes at all after the failure, the gap runs from the
        # failure to the end of the measured phase.
        if best_pair is None and not any(e >= fail_ts for e in success_ends):
            last_before = max((e for e in success_ends if e < fail_ts), default=fail_ts)
            end = max(int(r["end_ns"]) for r in rows)
            best = end - last_before
            best_pair = (last_before, end)
        if best_pair:
            gap_s = best / 1e9
            gap_start_rel = (best_pair[0] - t0) / 1e9
            gap_end_rel = (best_pair[1] - t0) / 1e9

    # Degradation onset: first failed request at or after the failure.
    degrade_rel = None
    for r in sorted(rows, key=lambda r: int(r["end_ns"])):
        if r.get("ok") != "1" and int(r["end_ns"]) >= (fail_ts or 0):
            degrade_rel = (int(r["end_ns"]) - t0) / 1e9 if t0 else None
            break

    # Decoupled recovery metric (election runs only): time from the END of
    # the isolation window to the first successful request. The availability
    # gap above includes the isolation duration, which is proportional to the
    # configured target (isoMS = target x election_timeout), so its
    # correlation with the measured election count is partly mechanical: each
    # failed election consumes one election timeout by construction. This
    # metric measures only the recovery behaviour AFTER the injection has
    # expired (election + client reconnect), decoupled from the injection
    # duration. If it is roughly constant across the measured election count,
    # the gap correlation is fully explained by the injection mechanism; if
    # it grows, the cluster needs extra time to stabilise after many failed
    # elections.
    meta = load_metadata(run_dir)
    cfg = (meta or {}).get("config", {})
    iso_end = isolation_expiry_ns(events, cfg)
    recovery_from_iso_end_s = None
    if iso_end is not None and t0:
        first_success = None
        for r in sorted(rows, key=lambda r: int(r["end_ns"])):
            if r.get("ok") == "1" and int(r["end_ns"]) >= iso_end:
                first_success = int(r["end_ns"])
                break
        if first_success is not None:
            recovery_from_iso_end_s = (first_success - iso_end) / 1e9

    return {
        "run_id": run_id,
        "experiment": (meta or {}).get("experiment", ""),
        "failure_mode": cfg.get("failure", {}).get("mode", ""),
        "protocol": cfg.get("protocol", ""),
        "timeline": timeline,
        "write_availability_gap_s_approx": gap_s,
        "gap_start_rel_s": gap_start_rel,
        "gap_end_rel_s": gap_end_rel,
        "degradation_onset_rel_s": degrade_rel,
        "recovery_from_isolation_end_s": recovery_from_iso_end_s,
        "gap_note": (
            "approximate: the largest interval with no successful completion "
            "at or after failure_injected; resolution is limited by request "
            "timing and the ~200ms resource monitor"
        ),
    }


def isolation_expiry_ns(events, cfg):
    """End of the election-failure isolation window, in ns since epoch.

    The isolation is applied per surviving replica with the same duration
    (isoMS = failed_elections_target x raft_election_ms); each replica's
    isolation expires at its own confirmation timestamp plus that duration.
    The cluster-wide isolation end is therefore the LATEST confirmation
    timestamp plus the duration. For target 0 (no isolation) the window is
    the kill itself, so the expiry is the failure_injected timestamp. None
    if the run is not an election run or the kill event is missing.
    """
    if cfg.get("failure", {}).get("mode") != "election":
        return None
    iso_ms = (int(cfg.get("failure", {}).get("failed_elections", 0))
              * int(cfg.get("raft_election_ms", 2000)))
    fail_ts = None
    confirmed = []
    for e in events:
        if e["event"] == "failure_injected":
            fail_ts = int(e["ts_ns"])
        elif e["event"] == "election_isolation_confirmed":
            confirmed.append(int(e["ts_ns"]))
    if fail_ts is None:
        return None
    if iso_ms <= 0 or not confirmed:
        return fail_ts
    return max(confirmed) + iso_ms * 1_000_000


def config_key(m):
    """Identity of a configuration from a metrics row (all independent
    variables, including the experiment family).

    The family must be part of the key: workload, concurrency, conflict (at
    conflict_pct 0) and scaling-r3 all share the same protocol/replicas/mix/
    concurrency values, and grouping on those alone silently merges four
    different experiments into one aggregate.
    """
    return (m.get("experiment", ""), m["protocol"], m["replicas"], m["read_pct"],
            m["write_pct"], m["concurrency"], m["conflict_pct"], m["failure_mode"],
            m.get("failed_elections_target", 0))


def summarize_configs(all_metrics):
    """Per-configuration statistics (n, mean, median, SD, 95% CI) computed
    from the raw per-run observations that passed the selection rule (see
    mark_included). This is the source for every aggregate the report and
    figures display. `n_observed` counts every valid-status run of the
    condition, so excluded runs stay visible."""
    groups = defaultdict(list)
    observed = defaultdict(int)
    for m in all_metrics:
        key = config_key(m)
        observed[key] += 1
        if m.get("included"):
            groups[key].append(m)
    for key in observed:
        groups.setdefault(key, [])

    out = []
    for key, runs in sorted(groups.items()):
        exp, proto, replicas, rp, wp, conc, conflict, fmode, ftarget = key
        tputs = [m["throughput_req_s"] for m in runs if m["throughput_req_s"] is not None]
        p50s = [m["latency_ns_p50"] for m in runs if m["latency_ns_p50"] is not None]
        p95s = [m["latency_ns_p95"] for m in runs if m["latency_ns_p95"] is not None]
        p99s = [m["latency_ns_p99"] for m in runs if m["latency_ns_p99"] is not None]
        gaps = [m["write_availability_gap_s_approx"] for m in runs
                if m.get("write_availability_gap_s_approx") is not None]
        recov = [m["recovery_from_isolation_end_s"] for m in runs
                 if m.get("recovery_from_isolation_end_s") is not None]
        melec = [m["measured_elections"] for m in runs if m.get("measured_elections") is not None]

        def stat(vals):
            return {
                "n": len(vals),
                "mean": labstats.mean(vals),
                "median": labstats.median(vals),
                "sd": labstats.stdev(vals),
                "ci95_lo": labstats.ci95(vals)[0],
                "ci95_hi": labstats.ci95(vals)[1],
            }

        row = {
            "experiment": exp,
            "protocol": proto, "replicas": replicas, "read_pct": rp,
            "write_pct": wp, "concurrency": conc, "conflict_pct": conflict,
            "failure_mode": fmode, "failed_elections_target": ftarget,
            "n_observed": observed[key],
            "n_excluded": observed[key] - len(runs),
            "throughput": stat(tputs),
            "latency_p50": stat(p50s),
            "latency_p95": stat(p95s),
            "latency_p99": stat(p99s),
            "availability_gap": stat(gaps),
            "recovery_from_isolation_end": stat(recov),
            "measured_elections": stat(melec),
        }
        out.append(row)
    return out


def flatten_summary(rows):
    """Flatten config-summary rows into CSV columns."""
    flat = []
    for r in rows:
        base = {k: v for k, v in r.items() if not isinstance(v, dict)}
        for metric in ("throughput", "latency_p50", "latency_p95", "latency_p99",
                       "availability_gap", "recovery_from_isolation_end",
                       "measured_elections"):
            s = r.get(metric, {})
            for field in ("n", "mean", "median", "sd", "ci95_lo", "ci95_hi"):
                flat.append({**base, "metric": metric, "stat": field,
                             "value": s.get(field)})
    return flat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="results/raw")
    ap.add_argument("--processed", default="results/processed")
    args = ap.parse_args()

    os.makedirs(args.processed, exist_ok=True)

    run_dirs = sorted(
        d for d in os.listdir(args.raw)
        if os.path.isdir(os.path.join(args.raw, d))
    )

    all_metrics = []
    all_resources = []
    all_failures = []
    index = []
    valid = 0
    for run_id in run_dirs:
        run_dir = os.path.join(args.raw, run_id)
        ok, reason, series = validate_run(run_dir, run_id)
        anomalies = detect_anomalies(run_dir)
        index.append({
            "run_id": run_id, "valid": ok, "reason": reason,
            "anomalies": anomalies,
            "contaminated": bool(anomalies),
        })
        if not ok:
            print(f"  skip {run_id}: {reason}")
            continue
        valid += 1
        m = process_run(run_dir, run_id, series)
        all_metrics.append(m)
        all_resources.extend(process_resources(run_dir, run_id, series))
        f = process_failure(run_dir, run_id)
        if f:
            meta = load_metadata(run_dir)
            n_replicas = (meta or {}).get("config", {}).get("replicas", 3)
            f["measured_elections"] = measured_elections(run_dir, n_replicas)
            all_failures.append(f)

    # Apply the documented run-selection rule (host contamination, with
    # replacement runs taking precedence) before anything is aggregated.
    all_metrics, n_excluded = mark_included(all_metrics)
    # The failure/recovery records must carry the same verdict: an aggregate
    # computed from failures.json must not include a run the rule excluded.
    included_ids = {m["run_id"] for m in all_metrics if m["included"]}
    for f in all_failures:
        f["included"] = 1 if f["run_id"] in included_ids else 0

    # Write aggregated metrics.
    with open(os.path.join(args.processed, "metrics.csv"), "w", newline="") as f:
        if all_metrics:
            w = csv.DictWriter(f, fieldnames=list(all_metrics[0].keys()))
            w.writeheader()
            w.writerows(all_metrics)
    with open(os.path.join(args.processed, "resources.csv"), "w", newline="") as f:
        if all_resources:
            w = csv.DictWriter(f, fieldnames=list(all_resources[0].keys()))
            w.writeheader()
            w.writerows(all_resources)
    with open(os.path.join(args.processed, "failures.json"), "w") as f:
        json.dump(all_failures, f, indent=2)
    with open(os.path.join(args.processed, "run-index.json"), "w") as f:
        json.dump(index, f, indent=2)

    # Per-configuration statistics with n and 95% CI.
    summary = summarize_configs(all_metrics)
    with open(os.path.join(args.processed, "config-summary.csv"), "w", newline="") as f:
        flat = flatten_summary(summary)
        if flat:
            w = csv.DictWriter(f, fieldnames=list(flat[0].keys()))
            w.writeheader()
            w.writerows(flat)
    with open(os.path.join(args.processed, "config-summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"processed {valid}/{len(run_dirs)} runs")
    print(f"  excluded from aggregation: {n_excluded} (host-contaminated or superseded by a replacement run)")
    print(f"  metrics: {os.path.join(args.processed, 'metrics.csv')}")
    print(f"  resources: {os.path.join(args.processed, 'resources.csv')}")
    print(f"  failures: {os.path.join(args.processed, 'failures.json')}")
    print(f"  config-summary: {os.path.join(args.processed, 'config-summary.csv')}")


if __name__ == "__main__":
    sys.exit(main())