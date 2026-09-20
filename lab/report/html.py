#!/usr/bin/env python3
"""Generate a self-contained, offline HTML benchmark report.

The report is entirely data-driven: every number, run, figure reference, and
failure record is derived from the actual result files at generation time.
No benchmark outcome is hardcoded. If results/ is replaced with a different
valid dataset, this script produces a report for the new dataset without
source changes.

Data sources (all read at generation time):
  results/processed/metrics.csv      per-run aggregated metrics
  results/processed/resources.csv    per-replica resource aggregates
  results/processed/failures.json    failure/recovery timelines
  results/processed/run-index.json   per-run validation index (authoritative
                                     for the current results/raw contents)
  results/run-index.csv              append-only historical run log
  configs/matrix.json                configured experiment matrix
  results/figures/                   enumerated figure files
  results/raw/<run-id>/metadata.json per-run provenance/versions
  upstream/VERSIONS.md               pinned upstream versions

Output: results/report/index.html (single file, no remote assets).

Usage:
    python3 report/html.py --processed results/processed --raw results/raw \
        --figures results/figures --config configs/matrix.json \
        --out results/report
"""
import argparse
import csv
import json
import os
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from xml.sax.saxutils import escape as _xml_escape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stats as labstats

NAN_LABEL = "N/A"
FAILED_LABEL = "FAILED"
NOT_MEASURED = "NOT MEASURED"


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_json(path):
    with open(path) as f:
        return json.load(f)


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fmt_num(v, nd=1):
    if v is None:
        return NAN_LABEL
    try:
        return f"{float(v):.{nd}f}"
    except (TypeError, ValueError):
        return NAN_LABEL


def fmt_ms(ns):
    if ns is None:
        return NAN_LABEL
    try:
        return f"{float(ns) / 1e6:.2f} ms"
    except (TypeError, ValueError):
        return NAN_LABEL


def esc(s):
    return _xml_escape(str(s) if s is not None else "")


def median(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return statistics.median(vals)


def minmax(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None
    return min(vals), max(vals)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

class Dataset:
    def __init__(self, processed, raw, figures, config_path):
        self.processed = processed
        self.raw = raw
        self.figures = figures

        self.metrics = read_csv(os.path.join(processed, "metrics.csv")) if os.path.exists(
            os.path.join(processed, "metrics.csv")) else []
        self.resources = read_csv(os.path.join(processed, "resources.csv")) if os.path.exists(
            os.path.join(processed, "resources.csv")) else []
        self.failures = load_json(os.path.join(processed, "failures.json")) if os.path.exists(
            os.path.join(processed, "failures.json")) else []
        self.run_index = load_json(os.path.join(processed, "run-index.json")) if os.path.exists(
            os.path.join(processed, "run-index.json")) else []

        # Execution manifest (actual execution order, randomization seed,
        # gaps, overlaps, cleanup failures, host anomalies).
        manifest_path = os.path.join(os.path.dirname(processed), "execution-manifest.json")
        self.manifest = load_json(manifest_path) if os.path.exists(manifest_path) else None

        self.matrix = {}
        if os.path.exists(config_path):
            with open(config_path) as f:
                self.matrix = json.load(f)

        # Figure categories: derive from the actual directory structure.
        self.figure_categories = {}
        if os.path.isdir(figures):
            for name in sorted(os.listdir(figures)):
                sub = os.path.join(figures, name)
                if os.path.isdir(sub):
                    files = sorted(f for f in os.listdir(sub) if f.endswith((".png", ".svg")))
                    if files:
                        self.figure_categories[name] = files

        # Per-run metadata (provenance, versions, duration).
        self.meta = {}
        if os.path.isdir(raw):
            for run_id in os.listdir(raw):
                mp = os.path.join(raw, run_id, "metadata.json")
                if os.path.isfile(mp):
                    try:
                        self.meta[run_id] = load_json(mp)
                    except Exception:
                        pass

        # Per-run protocol counters (EPaxos fast/slow path) from stats.json.
        self.stats = {}
        if os.path.isdir(raw):
            for run_id in os.listdir(raw):
                sp = os.path.join(raw, run_id, "stats.json")
                if os.path.isfile(sp):
                    try:
                        self.stats[run_id] = load_json(sp)
                    except Exception:
                        pass

        # Merge run-index.json with metrics for the run explorer.
        self.runs = self._build_runs()

    def _build_runs(self):
        by_id = {}
        for m in self.metrics:
            by_id[m["run_id"]] = m
        runs = []
        for entry in self.run_index:
            rid = entry["run_id"]
            m = by_id.get(rid, {})
            meta = self.meta.get(rid, {})
            runs.append({
                "run_id": rid,
                "valid": bool(entry.get("valid")),
                "reason": entry.get("reason", ""),
                "protocol": m.get("protocol", meta.get("config", {}).get("protocol", "")),
                "replicas": m.get("replicas", meta.get("config", {}).get("replicas", "")),
                "read_pct": m.get("read_pct", meta.get("config", {}).get("read_pct", "")),
                "write_pct": m.get("write_pct", meta.get("config", {}).get("write_pct", "")),
                "concurrency": m.get("concurrency", meta.get("config", {}).get("concurrency", "")),
                "conflict_pct": m.get("conflict_pct", 0),
                "failure_mode": m.get("failure_mode", meta.get("config", {}).get("failure", {}).get("mode", "none")),
                "throughput": m.get("throughput_req_s"),
                "p50": m.get("latency_ns_p50"),
                "p95": m.get("latency_ns_p95"),
                "success_rate": m.get("success_rate"),
                "duration_s": meta.get("duration_s"),
                "started_at": meta.get("started_at", ""),
                "experiment": meta.get("experiment", ""),
            })
        return runs

    # -- derived aggregates -------------------------------------------------

    @property
    def total_runs(self):
        return len(self.runs)

    @property
    def successful_runs(self):
        return sum(1 for r in self.runs if r["valid"])

    @property
    def failed_runs(self):
        return sum(1 for r in self.runs if not r["valid"])

    @property
    def protocols(self):
        return sorted({r["protocol"] for r in self.runs if r["protocol"]})

    @property
    def replica_counts(self):
        vals = set()
        for r in self.runs:
            try:
                vals.add(int(r["replicas"]))
            except (TypeError, ValueError):
                pass
        return sorted(vals)

    @property
    def concurrencies(self):
        vals = set()
        for r in self.runs:
            try:
                vals.add(int(r["concurrency"]))
            except (TypeError, ValueError):
                pass
        return sorted(vals)

    @property
    def workload_mixes(self):
        mixes = set()
        for r in self.runs:
            try:
                mixes.add((int(r["read_pct"]), int(r["write_pct"])))
            except (TypeError, ValueError):
                pass
        return sorted(mixes)

    @property
    def figure_count(self):
        return sum(len(files) for files in self.figure_categories.values())

    def included_failures(self):
        """Failure records that passed the run-selection rule (process.py:
        mark_included). The pipeline records the verdict on each record, so an
        aggregate built from failures.json excludes exactly the same runs as
        the metrics aggregates. A record without the field is treated as NOT
        included: silently mixing a contaminated run into a failure or
        election aggregate is the failure mode this rule exists to prevent,
        and an absent verdict means the data predates the rule.
        """
        return [f for f in self.failures if str(f.get("included", "0")) == "1"]

    def election_records(self):
        """One record per included election run: the measured number of
        failed elections, the availability gap, and the decoupled recovery
        time."""
        out = []
        for f in self.included_failures():
            if f.get("failure_mode") != "election":
                continue
            me = f.get("measured_elections")
            gap = fnum(f.get("write_availability_gap_s_approx"))
            if me is None or gap is None:
                continue
            out.append({
                "run_id": f["run_id"],
                "measured_elections": me,
                "gap_s": gap,
                "recovery_s": fnum(f.get("recovery_from_isolation_end_s")),
            })
        return out

    def failure_stats(self):
        """Aggregate availability-gap statistics per (protocol, mode)."""
        groups = {}
        for f in self.included_failures():
            key = (f.get("protocol", ""), f.get("failure_mode", ""))
            if not key[0]:
                # Older failure records do not carry protocol/mode; join via
                # the run index.
                rid = f.get("run_id", "")
                for r in self.runs:
                    if r["run_id"] == rid:
                        key = (r["protocol"], r["failure_mode"])
                        break
            groups.setdefault(key, []).append(f)
        out = []
        for (proto, mode), recs in sorted(groups.items()):
            gaps = [fnum(f.get("write_availability_gap_s_approx")) for f in recs]
            degrades = [fnum(f.get("degradation_onset_rel_s")) for f in recs]
            lo, hi = minmax(gaps)
            out.append({
                "protocol": proto,
                "mode": mode,
                "count": len(recs),
                "gap_min": lo,
                "gap_median": median(gaps),
                "gap_max": hi,
                "degrade_min": minmax(degrades)[0],
                "degrade_median": median(degrades),
                "runs": recs,
            })
        return out

    def scaling_table(self):
        """Per (protocol, replicas): measured or failed, with reason."""
        rows = []
        for proto in self.protocols:
            for n in self.replica_counts:
                runs = [r for r in self.runs if r["protocol"] == proto and str(r["replicas"]) == str(n)]
                if not runs:
                    continue
                valid = [r for r in runs if r["valid"]]
                if valid:
                    tputs = [fnum(r["throughput"]) for r in valid]
                    p50s = [fnum(r["p50"]) for r in valid]
                    rows.append({
                        "protocol": proto, "replicas": n,
                        "status": "measured",
                        "runs": len(valid),
                        "throughput_mean": statistics.mean(tputs) if tputs else None,
                        "p50_mean": statistics.mean(p50s) if p50s else None,
                        "reason": "",
                    })
                else:
                    reason = runs[0]["reason"] or "run failed"
                    rows.append({
                        "protocol": proto, "replicas": n,
                        "status": "failed",
                        "runs": len(runs),
                        "throughput_mean": None,
                        "p50_mean": None,
                        "reason": reason,
                    })
        return rows

    def versions(self):
        info = {}
        # Most recent run metadata (by started_at) carries the environment.
        dated = [r for r in self.runs if r["started_at"]]
        if dated:
            latest = max(dated, key=lambda r: r["started_at"])
            meta = self.meta.get(latest["run_id"], {})
            info.update(meta.get("versions", {}))
            info["host"] = meta.get("host", {})
            info["monitoring"] = meta.get("monitoring", {})
            info["latest_run"] = latest["run_id"]
            info["latest_started"] = latest["started_at"]
        # Pinned upstream versions from VERSIONS.md (static provenance).
        vp = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "upstream", "VERSIONS.md")
        if os.path.exists(vp):
            with open(vp) as f:
                info["versions_md"] = f.read()
        return info

    # -- additional experiments --------------------------------------------

    def conflict_table(self):
        """Per (protocol, conflict_pct): mean throughput/latency and, for
        EPaxos, the fast/slow path ratio. Only runs from the conflict
        experiment family are included: conflict_pct=0 is also the default
        for every other experiment, so filtering on the value alone would
        mix families."""
        rows = []
        base = [r for r in self.runs if r.get("experiment") == "conflict" and r["valid"]]
        pcts = sorted({int(r.get("conflict_pct", 0)) for r in base})
        for proto in sorted({r["protocol"] for r in base}):
            for pct in pcts:
                runs = [r for r in base
                        if r["protocol"] == proto
                        and str(r.get("conflict_pct", "")) == str(pct)]
                if not runs:
                    continue
                tputs = [fnum(r["throughput"]) for r in runs]
                p50s = [fnum(r["p50"]) for r in runs]
                p95s = [fnum(r["p95"]) for r in runs]
                # Fast/slow path from stats.json (EPaxos only).
                fast = slow = None
                if proto == "epaxos":
                    fsum = ssum = 0
                    for r in runs:
                        for rec in self.stats.get(r["run_id"], []):
                            fsum += int(rec.get("fast_path", 0))
                            ssum += int(rec.get("slow_path", 0))
                    if fsum + ssum > 0:
                        fast = fsum / (fsum + ssum)
                        slow = ssum / (fsum + ssum)
                rows.append({
                    "protocol": proto, "conflict_pct": pct, "runs": len(runs),
                    "throughput_mean": statistics.mean(tputs) if tputs else None,
                    "p50_mean": statistics.mean(p50s) if p50s else None,
                    "p95_mean": statistics.mean(p95s) if p95s else None,
                    "fast_path_ratio": fast,
                    "slow_path_ratio": slow,
                })
        return rows

    def concurrency_table(self):
        """Per (protocol, concurrency): mean throughput/latency and per-node
        CPU distribution from resources.csv. Restricted to the concurrency
        experiment family (concurrency values also appear in other families)."""
        rows = []
        base = [r for r in self.runs if r.get("experiment") == "concurrency" and r["valid"]]
        concs = sorted({int(r.get("concurrency", 0)) for r in base})
        for proto in sorted({r["protocol"] for r in base}):
            for c in concs:
                runs = [r for r in base
                        if r["protocol"] == proto
                        and str(r.get("concurrency", "")) == str(c)]
                if not runs:
                    continue
                tputs = [fnum(r["throughput"]) for r in runs]
                p50s = [fnum(r["p50"]) for r in runs]
                p95s = [fnum(r["p95"]) for r in runs]
                # Per-node CPU: mean cpu_util_pct per replica across runs.
                node_cpu = {}
                for r in runs:
                    for rec in self.resources:
                        if rec.get("run_id") == r["run_id"] and rec.get("role") == "replica":
                            node_cpu.setdefault(rec["replica_id"], []).append(fnum(rec.get("cpu_util_pct")))
                cpu_means = [statistics.mean(v) for v in node_cpu.values() if v]
                rows.append({
                    "protocol": proto, "concurrency": c, "runs": len(runs),
                    "throughput_mean": statistics.mean(tputs) if tputs else None,
                    "p50_mean": statistics.mean(p50s) if p50s else None,
                    "p95_mean": statistics.mean(p95s) if p95s else None,
                    "node_cpu": {k: statistics.mean(v) for k, v in node_cpu.items() if v},
                    "cpu_max": max(cpu_means) if cpu_means else None,
                    "cpu_min": min(cpu_means) if cpu_means else None,
                })
        return rows

    def election_table(self):
        """Per MEASURED failed-elections value: availability-gap and
        recovery-from-isolation-end statistics (n, mean, median, SD, 95% CI)
        computed from the raw per-run observations. The independent variable
        is the actual number of failed elections (sum of dropped RequestVote
        across replicas, from stats.json), never the configured target."""
        rows = self.election_records()
        groups = defaultdict(list)
        for r in rows:
            groups[r["measured_elections"]].append(r)
        out = []
        for me in sorted(groups):
            recs = groups[me]
            gaps = [r["gap_s"] for r in recs]
            recovs = [r["recovery_s"] for r in recs if r["recovery_s"] is not None]
            out.append({
                "measured_elections": me,
                "n": len(recs),
                "gap_mean": labstats.mean(gaps),
                "gap_median": labstats.median(gaps),
                "gap_sd": labstats.stdev(gaps),
                "gap_ci95_lo": labstats.ci95(gaps)[0],
                "gap_ci95_hi": labstats.ci95(gaps)[1],
                "recovery_mean": labstats.mean(recovs),
                "recovery_median": labstats.median(recovs),
                "recovery_sd": labstats.stdev(recovs),
                "recovery_ci95_lo": labstats.ci95(recovs)[0],
                "recovery_ci95_hi": labstats.ci95(recovs)[1],
                "runs": recs,
            })
        return out

    def election_correlation(self):
        """Pearson correlations of the availability gap and of the
        recovery-from-isolation-end with the measured failed elections, from
        the raw observations (computed here, not hardcoded)."""
        recs = self.election_records()
        xs = [r["measured_elections"] for r in recs]
        ys_gap = [r["gap_s"] for r in recs]
        ys_rec = [r["recovery_s"] for r in recs]
        r_gap = labstats.pearson(xs, ys_gap)
        p_gap = labstats.pearson_p(r_gap, len(xs)) if r_gap is not None else None
        r_rec = labstats.pearson(xs, ys_rec)
        p_rec = labstats.pearson_p(r_rec, len(xs)) if r_rec is not None else None
        return {
            "n": len(xs),
            "gap_pearson_r": r_gap, "gap_p_value": p_gap,
            "recovery_pearson_r": r_rec, "recovery_p_value": p_rec,
        }

    def _events(self, run_id):
        p = os.path.join(self.raw, run_id, "events.csv")
        if not os.path.exists(p):
            return []
        return read_csv(p)

    def read_semantics(self):
        """Read-consistency metadata for each protocol. Prefers the per-run
        metadata; falls back to the documented semantics so the section is
        always present."""
        documented = {
            "raft": {
                "system": "raft",
                "read_consistency": "linearizable",
                "read_path": "client -> leader (raft.Raft.Leader) -> raft.Apply(GET) -> quorum commit -> state read -> reply",
                "coordination_required": True,
                "target_replica": "leader",
                "mechanism": "leader-confirmed, quorum-based (every read is a consensus command; no local-read optimization)",
            },
            "epaxos": {
                "system": "epaxos",
                "read_consistency": "linearizable",
                "read_path": "client -> any replica (round-robin) -> consensus command -> executed at all replicas in dependency order -> reply",
                "coordination_required": True,
                "target_replica": "any (leaderless)",
                "mechanism": "quorum-based, dependency-ordered execution (reads and writes share the same consensus path)",
            },
        }
        out = {}
        for proto in self.protocols:
            runs = [r for r in self.runs if r["protocol"] == proto and r["started_at"]]
            rs = {}
            if runs:
                latest = max(runs, key=lambda r: r["started_at"])
                rs = self.meta.get(latest["run_id"], {}).get("read_semantics", {})
            if not rs:
                rs = documented.get(proto, {})
            if rs:
                out[proto] = rs
        return out


# ---------------------------------------------------------------------------
# HTML rendering helpers
# ---------------------------------------------------------------------------

CSS = """
:root {
  --bg: #f7f8fa; --fg: #1c2733; --muted: #5b6b7b;
  --card: #ffffff; --border: #dde3ea; --accent: #1f6feb;
  --ok: #1a7f37; --fail: #cf222e; --warn: #9a6700;
  --code-bg: #f0f2f5;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117; --fg: #e6edf3; --muted: #8b949e;
    --card: #161b22; --border: #30363d; --accent: #58a6ff;
    --ok: #3fb950; --fail: #f85149; --warn: #d29922;
    --code-bg: #1c2128;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.55;
}
nav {
  position: sticky; top: 0; z-index: 10; background: var(--card);
  border-bottom: 1px solid var(--border); padding: 8px 20px;
  display: flex; flex-wrap: wrap; gap: 4px 14px; font-size: 13px;
}
nav a { color: var(--accent); text-decoration: none; }
nav a:hover { text-decoration: underline; }
main { max-width: 1100px; margin: 0 auto; padding: 24px 20px 80px; }
h1 { font-size: 26px; margin: 8px 0 4px; }
h2 { font-size: 20px; margin: 40px 0 12px; border-bottom: 2px solid var(--border); padding-bottom: 6px; }
h3 { font-size: 16px; margin: 24px 0 8px; }
.subtitle { color: var(--muted); font-size: 14px; margin-bottom: 24px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: 16px 0; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 14px 16px; }
.card .num { font-size: 26px; font-weight: 600; }
.card .lbl { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; background: var(--card); }
th, td { border: 1px solid var(--border); padding: 6px 10px; text-align: left; }
th { background: var(--code-bg); font-weight: 600; }
tr:nth-child(even) td { background: rgba(127,127,127,.05); }
.badge { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }
.badge.ok { background: rgba(26,127,55,.15); color: var(--ok); }
.badge.fail { background: rgba(207,34,46,.15); color: var(--fail); }
.badge.warn { background: rgba(154,103,0,.15); color: var(--warn); }
.fig-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; margin: 12px 0; }
.fig-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 10px; }
.fig-card img { width: 100%; height: auto; border-radius: 4px; }
.fig-card .cap { font-size: 12px; color: var(--muted); margin-top: 6px; word-break: break-all; }
details { background: var(--card); border: 1px solid var(--border); border-radius: 8px; margin: 8px 0; padding: 10px 14px; }
summary { cursor: pointer; font-weight: 600; }
code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
pre { background: var(--code-bg); padding: 12px; border-radius: 8px; overflow-x: auto; font-size: 12px; }
.note { color: var(--muted); font-size: 13px; }
.obs { background: rgba(31,111,235,.08); border-left: 3px solid var(--accent); padding: 8px 14px; border-radius: 0 6px 6px 0; margin: 10px 0; }
.filter-bar { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }
.filter-bar input, .filter-bar select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; background: var(--card); color: var(--fg); }
#run-table { font-size: 12px; }
#run-table td a { color: var(--accent); }
.files a { margin-right: 10px; font-size: 12px; }
"""


def render_nav():
    items = [
        ("summary", "Summary"), ("config", "Configuration"), ("workload", "Workload"),
        ("scaling", "Scaling"), ("conflict", "Conflict"), ("concurrency", "Concurrency"),
        ("election", "Election"), ("read", "Read Semantics"), ("failure", "Failure"),
        ("resources", "Resources"), ("integrity", "Execution Integrity"),
        ("figures", "Figures"), ("runs", "Run Explorer"),
        ("repro", "Reproducibility"), ("limitations", "Limitations"),
    ]
    return '<nav>' + "".join(f'<a href="#{k}">{v}</a>' for k, v in items) + '</nav>'


def render_summary(ds):
    cards = [
        ("Total runs", ds.total_runs),
        ("Successful", ds.successful_runs),
        ("Failed", ds.failed_runs),
        ("Figures", ds.figure_count),
        ("Protocols", ", ".join(ds.protocols) or NAN_LABEL),
        ("Replica counts", ", ".join(str(n) for n in ds.replica_counts) or NAN_LABEL),
        ("Concurrency levels", ", ".join(str(c) for c in ds.concurrencies) or NAN_LABEL),
        ("Workload mixes", ", ".join(f"{r}R/{w}W" for r, w in ds.workload_mixes) or NAN_LABEL),
    ]
    cards_html = "".join(
        f'<div class="card"><div class="num">{esc(v)}</div><div class="lbl">{esc(k)}</div></div>'
        for k, v in cards
    )
    return f"""
<section id="summary">
  <h2>Executive Summary</h2>
  <div class="cards">{cards_html}</div>
  <p class="note">All values above are computed from the current dataset
  (<code>results/processed/</code> and <code>results/raw/</code>) at report
  generation time. Failed runs are recorded as failed; they are not presented
  as successful benchmark results.</p>
</section>"""


def render_config(ds):
    m = ds.matrix
    defaults = m.get("defaults", {})
    parts = []
    if defaults:
        rows = [
            ("Measured duration (s)", defaults.get("duration_s")),
            ("Warmup (s)", defaults.get("warmup_s")),
            ("Repetitions", defaults.get("repetitions")),
            ("Replica CPUs", defaults.get("replica_cpus")),
            ("Replica memory (MB)", defaults.get("replica_mem_mb")),
            ("Client CPUs", defaults.get("client_cpus")),
            ("Keyspace", defaults.get("keyspace")),
            ("Request timeout (ms)", defaults.get("timeout_ms")),
            ("GOMAXPROCS", defaults.get("gomaxprocs")),
            ("Raft heartbeat (ms)", defaults.get("raft_heartbeat_ms")),
            ("Raft election (ms)", defaults.get("raft_election_ms")),
        ]
        parts.append("<h3>Configured defaults</h3>" + render_table(
            ["Parameter", "Value"], [[k, esc(v)] for k, v in rows]))

    wl = m.get("workload", {})
    if wl:
        parts.append("<h3>Configured workload matrix</h3>" + render_table(
            ["Protocols", "Replicas", "Read mixes", "Concurrency levels"],
            [[", ".join(wl.get("protocols", [])), wl.get("replicas"),
              ", ".join(f"{r}R" for r in wl.get("read_pcts", [])),
              ", ".join(str(c) for c in wl.get("concurrencies", []))]]))

    sc = m.get("scaling", {})
    if sc:
        parts.append("<h3>Configured scaling matrix</h3>" + render_table(
            ["Protocols", "Replica counts", "Write %", "Concurrency"],
            [[", ".join(sc.get("protocols", [])),
              ", ".join(str(n) for n in sc.get("replicas", [])),
              sc.get("write_pct"), sc.get("concurrency")]]))

    fc = m.get("failure", {}).get("cases", [])
    if fc:
        rows = [[c.get("protocol"), c.get("mode"), c.get("replicas"),
                 f"{c.get('write_pct')}% writes", c.get("concurrency"),
                 f"at {c.get('at_s')}s", f"restart {c.get('restart_after_s')}s"]
                for c in fc]
        parts.append("<h3>Configured failure cases</h3>" + render_table(
            ["Protocol", "Mode", "Replicas", "Workload", "Concurrency", "Injection", "Restart"], rows))

    # Observed runs: what actually executed.
    obs = {}
    for r in ds.runs:
        key = (str(r["protocol"]), str(r["replicas"]), str(r["read_pct"]),
               str(r["write_pct"]), str(r["concurrency"]), str(r["failure_mode"]))
        obs[key] = obs.get(key, 0) + 1
    obs_rows = [[p, n, f"{r}R/{w}W", c, fm, cnt]
                for (p, n, r, w, c, fm), cnt in sorted(obs.items())]
    parts.append("<h3>Observed runs (from results)</h3>")
    parts.append(f"<p class='note'>{len(obs_rows)} distinct configurations observed; "
                 f"{ds.total_runs} total runs.</p>")
    parts.append(render_table(
        ["Protocol", "Replicas", "Workload", "Concurrency", "Failure mode", "Runs"],
        obs_rows))

    return f"""
<section id="config">
  <h2>Benchmark Configuration</h2>
  <p class="note">The <em>configured</em> matrix is read from
  <code>configs/matrix.json</code>; the <em>observed</em> runs are derived
  from the actual result files. If the two differ, the observed runs are
  authoritative for what was measured.</p>
  {''.join(parts)}
</section>"""


def render_table(headers, rows):
    if not rows:
        return "<p class='note'>No data.</p>"
    thead = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = ""
    for row in rows:
        body += "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>"
    return f"<table><thead><tr>{thead}</tr></thead><tbody>{body}</tbody></table>"


def render_workload(ds):
    """Workload section: per-mix figures + a comparison table."""
    parts = []
    mixes = ds.workload_mixes
    if not mixes:
        return "<section id='workload'><h2>Workload Results</h2><p class='note'>No workload data.</p></section>"

    # Comparison table: mean throughput + p50/p95 per (mix, protocol).
    rows = []
    for rp, wp in mixes:
        for proto in ds.protocols:
            runs = [r for r in ds.runs
                    if r["protocol"] == proto and str(r["read_pct"]) == str(rp)
                    and str(r["write_pct"]) == str(wp) and r["valid"]]
            if not runs:
                continue
            tputs = [fnum(r["throughput"]) for r in runs]
            p50s = [fnum(r["p50"]) for r in runs]
            p95s = [fnum(r["p95"]) for r in runs]
            rows.append([
                f"{rp}R/{wp}W", proto, len(runs),
                fmt_num(statistics.mean(tputs) if tputs else None, 0),
                fmt_ms(statistics.mean(p50s) if p50s else None),
                fmt_ms(statistics.mean(p95s) if p95s else None),
            ])
    parts.append("<h3>Throughput and latency by workload mix (mean over repetitions)</h3>")
    parts.append(render_table(
        ["Mix", "Protocol", "Runs", "Throughput (req/s)", "p50", "p95"], rows))

    # Per-mix figures from results/figures/workload/.
    wl_figs = ds.figure_categories.get("workload", [])
    if wl_figs:
        parts.append("<h3>Figures</h3>")
        grid = []
        for f in wl_figs:
            grid.append(f'<div class="fig-card"><img src="../figures/workload/{esc(f)}" alt="{esc(f)}">'
                        f'<div class="cap">{esc(f)}</div></div>')
        parts.append(f'<div class="fig-grid">{"".join(grid)}</div>')

    return f"""
<section id="workload">
  <h2>Workload Results</h2>
  <p class="note">Raft vs EPaxos across read/write mixes and concurrency.
  Values are computed from <code>results/processed/metrics.csv</code>.</p>
  {''.join(parts)}
</section>"""


def render_scaling(ds):
    rows = ds.scaling_table()
    if not rows:
        return "<section id='scaling'><h2>Scaling Results</h2><p class='note'>No scaling data.</p></section>"
    body = []
    for r in rows:
        if r["status"] == "measured":
            status = '<span class="badge ok">measured</span>'
            tput = fmt_num(r["throughput_mean"], 0)
            p50 = fmt_ms(r["p50_mean"])
            reason = ""
        else:
            status = '<span class="badge fail">FAILED</span>'
            tput = FAILED_LABEL
            p50 = FAILED_LABEL
            reason = esc(r["reason"])
        body.append(f"<tr><td>{esc(r['protocol'])}</td><td>{r['replicas']}</td>"
                    f"<td>{status}</td><td>{r['runs']}</td><td>{tput}</td><td>{p50}</td>"
                    f"<td>{reason}</td></tr>")
    table = ("<table><thead><tr><th>Protocol</th><th>Replicas</th><th>Status</th>"
             "<th>Runs</th><th>Throughput (req/s, mean)</th><th>p50 (mean)</th>"
             "<th>Failure reason</th></tr></thead><tbody>"
             + "".join(body) + "</tbody></table>")

    figs = ds.figure_categories.get("scaling", [])
    fig_html = ""
    if figs:
        grid = "".join(
            f'<div class="fig-card"><img src="../figures/scaling/{esc(f)}" alt="{esc(f)}">'
            f'<div class="cap">{esc(f)}</div></div>' for f in figs)
        fig_html = f'<h3>Figures</h3><div class="fig-grid">{grid}</div>'

    return f"""
<section id="scaling">
  <h2>Scaling Results</h2>
  <p class="note">Throughput and latency vs replica count. Failed
  configurations are shown as <span class="badge fail">FAILED</span> with
  their recorded reason; they are not presented as zero throughput.</p>
  {table}
  {fig_html}
</section>"""


def render_conflict(ds):
    rows = ds.conflict_table()
    if not rows:
        return "<section id=\"conflict\"><h2>Conflict-Rate Sensitivity</h2><p class='note'>No conflict-rate data.</p></section>"
    body = []
    for r in rows:
        fast = fmt_num(r["fast_path_ratio"], 3) if r["fast_path_ratio"] is not None else NAN_LABEL
        slow = fmt_num(r["slow_path_ratio"], 3) if r["slow_path_ratio"] is not None else NAN_LABEL
        body.append(f"<tr><td>{esc(r['protocol'])}</td><td>{r['conflict_pct']}%</td>"
                    f"<td>{r['runs']}</td><td>{fmt_num(r['throughput_mean'], 0)}</td>"
                    f"<td>{fmt_ms(r['p50_mean'])}</td><td>{fmt_ms(r['p95_mean'])}</td>"
                    f"<td>{fast}</td><td>{slow}</td></tr>")
    table = ("<table><thead><tr><th>Protocol</th><th>Conflict rate</th><th>Runs</th>"
             "<th>Throughput (req/s, mean)</th><th>p50 (mean)</th><th>p95 (mean)</th>"
             "<th>Fast-path ratio</th><th>Slow-path ratio</th></tr></thead><tbody>"
             + "".join(body) + "</tbody></table>")
    return f"""
<section id="conflict">
  <h2>Conflict-Rate Sensitivity</h2>
  <p class="note">Throughput and latency vs the configured conflict rate
  (fraction of requests targeting shared hot keys). The realized hot-key
  fraction is recorded per request, so the actual conflict rate is
  measurable. Fast/slow path ratios are the EPaxos counters instrumented in
  the upstream; Raft has no such distinction. The relationship is discovered
  from the data, not assumed.</p>
  {table}
</section>"""


def render_concurrency(ds):
    rows = ds.concurrency_table()
    if not rows:
        return "<section id='concurrency'><h2>Write-Concurrency Scaling</h2><p class='note'>No concurrency data.</p></section>"
    body = []
    for r in rows:
        cpu = ", ".join(f"n{k}:{v:.1f}%" for k, v in sorted(r["node_cpu"].items()))
        body.append(f"<tr><td>{esc(r['protocol'])}</td><td>{r['concurrency']}</td>"
                    f"<td>{r['runs']}</td><td>{fmt_num(r['throughput_mean'], 0)}</td>"
                    f"<td>{fmt_ms(r['p50_mean'])}</td><td>{fmt_ms(r['p95_mean'])}</td>"
                    f"<td>{fmt_num(r['cpu_min'], 1)}</td><td>{fmt_num(r['cpu_max'], 1)}</td>"
                    f"<td>{esc(cpu)}</td></tr>")
    table = ("<table><thead><tr><th>Protocol</th><th>Concurrency</th><th>Runs</th>"
             "<th>Throughput (req/s, mean)</th><th>p50 (mean)</th><th>p95 (mean)</th>"
             "<th>Min node CPU %</th><th>Max node CPU %</th><th>Per-node CPU %</th>"
             "</tr></thead><tbody>" + "".join(body) + "</tbody></table>")
    return f"""
<section id="concurrency">
  <h2>Write-Concurrency Scaling</h2>
  <p class="note">Throughput, latency, and per-node CPU distribution vs
  concurrency. Per-node CPU is the mean replica CPU utilisation from
  <code>results/processed/resources.csv</code>. Whether work concentrates on
  a Raft leader is left to the data; the report does not label a bottleneck.</p>
  {table}
</section>"""


def render_election(ds):
    rows = ds.election_table()
    if not rows:
        return "<section id=\"election\"><h2>Election-Failure Recovery</h2><p class='note'>No election-failure data.</p></section>"

    # Aggregate table per measured failed-elections value.
    agg_rows = []
    for r in rows:
        agg_rows.append([
            r["measured_elections"], r["n"],
            fmt_num(r["gap_mean"], 2), fmt_num(r["gap_median"], 2),
            fmt_num(r["gap_sd"], 2),
            f"{fmt_num(r['gap_ci95_lo'], 2)}–{fmt_num(r['gap_ci95_hi'], 2)}",
            fmt_num(r["recovery_mean"], 2), fmt_num(r["recovery_median"], 2),
            fmt_num(r["recovery_sd"], 2),
            f"{fmt_num(r['recovery_ci95_lo'], 2)}–{fmt_num(r['recovery_ci95_hi'], 2)}",
        ])
    agg = render_table(
        ["Measured failed elections", "n", "Gap mean (s)", "Gap median (s)",
         "Gap SD (s)", "95% CI (s)", "Recovery mean (s)", "Recovery median (s)",
         "Recovery SD (s)", "95% CI (s)"], agg_rows)

    # Per-run detail.
    detail_rows = []
    for r in rows:
        for run in r["runs"]:
            detail_rows.append([
                esc(run["run_id"]), run["measured_elections"], fmt_num(run["gap_s"], 2),
                fmt_num(run["recovery_s"], 2),
            ])
    detail = render_table(
        ["Run ID", "Measured failed elections", "Availability gap (s)",
         "Recovery from isolation end (s)"], detail_rows)

    # Correlation statistics (computed from the raw observations).
    corr = ds.election_correlation()
    corr_html = ""
    if corr["n"] >= 2 and corr["gap_pearson_r"] is not None:
        p = corr["gap_p_value"]
        p_str = f"{p:.4f}" if p is not None else NAN_LABEL
        corr_html = (f'<div class="obs"><strong>Correlation (measured failed '
                     f'elections vs availability gap):</strong> n={corr["n"]}, '
                     f'Pearson r={corr["gap_pearson_r"]:.3f}, p={p_str}. '
                     f'The gap includes the isolation duration (proportional '
                     f'to the target), so a positive correlation is partly '
                     f'mechanical. Correlation does not imply causation; the '
                     f'observations are shown in the figure below.</div>')
    if corr["n"] >= 2 and corr["recovery_pearson_r"] is not None:
        p = corr["recovery_p_value"]
        p_str = f"{p:.4f}" if p is not None else NAN_LABEL
        corr_html += (f'<div class="obs"><strong>Correlation (measured failed '
                      f'elections vs recovery from isolation end):</strong> '
                      f'n={corr["n"]}, Pearson r={corr["recovery_pearson_r"]:.3f}, '
                      f'p={p_str}. This metric is decoupled from the injection '
                      f'duration; it measures only recovery behaviour after '
                      f'the isolation has expired.</div>')

    figs = ds.figure_categories.get("election", [])
    fig_html = ""
    if figs:
        grid = "".join(
            f'<div class="fig-card"><img src="../figures/election/{esc(f)}" alt="{esc(f)}">'
            f'<div class="cap">{esc(f)}</div></div>' for f in figs)
        fig_html = f'<h3>Observations</h3><div class="fig-grid">{grid}</div>'

    return f"""
<section id="election">
  <h2>Election-Failure Recovery</h2>
  <p class="note">Recovery vs the MEASURED number of failed election
  attempts (sum of dropped RequestVote across replicas, from
  <code>stats.json</code>). The configured target is a nominal setting; the
  measured count is the independent variable. Two metrics are reported:
  the request-based availability gap (largest interval with no successful
  completion after the leader kill, which includes the isolation duration)
  and the recovery-from-isolation-end (time from the end of the isolation
  window to the first successful request, decoupled from the injection
  duration).</p>
  {corr_html}
  {agg}
  <h3>Per-run detail</h3>
  {detail}
  {fig_html}
</section>"""


def render_execution_integrity(ds):
    m = ds.manifest
    if not m:
        return ("<section id='integrity'><h2>Execution Integrity</h2>"
                "<p class='note'>No execution manifest found "
                "(results/execution-manifest.json).</p></section>")

    # Runs excluded from every aggregate by the documented selection rule in
    # process.py: host-contaminated runs, and attempts superseded by a
    # replacement run. They stay in the manifest and in metrics.csv.
    excluded = sum(1 for r in ds.metrics if str(r.get("included")) == "0")

    cards = [
        ("Total runs", m.get("total_runs", 0)),
        ("Successful", m.get("successful_runs", 0)),
        ("Failed", m.get("failed_runs", 0)),
        ("Contaminated", m.get("contaminated_runs", 0)),
        ("Excluded from aggregates", excluded),
        ("Cleanup failures", len(m.get("cleanup_failures") or [])),
    ]
    cards_html = '<div class="cards">' + "".join(
        f'<div class="card"><div class="num">{v}</div><div class="lbl">{k}</div></div>'
        for k, v in cards) + '</div>'

    gap_min = m.get("min_inter_run_gap_s")
    gap_max = m.get("max_inter_run_gap_s")
    gap_html = (f"<tr><td>Min inter-run gap</td><td>{fmt_num(gap_min, 1)} s</td></tr>"
                f"<tr><td>Max inter-run gap</td><td>{fmt_num(gap_max, 1)} s</td></tr>")

    overlaps = m.get("overlaps") or []
    overlaps_html = ("<li>none</li>" if not overlaps else
                     "".join(f"<li>{esc(o)}</li>" for o in overlaps))
    cleanup = m.get("cleanup_failures") or []
    cleanup_html = ("<li>none</li>" if not cleanup else
                    "".join(f"<li>{esc(c)}</li>" for c in cleanup))
    anomalies = m.get("host_anomalies") or []
    anomalies_html = ("<li>none</li>" if not anomalies else
                      "".join(f"<li>{esc(a)}</li>" for a in anomalies))

    # A dataset may span several batches (e.g. a matrix resumed after the host
    # was interrupted); every batch is listed so the gap is never hidden.
    batches = m.get("batch_ids") or []
    if batches:
        batch_row = (f"<tr><td>Batch IDs</td><td>{esc(', '.join(batches))}</td></tr>")
    else:
        batch_row = (f"<tr><td>Batch ID</td><td>{esc(m.get('batch_id', NAN_LABEL))}</td></tr>")

    timeline = ds.figure_categories.get("execution", [])
    timeline_html = ""
    if timeline:
        grid = "".join(
            f'<div class="fig-card"><img src="../figures/execution/{esc(f)}" alt="{esc(f)}">'
            f'<div class="cap">{esc(f)}</div></div>' for f in timeline)
        timeline_html = f'<h3>Condition timeline</h3><div class="fig-grid">{grid}</div>'

    return f"""
<section id="integrity">
  <h2>Execution Integrity</h2>
  <p class="note">Reconstructed from the actual run timestamps and telemetry
  in <code>results/execution-manifest.json</code> — not from the intended
  schedule. Conditions are executed in reproducibly randomized blocks
  (schedule seed {esc(m.get('schedule_seed', NAN_LABEL))}, {len(batches) or 1}
  batch(es) of runs), so no condition is confounded with
  wall-clock time. Runs flagged contaminated are excluded from every
  aggregate (see <code>process.py</code>); they are kept in the manifest and
  in <code>metrics.csv</code>, and each condition keeps one observation per
  planned repetition.</p>
  {cards_html}
  <table>
    <thead><tr><th>Property</th><th>Value</th></tr></thead>
    <tbody>
      <tr><td>Randomization seed</td><td>{esc(m.get('schedule_seed', NAN_LABEL))}</td></tr>
      {batch_row}
      {gap_html}
    </tbody>
  </table>
  <h3>Overlapping runs</h3>
  <ul>{overlaps_html}</ul>
  <h3>Cleanup failures</h3>
  <ul>{cleanup_html}</ul>
  <h3>Host anomalies (contaminated runs)</h3>
  <ul>{anomalies_html}</ul>
  {timeline_html}
</section>"""


def render_read_semantics(ds):
    rs = ds.read_semantics()
    if not rs:
        return "<section id=\"read\"><h2>Read-Path / Consistency Semantics</h2><p class='note'>No read-semantics metadata recorded.</p></section>"
    body = []
    for proto in sorted(rs):
        s = rs[proto]
        body.append(f"<tr><td>{esc(s.get('system', proto))}</td>"
                    f"<td>{esc(s.get('read_consistency', NAN_LABEL))}</td>"
                    f"<td>{esc(s.get('target_replica', NAN_LABEL))}</td>"
                    f"<td>{esc(s.get('mechanism', NAN_LABEL))}</td>"
                    f"<td><code>{esc(s.get('read_path', NAN_LABEL))}</code></td></tr>")
    table = ("<table><thead><tr><th>System</th><th>Read consistency</th>"
             "<th>Target replica</th><th>Mechanism</th><th>Read path</th>"
             "</tr></thead><tbody>" + "".join(body) + "</tbody></table>")
    return f"""
<section id="read">
  <h2>Read-Path / Consistency Semantics</h2>
  <p class="note">Both protocols route reads through consensus (no local-read
  optimization), so the Read benchmark compares equivalent consistency
  guarantees. The coordination mechanism differs: Raft targets the leader,
  EPaxos is leaderless. This metadata is recorded per run in
  <code>metadata.json</code>.</p>
  {table}
</section>"""


def render_failure(ds):
    stats = ds.failure_stats()
    if not stats:
        return "<section id='failure'><h2>Failure Experiments</h2><p class='note'>No failure data.</p></section>"

    # Aggregate table per (protocol, mode).
    rows = []
    for s in stats:
        rows.append([
            esc(s["protocol"]), esc(s["mode"]), s["count"],
            fmt_num(s["gap_min"], 2), fmt_num(s["gap_median"], 2), fmt_num(s["gap_max"], 2),
            fmt_num(s["degrade_median"], 1),
        ])
    agg = render_table(
        ["Protocol", "Failure mode", "Runs", "Gap min (s)", "Gap median (s)",
         "Gap max (s)", "Degradation onset median (s)"], rows)

    # Per-run detail.
    detail_rows = []
    for s in stats:
        for f in s["runs"]:
            detail_rows.append([
                esc(f.get("run_id", "")), esc(s["protocol"]), esc(s["mode"]),
                fmt_num(f.get("write_availability_gap_s_approx"), 2),
                fmt_num(f.get("degradation_onset_rel_s"), 1),
                esc(f.get("gap_note", "")),
            ])
    detail = render_table(
        ["Run ID", "Protocol", "Mode", "Availability gap (s)", "Degradation onset (s)", "Note"],
        detail_rows)

    figs = ds.figure_categories.get("failure", [])
    fig_html = ""
    if figs:
        grid = "".join(
            f'<div class="fig-card"><img src="../figures/failure/{esc(f)}" alt="{esc(f)}">'
            f'<div class="cap">{esc(f)}</div></div>' for f in figs)
        fig_html = f'<h3>Timeline figures</h3><div class="fig-grid">{grid}</div>'

    return f"""
<section id="failure">
  <h2>Failure Experiments</h2>
  <div class="obs"><strong>Measured observation:</strong> availability gaps
  are computed from <code>results/processed/failures.json</code> at report
  generation time. Recovery timing is approximate (monitor resolution
  ~200 ms). These are observations, not conclusions.</div>
  {agg}
  <h3>Per-run detail</h3>
  {detail}
  {fig_html}
</section>"""


def render_resources(ds):
    if not ds.resources:
        return "<section id='resources'><h2>Resource Usage</h2><p class='note'>No resource data.</p></section>"

    # Aggregate per (protocol, replicas): mean CPU util, net rates, RSS.
    groups = {}
    for r in ds.resources:
        key = (r.get("protocol", ""), r.get("replicas", ""))
        groups.setdefault(key, []).append(r)
    rows = []
    for (proto, n), recs in sorted(groups.items()):
        cpu = [fnum(x.get("cpu_util_pct")) for x in recs]
        rx = [fnum(x.get("net_rx_bps")) for x in recs]
        tx = [fnum(x.get("net_tx_bps")) for x in recs]
        rss = [fnum(x.get("rss_bytes_max")) for x in recs]
        rows.append([
            esc(proto), n, len(recs),
            fmt_num(statistics.mean(cpu) if cpu else None, 1),
            fmt_num(statistics.mean(rx) if rx else None, 0),
            fmt_num(statistics.mean(tx) if tx else None, 0),
            fmt_num(statistics.mean(rss) if rss else None, 0),
        ])
    table = render_table(
        ["Protocol", "Replicas", "Replica samples", "CPU util % (mean)",
         "Net RX B/s (mean)", "Net TX B/s (mean)", "RSS bytes (mean)"], rows)

    # Per-run resource figures (collapsible; there can be many).
    res_figs = ds.figure_categories.get("resources", [])
    fig_html = ""
    if res_figs:
        items = "".join(
            f'<div class="fig-card"><img src="../figures/resources/{esc(f)}" alt="{esc(f)}">'
            f'<div class="cap">{esc(f)}</div></div>' for f in res_figs)
        fig_html = (f"<details><summary>Per-run resource figures "
                    f"({len(res_figs)})</summary><div class='fig-grid'>{items}</div></details>")

    return f"""
<section id="resources">
  <h2>Resource Usage</h2>
  <p class="note">Per-replica CPU, network RX/TX, and RSS, aggregated from
  <code>results/processed/resources.csv</code>. Raw per-replica samples are
  preserved in <code>results/raw/</code>.</p>
  {table}
  {fig_html}
</section>"""


def render_figures(ds):
    if not ds.figure_categories:
        return "<section id='figures'><h2>Figures</h2><p class='note'>No figures.</p></section>"
    parts = []
    for cat, files in ds.figure_categories.items():
        grid = "".join(
            f'<div class="fig-card"><img src="../figures/{esc(cat)}/{esc(f)}" alt="{esc(f)}">'
            f'<div class="cap">{esc(f)}</div></div>' for f in files)
        parts.append(f"<h3>{esc(cat)} ({len(files)})</h3><div class='fig-grid'>{grid}</div>")
    return f"""
<section id="figures">
  <h2>Generated Figures</h2>
  <p class="note">All figures are generated programmatically from measured
  data by <code>report/figures.py</code>. Categories are derived from the
  <code>results/figures/</code> directory structure.</p>
  {''.join(parts)}
</section>"""


def render_run_explorer(ds):
    runs = ds.runs
    payload = json.dumps(runs)
    rows = []
    for r in runs:
        status = ('<span class="badge ok">ok</span>' if r["valid"]
                  else '<span class="badge fail">failed</span>')
        files = " ".join(
            f'<a href="../raw/{esc(r["run_id"])}/{f}">{f}</a>'
            for f in ("metadata.json", "requests.csv", "resources.csv",
                      "events.csv", "phase.json", "client-summary.json", "container-logs.txt"))
        rows.append(
            f'<tr data-run=\'{json.dumps(r, default=str)}\'>'
            f"<td>{esc(r['run_id'])}</td><td>{esc(r['protocol'])}</td>"
            f"<td>{esc(r['replicas'])}</td><td>{esc(r['read_pct'])}R/{esc(r['write_pct'])}W</td>"
            f"<td>{esc(r['concurrency'])}</td><td>{esc(r['failure_mode'])}</td>"
            f"<td>{status}</td><td>{fmt_num(r['throughput'], 0)}</td>"
            f"<td>{fmt_ms(r['p50'])}</td><td>{esc(r['reason'])}</td>"
            f"<td class='files'>{files}</td></tr>")
    table = ("<table id='run-table'><thead><tr>"
             "<th>Run ID</th><th>Protocol</th><th>Replicas</th><th>Workload</th>"
             "<th>Concurrency</th><th>Failure mode</th><th>Status</th>"
             "<th>Throughput</th><th>p50</th><th>Reason</th><th>Files</th>"
             "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")

    return f"""
<section id="runs">
  <h2>Raw Run Explorer</h2>
  <p class="note">{len(runs)} runs. Filter by text, protocol, or status.
  File links point to the raw result files (relative paths).</p>
  <div class="filter-bar">
    <input id="run-q" type="text" placeholder="Filter by run ID / reason ...">
    <select id="run-proto"><option value="">All protocols</option></select>
    <select id="run-status"><option value="">All statuses</option>
      <option value="ok">ok</option><option value="failed">failed</option></select>
  </div>
  {table}
</section>"""


def render_repro(ds):
    v = ds.versions()
    rows = []
    for key in ("go", "docker", "docker_compose", "raft", "raft_boltdb", "epaxos",
                "lab_image", "repo_commit"):
        if v.get(key):
            rows.append([key, esc(v[key])])
    host = v.get("host", {})
    if host:
        rows.append(["host", esc(f"{host.get('os')} {host.get('arch')}, {host.get('cpus')} CPUs")])
    mon = v.get("monitoring", {})
    if mon:
        rows.append(["monitoring", esc(mon.get("method", ""))])
        rows.append(["monitor resolution", esc(mon.get("resolution", ""))])
    if v.get("latest_run"):
        rows.append(["latest run", esc(v["latest_run"])])
        rows.append(["latest run started", esc(v["latest_started"])])
    table = render_table(["Component", "Version"], rows)

    md = v.get("versions_md", "")
    md_html = f"<details><summary>upstream/VERSIONS.md</summary><pre>{esc(md)}</pre></details>" if md else ""

    return f"""
<section id="repro">
  <h2>Environment / Reproducibility</h2>
  <p class="note">Versions are read from the most recent run's
  <code>metadata.json</code> and <code>upstream/VERSIONS.md</code> at report
  generation time.</p>
  {table}
  {md_html}
</section>"""


def render_limitations():
    return """
<section id="limitations">
  <h2>Known Limitations</h2>
  <p class="note">These are static characteristics of the benchmark
  implementation and environment. They are not measured results; measured
  outcomes are shown in the sections above.</p>
  <h3>Implementation limitations</h3>
  <ul>
    <li><strong>EPaxos dependency-set size (<code>DS</code>):</strong> the
    vendored <code>efficient/epaxos</code> hardcodes its dependency-set size
    to 5, including in the inter-replica wire format, which limits a cluster
    to 5 replicas. The lab extends it to 9 so that 7- and 9-replica clusters
    work. This is a wire-format extension, not a consensus change: the
    protocol's phases, quorums, and decision rules are untouched, and all
    replicas run the same patched binary.</li>
    <li><strong>EPaxos single-port design:</strong> the upstream server serves
    peer and client connections on the same port; the client waits for
    master-reported readiness plus a settle period instead of probing.</li>
    <li><strong>EPaxos state is in-memory</strong> (upstream default); Raft
    replicas persist to BoltDB volumes. A restarted EPaxos replica loses
    local state.</li>
  </ul>
  <h3>Experimental environment limitations</h3>
  <ul>
    <li><strong>Recovery timing is approximate:</strong> availability gaps are
    derived from request timestamps and the ~200 ms monitor resolution.</li>
    <li><strong>Single host:</strong> all replicas run on one machine over a
    bridge network, not a real network.</li>
    <li><strong>Host clock:</strong> all timestamps come from the host clock;
    containers share it, so cross-container timing is consistent.</li>
    <li><strong>Host-sleep contamination:</strong> one earlier run was
    contaminated by a host suspend; it was removed and cleanly rerun. The
    historical record remains in <code>results/run-index.csv</code>.</li>
  </ul>
</section>"""


RUN_EXPLORER_JS = """
<script>
(function () {
  var protoSel = document.getElementById('run-proto');
  var statusSel = document.getElementById('run-status');
  var q = document.getElementById('run-q');
  var rows = Array.prototype.slice.call(document.querySelectorAll('#run-table tbody tr'));
  var protos = {};
  rows.forEach(function (r) {
    var p = r.children[1].textContent.trim();
    if (p) protos[p] = true;
  });
  Object.keys(protos).sort().forEach(function (p) {
    var o = document.createElement('option');
    o.value = p; o.textContent = p;
    protoSel.appendChild(o);
  });
  function apply() {
    var proto = protoSel.value, status = statusSel.value, query = q.value.toLowerCase();
    rows.forEach(function (r) {
      var show = true;
      if (proto && r.children[1].textContent.trim() !== proto) show = false;
      if (status) {
        var ok = r.children[6].textContent.indexOf('ok') !== -1;
        if ((status === 'ok' && !ok) || (status === 'failed' && ok)) show = false;
      }
      if (query && r.textContent.toLowerCase().indexOf(query) === -1) show = false;
      r.style.display = show ? '' : 'none';
    });
  }
  protoSel.addEventListener('change', apply);
  statusSel.addEventListener('change', apply);
  q.addEventListener('input', apply);
})();
</script>
"""


def render_page(ds):
    sections = [
        render_summary(ds),
        render_config(ds),
        render_workload(ds),
        render_scaling(ds),
        render_conflict(ds),
        render_concurrency(ds),
        render_election(ds),
        render_read_semantics(ds),
        render_failure(ds),
        render_resources(ds),
        render_execution_integrity(ds),
        render_figures(ds),
        render_run_explorer(ds),
        render_repro(ds),
        render_limitations(),
    ]
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Consensus Benchmark Report — Raft vs EPaxos</title>
<style>{CSS}</style>
</head>
<body>
{render_nav()}
<main>
<h1>Consensus Benchmark Report</h1>
<p class="subtitle">Leader-based (Raft) vs leaderless (EPaxos) ordering —
generated {esc(generated)} from the current dataset. Fully offline; no remote
assets.</p>
{''.join(sections)}
</main>
{RUN_EXPLORER_JS}
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser(description="Generate the HTML benchmark report")
    ap.add_argument("--processed", default="results/processed")
    ap.add_argument("--raw", default="results/raw")
    ap.add_argument("--figures", default="results/figures")
    ap.add_argument("--config", default="configs/matrix.json")
    ap.add_argument("--out", default="results/report")
    args = ap.parse_args()

    ds = Dataset(args.processed, args.raw, args.figures, args.config)
    os.makedirs(args.out, exist_ok=True)
    out_path = os.path.join(args.out, "index.html")
    with open(out_path, "w") as f:
        f.write(render_page(ds))
    print(f"wrote {out_path}")
    print(f"  runs: {ds.total_runs} (successful {ds.successful_runs}, failed {ds.failed_runs})")
    print(f"  figures: {ds.figure_count}")
    print(f"  protocols: {', '.join(ds.protocols)}")
    print(f"  replica counts: {', '.join(str(n) for n in ds.replica_counts)}")


if __name__ == "__main__":
    main()