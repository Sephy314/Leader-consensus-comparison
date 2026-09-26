#!/usr/bin/env python3
"""Generate figures from processed metrics.

Every figure is generated programmatically from measured data in
results/processed/. A figure is only produced if the underlying experiment
produced valid data. No values are entered manually.

Two sets of figures are produced:

1. Report figures (PNG): per-mix, per-run, and per-replica detail views used
   by the HTML report.
2. Publication figures (PDF + PNG): the figures used in the paper's
   Evaluation section, generated from the same processed data with a
   consistent academic style.

Data selection: only the runs selected by the documented rule in
process.py (mark_included) are plotted -- one observation per planned
repetition, excluding runs whose host telemetry was contaminated. The rule
lives in process.py; every figure reads its verdict from metrics.csv.

Usage:
    python3 report/figures.py --processed results/processed --figures results/figures [--raw results/raw]
"""
import argparse
import csv
import json
import os
import statistics
import sys
from collections import defaultdict

# report/html.py shadows the stdlib `html` package for any script run from
# this directory (sys.path[0] = report/). matplotlib's pyparsing dependency
# imports html.entities, so drop the script directory from sys.path before
# importing matplotlib to let the stdlib html resolve.
_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path = [p for p in sys.path if os.path.abspath(p) != _script_dir]

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, _script_dir)
import stats as labstats

# --- Publication style ------------------------------------------------------

plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 8.5,
    "font.family": "serif",
    "font.serif": ["DejaVu Serif", "STIXGeneral", "Times New Roman"],
    "axes.grid": True,
    "grid.alpha": 0.35,
    "grid.linewidth": 0.5,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "lines.linewidth": 1.6,
    "lines.markersize": 5.5,
    "legend.frameon": False,
    "legend.fontsize": 8,
    "axes.labelsize": 9,
    "axes.titlesize": 9.5,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
})

COLORS = {"raft": "#1f77b4", "epaxos": "#d62728"}
MARKERS = {"raft": "o", "epaxos": "s"}
PROTO_LABELS = {"raft": "Raft", "epaxos": "EPaxos"}


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_metrics(processed):
    path = os.path.join(processed, "metrics.csv")
    if not os.path.exists(path):
        return []
    return read_csv(path)


def load_resources(processed):
    path = os.path.join(processed, "resources.csv")
    if not os.path.exists(path):
        return []
    return read_csv(path)


def load_failures(processed):
    path = os.path.join(processed, "failures.json")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def save(fig, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {path}")


def save_pub(fig, basepath):
    """Save a publication figure as both PDF and PNG."""
    os.makedirs(os.path.dirname(basepath), exist_ok=True)
    fig.savefig(basepath + ".pdf", bbox_inches="tight")
    fig.savefig(basepath + ".png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {basepath}.pdf, {basepath}.png")


# --- Run selection ----------------------------------------------------------

def select_included(rows):
    """Keep only the runs that passed the selection rule applied by
    process.py (mark_included): one observation per planned repetition,
    excluding runs whose host telemetry was contaminated. The rule itself
    lives in one place; this only reads its verdict."""
    selected = [r for r in rows if str(r.get("included")) == "1"]
    # Fail loudly rather than quietly plotting a contaminated run: a metrics.csv
    # without the column predates the selection rule.
    if rows and not selected and any("included" not in r for r in rows):
        raise SystemExit("metrics.csv has no 'included' column; re-run process.py")
    return selected


# --- Shared plotting helpers ------------------------------------------------

def _series(rows, x_key, val_fn):
    """Group rows by (protocol, x) and return per-run value lists."""
    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        p = r["protocol"]
        x = x_key(r)
        v = val_fn(r)
        if v is not None:
            groups[p][x].append(v)
    return groups


def _plot_series(ax, groups, xlabel, ylabel, logx=False, xticks=None):
    """Median line plus per-repetition points for each protocol.

    The line is the median of the per-repetition values, matching the
    reported headline numbers: a small number of stalled repetitions would
    otherwise pull the mean line below the healthy runs.
    """
    protos = [p for p in ("raft", "epaxos") if p in groups]
    for p in protos:
        xs = sorted(groups[p])
        meds = [statistics.median(groups[p][x]) for x in xs]
        ax.plot(xs, meds, marker=MARKERS[p], color=COLORS[p],
                label=PROTO_LABELS[p], linestyle="-")
        for x, vals in zip(xs, (groups[p][x] for x in xs)):
            ax.scatter([x] * len(vals), vals, color=COLORS[p], s=10,
                       alpha=0.45, zorder=3)
    if logx:
        ax.set_xscale("log", base=2)
    if xticks is not None:
        ax.set_xticks(xticks)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend()


# --- Report figures (HTML report detail views) ------------------------------

def workload_figures(metrics, outdir):
    """Throughput and latency vs concurrency, per protocol and R/W mix."""
    rows = [m for m in metrics if m["experiment"] == "workload"
            and m["failure_mode"] == "none" and m["replicas"] == "3"]
    rows = select_included(rows)
    if not rows:
        return
    mixes = sorted({(m["read_pct"], m["write_pct"]) for m in rows})
    for rp, wp in mixes:
        sub = [m for m in rows if m["read_pct"] == rp]
        protos = sorted({m["protocol"] for m in sub})
        if len(protos) < 2:
            continue
        # Throughput vs concurrency.
        fig, ax = plt.subplots()
        for p in protos:
            pts = sorted(
                ((int(m["concurrency"]), fnum(m["throughput_req_s"])) for m in sub if m["protocol"] == p),
                key=lambda kv: kv[0],
            )
            pts = [(c, t) for c, t in pts if t is not None]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=p)
        ax.set_xlabel("concurrency")
        ax.set_ylabel("throughput (req/s)")
        ax.set_title(f"throughput vs concurrency — {rp}R/{wp}W, 3 replicas")
        ax.legend()
        ax.set_xscale("log", base=2)
        save(fig, os.path.join(outdir, "workload", f"throughput-{rp}r{wp}w.png"))

        # Latency vs concurrency (p50/p95/p99).
        for pct in ("p50", "p95", "p99"):
            fig, ax = plt.subplots()
            for p in protos:
                pts = sorted(
                    (
                        (int(m["concurrency"]), fnum(m[f"latency_ns_{pct}"]))
                        for m in sub
                        if m["protocol"] == p
                    ),
                    key=lambda kv: kv[0],
                )
                pts = [(c, t / 1e6) for c, t in pts if t is not None]
                if pts:
                    ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=p)
            ax.set_xlabel("concurrency")
            ax.set_ylabel(f"latency {pct} (ms)")
            ax.set_title(f"latency {pct} vs concurrency — {rp}R/{wp}W, 3 replicas")
            ax.legend()
            ax.set_xscale("log", base=2)
            save(fig, os.path.join(outdir, "workload", f"latency-{pct}-{rp}r{wp}w.png"))


def scaling_figures(metrics, outdir):
    """Throughput and latency vs replica count (100% write)."""
    rows = [
        m
        for m in metrics
        if m["experiment"] == "scaling" and m["failure_mode"] == "none"
        and m["write_pct"] == "100" and m["concurrency"] == "32"
    ]
    rows = select_included(rows)
    if not rows:
        return
    protos = sorted({m["protocol"] for m in rows})
    for metric, ylabel, fname in (
        ("throughput_req_s", "throughput (req/s)", "throughput"),
        ("latency_ns_p50", "latency p50 (ms)", "latency-p50"),
        ("latency_ns_p95", "latency p95 (ms)", "latency-p95"),
    ):
        fig, ax = plt.subplots()
        for p in protos:
            pts = sorted(
                ((int(m["replicas"]), fnum(m[metric])) for m in rows if m["protocol"] == p),
                key=lambda kv: kv[0],
            )
            pts = [(n, v / 1e6 if "latency" in metric else v) for n, v in pts if v is not None]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=p)
        ax.set_xlabel("replicas")
        ax.set_ylabel(ylabel)
        ax.set_title(f"{fname} vs replica count — 100% write, concurrency 32")
        ax.legend()
        save(fig, os.path.join(outdir, "scaling", f"{fname}.png"))


def commcost_figures(metrics, outdir):
    """Throughput and p50 latency vs one-way communication cost, per
    protocol and implementation. Only the jitter=0 points are plotted so the
    x axis is the cost; the jitter sweep is a separate figure."""
    rows = [m for m in metrics if m["experiment"] == "commcost" and m["failure_mode"] == "none"]
    rows = select_included(rows)
    if not rows:
        return

    def impl_of(m):
        return m.get("implementation") or ("hashicorp" if m["protocol"] == "raft" else "original")

    for metric, ylabel, fname in (
        ("throughput_req_s", "throughput (req/s)", "throughput"),
        ("latency_ns_p50", "latency p50 (ms)", "latency-p50"),
    ):
        fig, ax = plt.subplots()
        for proto in sorted({m["protocol"] for m in rows}):
            for impl in sorted({impl_of(m) for m in rows if m["protocol"] == proto}):
                pts = sorted(
                    ((int(m.get("comm_cost_ms", 0)), fnum(m[metric]))
                     for m in rows if m["protocol"] == proto and impl_of(m) == impl
                     and int(m.get("comm_jitter_pct", 0)) == 0),
                    key=lambda kv: kv[0],
                )
                pts = [(c, v / 1e6 if "latency" in metric else v) for c, v in pts if v is not None]
                if pts:
                    ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o",
                            label=f"{proto}/{impl}")
        ax.set_xlabel("one-way communication cost (ms)")
        ax.set_ylabel(ylabel)
        ax.set_title(f"{fname} vs communication cost — 3 replicas, 100% write, c32")
        ax.legend(fontsize=8)
        save(fig, os.path.join(outdir, "commcost", f"{fname}.png"))

    # Jitter sweep at cost=5: throughput vs jitter percentage.
    fig, ax = plt.subplots()
    for proto in sorted({m["protocol"] for m in rows}):
        for impl in sorted({impl_of(m) for m in rows if m["protocol"] == proto}):
            pts = sorted(
                ((int(m.get("comm_jitter_pct", 0)), fnum(m["throughput_req_s"]))
                 for m in rows if m["protocol"] == proto and impl_of(m) == impl
                 and int(m.get("comm_cost_ms", 0)) == 5),
                key=lambda kv: kv[0],
            )
            pts = [(j, v) for j, v in pts if v is not None]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o",
                        label=f"{proto}/{impl}")
    ax.set_xlabel("jitter (% of 5 ms cost)")
    ax.set_ylabel("throughput (req/s)")
    ax.set_title("throughput vs jitter — 5 ms cost, 3 replicas, 100% write, c32")
    ax.legend(fontsize=8)
    save(fig, os.path.join(outdir, "commcost", "throughput-jitter.png"))


# --- Families added after the recorded suite --------------------------------
#
# Persistence matching, the clean OS-level network delay and the conflict
# validation each have their own experiment family and their own results
# subtree. Every generator below is a no-op when its family is absent from the
# processed data, so running the pipeline over a dataset that does not contain
# them (for example the recorded suite) is unaffected.

IMPL_COLORS = {"hashicorp": COLORS["raft"], "etcd": "tab:orange",
               "original": COLORS["epaxos"], "nvb": "tab:purple"}
IMPL_LABELS = {"hashicorp": "Raft (HashiCorp)", "etcd": "Raft (etcd)",
               "original": "EPaxos (original)", "nvb": "EPaxos (nvb)"}
IMPL_ORDER = ("hashicorp", "etcd", "original", "nvb")
MODES = ("durable", "memory")
MODE_COLORS = {"durable": "tab:blue", "memory": "tab:green"}


def _impl(m):
    """The implementation of a metrics row. Rows recorded before the
    implementation field existed are the primary implementation of their
    protocol."""
    return m.get("implementation") or ("hashicorp" if m["protocol"] == "raft" else "original")


def _median_range(rows, metric, scale=1.0):
    """(median, lo_err, hi_err, n) over rows, or None when no value exists.

    The reported value is the median over the repetitions and the error bar is
    the observed min-max range: with ten repetitions and a few stalled ones,
    the range is what shows whether two conditions actually separate.
    """
    vals = [v / scale for v in (fnum(m.get(metric)) for m in rows) if v is not None]
    if not vals:
        return None
    med = statistics.median(vals)
    return med, med - min(vals), max(vals) - med, len(vals)


def _bars(ax, groups, ylabel, series_order, colors=None, xticklabels=True):
    """Grouped bar panel. groups maps (group_key, series_key) -> _median_range
    tuple; both keys are shown in their sorted order. `xticklabels=False`
    suppresses the labels when the neighbouring panel already carries the same
    ones."""
    keys = sorted({g for g, _ in groups})
    series = [s for s in series_order if any(s == sk for _, sk in groups)]
    width = 0.8 / max(1, len(series))
    for i, s in enumerate(series):
        xs, meds, los, his = [], [], [], []
        for k, key in enumerate(keys):
            got = groups.get((key, s))
            if not got:
                continue
            med, lo, hi, _ = got
            xs.append(k + (i - (len(series) - 1) / 2) * width)
            meds.append(med)
            los.append(lo)
            his.append(hi)
        if xs:
            # matplotlib wants asymmetric errors as a (2, n) array.
            yerr = [los, his] if any(los) or any(his) else None
            ax.bar(xs, meds, width, label=s, color=(colors or {}).get(s, "tab:gray"),
                   yerr=yerr, capsize=2, error_kw={"lw": 0.6, "alpha": 0.7})
    ax.set_xticks(range(len(keys)))
    if xticklabels:
        ax.set_xticklabels(keys, fontsize=7, rotation=20, ha="right")
    else:
        ax.set_xticklabels([])
    ax.set_ylabel(ylabel)
    ax.legend(fontsize=7)


def _lines(ax, series, xlabel, ylabel, xticks=None):
    """Line panel. series maps label -> (color, [(x, y)]) with y already
    scaled."""
    for label in sorted(series):
        color, pts = series[label]
        pts = sorted((x, y) for x, y in pts if y is not None)
        if pts:
            ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o",
                    color=color, label=label)
    if xticks is not None:
        ax.set_xticks(xticks)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(fontsize=7)


def _persistence_panels(metrics, figsize):
    """Throughput and p50 latency by persistence mode, one panel per replica
    count. The mode is the family's independent variable, so it is the series
    rather than a second x axis."""
    rows = select_included([m for m in metrics if m["experiment"] == "persistence"])
    if not rows:
        return []
    replicas = sorted({int(m["replicas"]) for m in rows})
    out = []
    for metric, scale, ylabel, name in (
        ("throughput_req_s", 1.0, "Throughput (req/s)", "throughput-persistence"),
        ("latency_ns_p50", 1e6, "Median latency (ms)", "latency-persistence"),
    ):
        fig, axes = plt.subplots(1, len(replicas), figsize=figsize, squeeze=False)
        for ax, r in zip(axes[0], replicas):
            groups = {}
            for impl in IMPL_ORDER:
                for mode in MODES:
                    got = _median_range(
                        [m for m in rows if int(m["replicas"]) == r and _impl(m) == impl
                         and m.get("persistence_mode") == mode], metric, scale)
                    if got:
                        groups[(IMPL_LABELS[impl], mode)] = got
            _bars(ax, groups, ylabel if r == replicas[0] else "", MODES, MODE_COLORS,
                  xticklabels=(r == replicas[0]))
            ax.set_title(f"{r} replicas", fontsize=8.5)
        out.append((name, fig))
    return out


def _persistence_matched_panels(metrics, figsize):
    """The matched comparison: within one persistence mode, Raft against
    EPaxos, one panel per mode, with the ratio printed so the direction of the
    difference is readable without dividing numbers by eye."""
    rows = select_included([m for m in metrics if m["experiment"] == "persistence"])
    if not rows:
        return []
    pairs = [("A: hashicorp / original", "hashicorp", "original"),
             ("B: etcd / nvb", "etcd", "nvb")]
    replicas = sorted({int(m["replicas"]) for m in rows})
    fig, axes = plt.subplots(1, len(replicas), figsize=figsize, squeeze=False)
    for ax, r in zip(axes[0], replicas):
        groups = {}
        for label, r_impl, e_impl in pairs:
            for mode in MODES:
                got = _median_range(
                    [m for m in rows if int(m["replicas"]) == r and _impl(m) == r_impl
                     and m.get("persistence_mode") == mode], "throughput_req_s")
                if got:
                    groups[(label + f"  [{mode}]", "Raft")] = got
                got = _median_range(
                    [m for m in rows if int(m["replicas"]) == r and _impl(m) == e_impl
                     and m.get("persistence_mode") == mode], "throughput_req_s")
                if got:
                    groups[(label + f"  [{mode}]", "EPaxos")] = got
        _bars(ax, groups, "Throughput (req/s)" if r == replicas[0] else "",
              ("Raft", "EPaxos"), {"Raft": COLORS["raft"], "EPaxos": COLORS["epaxos"]},
              xticklabels=(r == replicas[0]))
        ax.set_title(f"{r} replicas", fontsize=8.5)
    return [("throughput-persistence-matched", fig)]


def _networkdelay_panels(metrics, figsize):
    """Throughput and p50 latency against the emulated one-way delay, per
    implementation, plus the fraction of the un-delayed throughput each
    implementation retains. The retained fraction is the comparison that does
    not depend on the implementations' different baselines."""
    rows = select_included([m for m in metrics if m["experiment"] == "networkdelay"])
    if not rows:
        return []
    delays = sorted({int(m["network_delay_ms"]) for m in rows})
    out = []
    for metric, scale, ylabel, name in (
        ("throughput_req_s", 1.0, "Throughput (req/s)", "throughput-networkdelay"),
        ("latency_ns_p50", 1e6, "Median latency (ms)", "latency-networkdelay"),
    ):
        fig, ax = plt.subplots(figsize=figsize)
        series = {}
        for impl in IMPL_ORDER:
            pts = [(d, _median_range([m for m in rows if _impl(m) == impl
                                      and int(m["network_delay_ms"]) == d], metric, scale))
                   for d in delays]
            series[IMPL_LABELS[impl]] = (IMPL_COLORS[impl],
                                         [(d, g[0] if g else None) for d, g in pts])
        _lines(ax, series, "One-way emulated delay (ms)", ylabel, xticks=delays)
        out.append((name, fig))

    # Retained fraction of each implementation's own 0 ms point.
    fig, ax = plt.subplots(figsize=figsize)
    series = {}
    for impl in IMPL_ORDER:
        base = _median_range([m for m in rows if _impl(m) == impl
                              and int(m["network_delay_ms"]) == 0], "throughput_req_s")
        if not base:
            continue
        pts = []
        for d in delays:
            got = _median_range([m for m in rows if _impl(m) == impl
                                 and int(m["network_delay_ms"]) == d], "throughput_req_s")
            pts.append((d, 100.0 * got[0] / base[0] if got and base[0] else None))
        series[IMPL_LABELS[impl]] = (IMPL_COLORS[impl], pts)
    _lines(ax, series, "One-way emulated delay (ms)",
           "Throughput retained (% of 0 ms)", xticks=delays)
    out.append(("retained-networkdelay", fig))
    return out


def _conflictvalidation_panels(metrics, figsize):
    """Did the workload do what it says, and did that create contention?

    Panel 1 checks the workload itself: the realized hot-key fraction against
    the configured one, which the per-request hot flag makes exact. Panel 2
    reports the fraction of EPaxos commands that took the slow path, the
    protocol-level contention the configured fraction actually produced.
    """
    rows = select_included([m for m in metrics if m["experiment"] == "conflictvalidation"])
    if not rows:
        return []
    rows = [m for m in rows if fnum(m.get("realized_hot_fraction")) is not None]
    if not rows:
        return []
    hot_keys = sorted({int(m.get("hot_keys") or 1) for m in rows})
    configured = sorted({int(m["conflict_pct"]) for m in rows})

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize)

    series = {}
    for hk in hot_keys:
        pts = []
        for pct in configured:
            got = _median_range([m for m in rows if int(m["conflict_pct"]) == pct
                                 and int(m.get("hot_keys") or 1) == hk],
                                "realized_hot_fraction")
            pts.append((pct, 100.0 * got[0] if got else None))
        series[f"{hk} hot key" + ("s" if hk > 1 else "")] = ("tab:blue", pts)
    _lines(ax1, series, "Configured conflict fraction (%)",
           "Realized hot-key fraction (%)", xticks=configured)
    ax1.plot(configured, configured, linestyle="--", linewidth=1,
             color="0.4", label="configured = realized")
    ax1.legend(fontsize=7)
    ax1.set_title("(a) the workload delivered what it configured", fontsize=8.5)

    # Only runs whose implementation exposes the counters contribute; the
    # availability flag is set per run, so an implementation without them is
    # absent rather than plotted as zero.
    counted = [m for m in rows if str(m.get("epaxos_counters_available")).lower() == "true"]
    series = {}
    for hk in hot_keys:
        pts = []
        for pct in configured:
            sel = [m for m in counted if int(m["conflict_pct"]) == pct
                   and int(m.get("hot_keys") or 1) == hk]
            shares = []
            for m in sel:
                fast, slow = fnum(m.get("epaxos_fast_path")), fnum(m.get("epaxos_slow_path"))
                if fast is not None and slow is not None and (fast + slow) > 0:
                    shares.append(100.0 * slow / (fast + slow))
            pts.append((pct, statistics.median(shares) if shares else None))
        series[f"{hk} hot key" + ("s" if hk > 1 else "")] = ("tab:red", pts)
    if counted:
        _lines(ax2, series, "Configured conflict fraction (%)",
               "Slow-path share (%)", xticks=configured)
    # The upstream counters are reset on every accepted client connection, so
    # they cover only the interval since the last one. The coverage is measured
    # per run and stated here: a counter-derived share is a within-window
    # indicator, never a census of the measured phase.
    coverages = [c for c in (fnum(m.get("epaxos_counters_coverage")) for m in counted) if c]
    if coverages:
        ax2.set_title(f"(b) EPaxos path mix — counters cover {100 * statistics.median(coverages):.0f}% of requests",
                      fontsize=8.5)
    else:
        ax2.set_title("(b) EPaxos path mix — counter coverage unknown", fontsize=8.5)
    fig.tight_layout()
    return [("conflict-validation", fig)]


def paper_persistence_figures(metrics, outdir):
    """Throughput and latency by persistence mode, plus the matched
    Raft-against-EPaxos comparison within each mode."""
    panels = _persistence_panels(metrics, (6.8, 2.5)) + _persistence_matched_panels(metrics, (6.8, 2.5))
    for name, fig in panels:
        save_pub(fig, os.path.join(outdir, "persistence", name))


def paper_networkdelay_figures(metrics, outdir):
    """Throughput and latency against the emulated delay, plus the fraction
    of the un-delayed throughput each implementation retains."""
    for name, fig in _networkdelay_panels(metrics, (3.4, 2.5)):
        save_pub(fig, os.path.join(outdir, "networkdelay", name))


def paper_conflictvalidation_figures(metrics, outdir):
    """The realized hot-key fraction against the configured one, and the
    EPaxos slow-path share it produced."""
    for name, fig in _conflictvalidation_panels(metrics, (6.8, 2.5)):
        save_pub(fig, os.path.join(outdir, "conflictvalidation", name))


def resource_figures(resources, outdir):
    """Per-replica CPU and network distribution."""
    if not resources:
        return
    # Group by (protocol, replicas, run_id) — use the first run per config.
    seen = set()
    for r in resources:
        key = (r["protocol"], r["replicas"], r["run_id"])
        if key in seen:
            continue
        seen.add(key)
        run_res = [x for x in resources if x["run_id"] == r["run_id"]]
        run_res.sort(key=lambda x: int(x["replica_id"]))
        ids = [int(x["replica_id"]) for x in run_res]
        cpu = [fnum(x["cpu_util_pct"]) or 0 for x in run_res]
        rx = [fnum(x["net_rx_bps"]) or 0 for x in run_res]
        tx = [fnum(x["net_tx_bps"]) or 0 for x in run_res]

        fig, axes = plt.subplots(1, 3, figsize=(12, 3.5))
        axes[0].bar(ids, cpu)
        axes[0].set_title("CPU util %")
        axes[0].set_xlabel("replica")
        axes[1].bar(ids, [v / 1e3 for v in rx])
        axes[1].set_title("net RX (kB/s)")
        axes[1].set_xlabel("replica")
        axes[2].bar(ids, [v / 1e3 for v in tx])
        axes[2].set_title("net TX (kB/s)")
        axes[2].set_xlabel("replica")
        fig.suptitle(f"per-replica resources — {r['protocol']} r{r['replicas']} ({r['run_id']})")
        save(fig, os.path.join(outdir, "resources", f"{r['run_id']}.png"))


def failure_figures(failures, processed, outdir):
    """Failure timeline: request success/failure, throughput, latency over time."""
    if not failures:
        return
    for f in failures:
        run_id = f["run_id"]
        # Reconstruct per-second aggregates from raw requests.
        req_path = os.path.join(os.path.dirname(processed), "raw", run_id, "requests.csv")
        if not os.path.exists(req_path):
            continue
        rows = read_csv(req_path)
        if not rows:
            continue
        t0 = None
        phase_path = os.path.join(os.path.dirname(processed), "raw", run_id, "phase.json")
        if os.path.exists(phase_path):
            with open(phase_path) as fh:
                t0 = json.load(fh).get("phase_started_ns")
        if t0 is None:
            continue

        buckets = {}
        for r in rows:
            sec = int((int(r["end_ns"]) - t0) / 1e9)
            b = buckets.setdefault(sec, {"ok": 0, "fail": 0, "lats": []})
            if r["ok"] == "1":
                b["ok"] += 1
                b["lats"].append(int(r["latency_ns"]))
            else:
                b["fail"] += 1
        secs = sorted(buckets)
        ok = [buckets[s]["ok"] for s in secs]
        fail = [buckets[s]["fail"] for s in secs]
        total = [ok[i] + fail[i] for i in range(len(secs))]
        p95 = [
            sorted(buckets[s]["lats"])[int(0.95 * (len(buckets[s]["lats"]) - 1))] / 1e6
            if buckets[s]["lats"]
            else 0
            for s in secs
        ]

        fig, axes = plt.subplots(3, 1, figsize=(9, 9), sharex=True)
        axes[0].bar(secs, ok, label="ok", color="tab:green")
        axes[0].bar(secs, fail, bottom=ok, label="failed", color="tab:red")
        axes[0].set_ylabel("requests/s")
        axes[0].legend()
        axes[0].set_title(f"failure timeline — {run_id}")
        axes[1].plot(secs, total, marker=".", label="throughput")
        axes[1].set_ylabel("throughput (req/s)")
        axes[1].legend()
        axes[2].plot(secs, p95, marker=".", label="p95 latency")
        axes[2].set_ylabel("p95 latency (ms)")
        axes[2].set_xlabel("seconds since phase start")
        axes[2].legend()

        # Mark failure injection and recovery events.
        tl = f.get("timeline", {})
        for ev, color in (("failure_injected", "red"), ("restart_confirmed", "blue")):
            if ev in tl and tl[ev].get("rel_s") is not None:
                for ax in axes:
                    ax.axvline(tl[ev]["rel_s"], color=color, linestyle="--", alpha=0.7)
        save(fig, os.path.join(outdir, "failure", f"{run_id}.png"))


# --- Publication figures (paper Evaluation section) -------------------------

def paper_scaling_figures(metrics, outdir):
    """Throughput and p50 latency vs replica count (3, 5, 7, 9).

    Every point comes from the scaling family, which covers 3-9 replicas at
    100% writes and concurrency 32. (Earlier revisions of the matrix had no
    3-replica scaling configuration and borrowed that point from the
    concurrency family; mixing two experiments at one x value would now
    average them, so the family is the only source.)
    """
    rows = [m for m in metrics
            if m["experiment"] == "scaling" and m["failure_mode"] == "none"]
    rows = select_included(rows)
    if not rows:
        return

    def xkey(m):
        return int(m["replicas"])

    groups = _series(rows, xkey, lambda m: fnum(m["throughput_req_s"]))
    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    _plot_series(ax, groups, "Replicas", "Throughput (req/s)", xticks=[3, 5, 7, 9])
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "scaling", "throughput-scaling"))

    groups = _series(rows, xkey, lambda m: fnum(m["latency_ns_p50"]) / 1e6)
    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    _plot_series(ax, groups, "Replicas", "Median latency (ms)", xticks=[3, 5, 7, 9])
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "scaling", "latency-replica-scaling"))


def paper_concurrency_figures(metrics, outdir):
    """Throughput and p50 latency vs client concurrency (1-256)."""
    rows = [m for m in metrics
            if m["experiment"] == "concurrency" and m["failure_mode"] == "none"]
    rows = select_included(rows)
    if not rows:
        return

    def xkey(m):
        return int(m["concurrency"])

    levels = sorted({xkey(m) for m in rows})
    groups = _series(rows, xkey, lambda m: fnum(m["throughput_req_s"]))
    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    _plot_series(ax, groups, "Client concurrency", "Throughput (req/s)",
                 logx=True, xticks=levels)
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "workload", "throughput-concurrency"))

    groups = _series(rows, xkey, lambda m: fnum(m["latency_ns_p50"]) / 1e6)
    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    _plot_series(ax, groups, "Client concurrency", "Median latency (ms)",
                 logx=True, xticks=levels)
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "workload", "latency-concurrency"))


def paper_workload_figures(metrics, outdir):
    """Throughput vs write ratio at concurrency 32."""
    rows = [m for m in metrics
            if m["experiment"] == "workload"
            and m["concurrency"] == "32" and m["failure_mode"] == "none"]
    rows = select_included(rows)
    if not rows:
        return

    def xkey(m):
        return int(m["write_pct"])

    groups = _series(rows, xkey, lambda m: fnum(m["throughput_req_s"]))
    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    _plot_series(ax, groups, "Write ratio (%)", "Throughput (req/s)")
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "workload", "throughput-write-ratio"))


def paper_conflict_figures(metrics, outdir):
    """Throughput vs conflict ratio at 3 replicas, 100% writes, c32."""
    rows = [m for m in metrics
            if m["experiment"] == "conflict" and m["failure_mode"] == "none"]
    rows = select_included(rows)
    if not rows:
        return

    def xkey(m):
        return int(m["conflict_pct"])

    groups = _series(rows, xkey, lambda m: fnum(m["throughput_req_s"]))
    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    _plot_series(ax, groups, "Conflict ratio (%)", "Throughput (req/s)")
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "conflict", "throughput-conflict-ratio"))


def paper_commcost_figures(metrics, outdir):
    """Throughput and p50 latency vs one-way communication cost, per
    implementation (the commcost matrix runs all four). The jitter=0 points
    are plotted so the x axis is the cost; the jitter sweep is a separate
    figure at the 5 ms cost level."""
    rows = [m for m in metrics
            if m["experiment"] == "commcost" and m["failure_mode"] == "none"]
    rows = select_included(rows)
    if not rows:
        return

    def impl_of(m):
        return m.get("implementation") or ("hashicorp" if m["protocol"] == "raft" else "original")

    impls = sorted({impl_of(m) for m in rows})
    colors = {"hashicorp": COLORS["raft"], "etcd": "tab:orange",
              "original": COLORS["epaxos"], "nvb": "tab:purple"}
    labels = {"hashicorp": "Raft (HashiCorp)", "etcd": "Raft (etcd)",
              "original": "EPaxos (original)", "nvb": "EPaxos (nvb)"}

    # Cost sweep (jitter = 0).
    for metric, ylabel, fname in (
        ("throughput_req_s", "Throughput (req/s)", "throughput-commcost"),
        ("latency_ns_p50", "Median latency (ms)", "latency-commcost"),
    ):
        fig, ax = plt.subplots(figsize=(3.4, 2.5))
        for impl in impls:
            pts = sorted(
                ((int(m.get("comm_cost_ms", 0)), fnum(m[metric]))
                 for m in rows if impl_of(m) == impl
                 and int(m.get("comm_jitter_pct", 0)) == 0),
                key=lambda kv: kv[0],
            )
            pts = [(c, v / 1e6 if "latency" in metric else v) for c, v in pts if v is not None]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o",
                        color=colors.get(impl, "tab:gray"), label=labels.get(impl, impl))
        ax.set_xlabel("One-way communication cost (ms)")
        ax.set_ylabel(ylabel)
        ax.set_xticks([0, 1, 3, 5, 10])
        ax.legend(fontsize=7)
        fig.tight_layout()
        save_pub(fig, os.path.join(outdir, "commcost", fname))

    # Jitter sweep at cost = 5 ms.
    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    for impl in impls:
        pts = sorted(
            ((int(m.get("comm_jitter_pct", 0)), fnum(m["throughput_req_s"]))
             for m in rows if impl_of(m) == impl
             and int(m.get("comm_cost_ms", 0)) == 5),
            key=lambda kv: kv[0],
        )
        pts = [(j, v) for j, v in pts if v is not None]
        if pts:
            ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o",
                    color=colors.get(impl, "tab:gray"), label=labels.get(impl, impl))
    ax.set_xlabel("Jitter (% of 5 ms cost)")
    ax.set_ylabel("Throughput (req/s)")
    ax.set_xticks([0, 5, 10, 50, 100])
    ax.legend(fontsize=7)
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "commcost", "throughput-commcost-jitter"))


def paper_resource_figures(resources, outdir):
    """Mean CPU, peak RSS, and mean network rates per protocol."""
    if not resources:
        return
    by_run = defaultdict(list)
    for r in resources:
        by_run[r["run_id"]].append(r)
    agg = defaultdict(lambda: {"cpu": [], "rss": [], "rx": [], "tx": []})
    for rid, recs in by_run.items():
        proto = recs[0]["protocol"]
        agg[proto]["cpu"].append(statistics.mean(fnum(x["cpu_util_pct"]) for x in recs))
        agg[proto]["rss"].append(max(fnum(x["rss_bytes_max"]) for x in recs))
        agg[proto]["rx"].append(statistics.mean(fnum(x["net_rx_bps"]) for x in recs))
        agg[proto]["tx"].append(statistics.mean(fnum(x["net_tx_bps"]) for x in recs))

    protos = [p for p in ("raft", "epaxos") if p in agg]
    labels = [PROTO_LABELS[p] for p in protos]
    colors = [COLORS[p] for p in protos]
    cpu = [statistics.mean(agg[p]["cpu"]) for p in protos]
    rss = [statistics.mean(agg[p]["rss"]) / 1e6 for p in protos]
    rx = [statistics.mean(agg[p]["rx"]) / 1e3 for p in protos]
    tx = [statistics.mean(agg[p]["tx"]) / 1e3 for p in protos]

    fig, axes = plt.subplots(1, 3, figsize=(6.9, 2.3))
    x = np.arange(len(protos))

    axes[0].bar(x, cpu, color=colors, width=0.55)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(labels)
    axes[0].set_ylabel("CPU utilisation (%)")
    axes[0].set_title("Mean per-replica CPU")

    axes[1].bar(x, rss, color=colors, width=0.55)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels)
    axes[1].set_ylabel("RSS (MB)")
    axes[1].set_title("Peak per-replica RSS")

    width = 0.35
    axes[2].bar(x - width / 2, rx, width, color=colors, alpha=0.45, label="RX")
    axes[2].bar(x + width / 2, tx, width, color=colors, label="TX")
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(labels)
    axes[2].set_ylabel("Network rate (kB/s)")
    axes[2].set_title("Mean per-replica network")
    axes[2].legend()

    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "resources", "resource-utilisation"))


def paper_per_replica_cpu(resources, outdir):
    """Per-replica CPU in the 9-replica scaling runs."""
    rows = [r for r in resources
            if r["replicas"] == "9" and r["experiment"] == "scaling"]
    if not rows:
        return
    data = defaultdict(lambda: defaultdict(list))
    for r in rows:
        v = fnum(r["cpu_util_pct"])
        if v is not None:
            data[r["protocol"]][int(r["replica_id"])].append(v)
    protos = [p for p in ("raft", "epaxos") if p in data]
    ids = sorted(data[protos[0]])
    x = np.arange(len(ids))
    width = 0.38

    fig, ax = plt.subplots(figsize=(3.4, 2.5))
    for i, p in enumerate(protos):
        means = [statistics.mean(data[p][rid]) for rid in ids]
        off = (i - 0.5) * width
        ax.bar(x + off, means, width, color=COLORS[p], alpha=0.85,
               label=PROTO_LABELS[p])
        for j, rid in enumerate(ids):
            vals = data[p][rid]
            ax.scatter([x[j] + off] * len(vals), vals, color=COLORS[p],
                       s=9, alpha=0.6, zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(ids)
    ax.set_xlabel("Replica index")
    ax.set_ylabel("CPU utilisation (%)")
    ax.legend()
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "resources", "per-replica-cpu"))


def paper_failure_figures(failures, metrics, outdir):
    """Availability gap and failed requests by failure type."""
    if not failures:
        return

    gaps = defaultdict(list)
    for f in failures:
        if f.get("failure_mode") in ("", "election") or str(f.get("included")) != "1":
            continue
        gaps[f["failure_mode"]].append(f["write_availability_gap_s_approx"])

    failed = defaultdict(list)
    for m in metrics:
        if m["failure_mode"] in ("none", "election"):
            continue
        failed[m["failure_mode"]].append(int(m["requests_failed"]))

    types = ["leader", "follower", "replica"]
    labels = ["Raft\nleader kill", "Raft\nfollower kill", "EPaxos\nreplica kill"]
    colors = [COLORS["raft"], COLORS["raft"], COLORS["epaxos"]]

    fig, axes = plt.subplots(1, 2, figsize=(6.9, 2.3))
    for ax, key, ylabel in ((axes[0], "gap", "Availability gap (s)"),
                            (axes[1], "failed", "Failed requests")):
        for i, t in enumerate(types):
            vals = gaps[t] if key == "gap" else failed[t]
            if not vals:
                continue
            ax.scatter([i] * len(vals), vals, color=colors[i], s=22, zorder=3)
            ax.plot([i], [statistics.mean(vals)], marker="_", color="black",
                    ms=10, mew=1.5)
        ax.set_xticks(range(len(types)))
        ax.set_xticklabels(labels)
        ax.set_ylabel(ylabel)
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "failure", "failure-recovery"))


def paper_election_figures(failures, raw, outdir):
    """Recovery vs the MEASURED number of failed elections.

    The independent variable is the actual number of failed elections
    (sum of dropped RequestVote across replicas, from stats.json), never the
    configured target. Two panels:

    (a) Availability gap vs measured elections. The gap includes the
        isolation duration, which is proportional to the target
        (isoMS = target x election_timeout), so a positive correlation here
        is partly mechanical: each failed election consumes one election
        timeout by construction.
    (b) Recovery-from-isolation-end vs measured elections: time from the end
        of the isolation window to the first successful request. This is
        decoupled from the injection duration and measures only the
        recovery behaviour after the injection has expired.

    Both panels show every observation, the per-value means, and the Pearson
    correlation with its 95% CI and p-value.
    """
    if not failures:
        return
    xs, ys_gap, ys_rec = [], [], []
    for f in failures:
        if f.get("failure_mode") != "election":
            continue
        if str(f.get("included")) != "1":
            continue  # same selection rule as the metrics aggregates
        me = f.get("measured_elections")
        gap = f.get("write_availability_gap_s_approx")
        rec = f.get("recovery_from_isolation_end_s")
        if me is None or gap is None:
            continue
        xs.append(me)
        ys_gap.append(gap)
        ys_rec.append(rec)

    fig, axes = plt.subplots(1, 2, figsize=(6.9, 2.5))
    for ax, ys, ylabel in ((axes[0], ys_gap, "Availability gap (s)"),
                           (axes[1], ys_rec, "Recovery from isolation end (s)")):
        pts = [(x, y) for x, y in zip(xs, ys) if y is not None]
        if pts:
            ax.scatter([p[0] for p in pts], [p[1] for p in pts],
                       color=COLORS["raft"], s=22, zorder=3, label="Raft")
        ts = sorted(set(xs))
        means = [statistics.mean([g for xx, g in zip(xs, ys)
                                  if xx == t and g is not None]) for t in ts]
        ax.plot(ts, means, color=COLORS["raft"], linestyle="--", linewidth=1.2,
                label="Mean")
        ax.set_xlabel("Measured failed elections")
        ax.set_ylabel(ylabel)
        ax.set_xticks(ts)
        ax.legend()
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "election", "election-failure"))

    # Correlation statistics (recorded alongside the figure).
    r_gap = labstats.pearson(xs, ys_gap)
    p_gap = labstats.pearson_p(r_gap, len(xs)) if r_gap is not None else None
    r_rec = labstats.pearson(xs, ys_rec)
    p_rec = labstats.pearson_p(r_rec, len(xs)) if r_rec is not None else None
    stats_out = {
        "n": len(xs),
        "gap_vs_measured_elections": {
            "pearson_r": r_gap,
            "p_value": p_gap,
            "note": "availability gap includes the isolation duration "
                    "(proportional to the target), so a positive correlation "
                    "is partly mechanical",
        },
        "recovery_from_isolation_end_vs_measured_elections": {
            "pearson_r": r_rec,
            "p_value": p_rec,
            "note": "decoupled from the injection duration; measures only "
                    "recovery behaviour after the isolation has expired",
        },
        "note": "correlation does not imply causation; the observations are "
                "shown in the figure",
    }
    os.makedirs(os.path.join(outdir, "election"), exist_ok=True)
    with open(os.path.join(outdir, "election", "election-correlation.json"), "w") as fh:
        json.dump(stats_out, fh, indent=2)
    print(f"  election correlation: n={stats_out['n']} "
          f"gap r={r_gap} p={p_gap}; recovery r={r_rec} p={p_rec}")


def execution_timeline_figure(manifest_path, outdir):
    """Timeline of experimental conditions over wall-clock time, from the
    execution manifest. Makes condition/time confounding immediately
    visible: if a condition is grouped at one end of the timeline, the
    randomization failed."""
    if not os.path.exists(manifest_path):
        return
    with open(manifest_path) as f:
        manifest = json.load(f)
    runs = manifest.get("runs", [])
    if not runs:
        return
    runs = sorted(runs, key=lambda r: r.get("started_at", ""))
    t0 = None
    for r in runs:
        try:
            from datetime import datetime
            t = datetime.fromisoformat(r["started_at"].replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue
        if t0 is None or t < t0:
            t0 = t
    if t0 is None:
        return

    conditions = sorted({r.get("condition", "?") for r in runs})
    cond_idx = {c: i for i, c in enumerate(conditions)}

    fig, ax = plt.subplots(figsize=(6.9, 3.2))
    for r in runs:
        try:
            from datetime import datetime
            t = datetime.fromisoformat(r["started_at"].replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue
        x = (t - t0).total_seconds() / 3600.0
        y = cond_idx.get(r.get("condition", "?"), 0)
        color = COLORS.get(r.get("condition", "").split("/")[0], "#888888")
        ax.scatter(x, y, s=8, color=color, alpha=0.7, zorder=3)
    ax.set_yticks(range(len(conditions)))
    ax.set_yticklabels(conditions, fontsize=6)
    ax.set_xlabel("Hours since first run")
    ax.set_ylabel("Condition")
    ax.set_title("Execution timeline (conditions over wall-clock time)")
    fig.tight_layout()
    save_pub(fig, os.path.join(outdir, "execution", "timeline"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="results/processed")
    ap.add_argument("--figures", default="results/figures")
    ap.add_argument("--raw", default=None,
                    help="results/raw (for metadata; default: sibling of --processed)")
    ap.add_argument("--manifest", default=None,
                    help="results/execution-manifest.json (for the execution timeline)")
    args = ap.parse_args()

    raw = args.raw or os.path.join(os.path.dirname(os.path.abspath(args.processed)), "raw")
    manifest = args.manifest or os.path.join(os.path.dirname(os.path.abspath(args.processed)), "execution-manifest.json")

    metrics = load_metrics(args.processed)
    resources = load_resources(args.processed)
    failures = load_failures(args.processed)

    print("generating report figures from processed data ...")
    workload_figures(metrics, args.figures)
    scaling_figures(metrics, args.figures)
    commcost_figures(metrics, args.figures)
    resource_figures(resources, args.figures)
    failure_figures(failures, args.processed, args.figures)

    # The three families added after the recorded suite have no HTML report
    # section, so they are generated once, in the publication style.
    paper_persistence_figures(metrics, args.figures)
    paper_networkdelay_figures(metrics, args.figures)
    paper_conflictvalidation_figures(metrics, args.figures)

    print("generating publication figures ...")
    paper_scaling_figures(metrics, args.figures)
    paper_concurrency_figures(metrics, args.figures)
    paper_workload_figures(metrics, args.figures)
    paper_conflict_figures(metrics, args.figures)
    paper_commcost_figures(metrics, args.figures)
    paper_resource_figures(resources, args.figures)
    paper_per_replica_cpu(resources, args.figures)
    paper_failure_figures(failures, metrics, args.figures)
    paper_election_figures(failures, raw, args.figures)
    execution_timeline_figure(manifest, args.figures)
    print("done")


if __name__ == "__main__":
    main()