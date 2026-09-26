# The Effects of Leadership in Distributed Consensus

> An Experimental Comparison of Leader-Based and Leaderless Consensus

Research repository.

<https://github.com/Sephy314/Leader-consensus-comparison>

> Status: the experiment matrix has been run and processed, the correctness
> harness passes for all four implementations, and the paper is written from
> abstract to conclusion. The released PDF lives in `paper/release/` (with a
> Korean translation alongside); `paper/` rebuilds it from source.
>
> Three further families were added after that dataset and write to their own
> result subtrees without touching it: **persistence matching** (vary only how
> each implementation persists consensus state), **clean network delay**
> (applied at the OS level with `tc/netem`, outside the implementations) and
> **conflict validation** (does the configured conflict fraction actually
> create protocol-level contention?). See *Experiments after the recorded
> suite* below. The headline numbers below are the **recorded** dataset and
> do not yet include these runs.

This repository contains a reproducible benchmark infrastructure (the
**Consensus Lab**) for comparing the write path of two consensus protocols in
the same execution environment, each through two independent implementations:

| Protocol | Category | Implementation |
|----------|----------|----------------|
| Raft | Leader-based | `github.com/hashicorp/raft` (pinned v1.7.3, module dep) |
| Raft | Leader-based | `go.etcd.io/raft` (v3.7.0, module dep; lab adapter) |
| EPaxos | Leaderless / Egalitarian | `efficient/epaxos` (pinned commit, vendored) |
| EPaxos | Leaderless / Egalitarian | `nvanbenschoten/epaxos` (pinned commit, vendored) |

Raft and EPaxos are used as representative implementations of leader-based
and leaderless consensus respectively. The lab does **not** modify the
upstream consensus protocol logic. It vendors the upstream source, builds it
into a Docker image, and drives it with a controlled client harness to
measure write throughput, write latency, write scalability, per-replica
resource utilisation, and write behaviour during failures. A second,
independent implementation of each protocol (etcd Raft, nvb EPaxos) isolates
implementation effects from protocol-level behaviour, and a separate
correctness harness verifies that every implementation performs consistent
consensus execution.

> The lab does not modify the upstream consensus logic and does not favour
> either protocol: it measures, validates, and reports. The interpretation of
> the measurements is in `paper/`.

## Headline results

All values are medians over ten independent repetitions computed from the raw
runs in `lab/results/raw/`; the table is the scaling family at 100 % writes
and concurrency 32. Medians are used because a small number of repetitions
stalled (two Raft runs coinciding with elevated host load, six EPaxos runs
at both low and high load); the means are lower for both protocols at every
replica count. The paper in `paper/` reports the full evaluation.

| Replicas | Raft (req/s) | EPaxos (req/s) | EPaxos/Raft | Raft p50 (ms) | EPaxos p50 (ms) |
|---------:|-------------:|---------------:|------------:|--------------:|----------------:|
| 3 | 4,351 | 5,292 | 1.22× | 6.9 | 5.6 |
| 5 | 3,150 | 5,443 | 1.73× | 9.2 | 5.5 |
| 7 | 2,590 | 3,053 | 1.18× | 12.0 | 5.4 |
| 9 | 2,113 | 2,094 | 0.99× | 14.6 | 5.5 |

Three properties of the recorded setup bound how these numbers may be read.
They are properties of the configuration, not new measurements:

- **The two protocol families are not persistence-matched.** The recorded
  Raft implementations persist consensus state (BoltDB for HashiCorp, a
  fsynced write-ahead log for etcd) while both recorded EPaxos
  implementations keep it in memory. The measured throughput difference
  therefore combines consensus structure with a storage path difference. The
  persistence-matching family varies persistence *within* each implementation
  to separate the two.
- **The communication-cost points were injected inside the implementations.**
  The archived `commcost` family applied a per-message sleep, plus jitter, in
  each implementation's own send path, so the injection point differs per
  implementation and is part of what those points measured. The clean
  network-delay family applies the delay outside the implementations
  (`tc/netem` in each replica's network namespace, no jitter) and records
  kernel-level evidence that it was installed.
- **The large replica counts are not per-replica CPU saturated.**
  `make resource-contention` reports the busiest replica reaching 38 % (7
  replicas) and 35 % (9 replicas) of its own 2-CPU quota, with host telemetry
  flagging 0 of 20 runs at each size. The requested quota is oversubscribed on
  paper (1.75x at 9 replicas), but the recorded telemetry does not support
  attributing these configurations' results to local CPU contention, so that
  cause must not be asserted.

- Throughput fell as replicas were added for **both** protocols (3 → 9
  replicas: −60 % EPaxos, −51 % Raft), so the scaling penalty is not specific
  to leader-based ordering. The EPaxos advantage disappeared at 9 replicas
  (ratio 0.99).
- EPaxos recorded the higher median throughput at 3, 5, and 7 replicas and
  the lower median latency at every replica count tested, but not at every
  concurrency level: at concurrency 1 the Raft median was 304 req/s against
  175 req/s for EPaxos, and the gap in EPaxos's favour grows with load
  (19,333 vs 41,077 req/s at concurrency 256, with p50 12.1 vs 5.6 ms).
- Aggregate per-replica CPU utilisation was similar (35.8 % Raft, 35.0 %
  EPaxos), but its distribution was not: in Raft the leader consumed
  *less* CPU than the followers (leader/follower ratio 0.61–0.75 across the
  scaling configurations), consistent with the followers doing the log
  persistence and apply work, while EPaxos stays flat (22.4–23.0 %). Peak
  per-replica RSS was ~1.8× higher for EPaxos (46.8 MB vs 26.4 MB).
- Failure cost follows the failed **role**: killing the Raft leader left a
  1.81–4.74 s write-availability gap and 2,169–4,133 failed requests, while
  killing a Raft follower or an EPaxos replica stayed below 0.09 s and 106
  failed requests. In the election experiment the gap tracks the measured
  failed-election count (r = 0.90); measured from the end of the isolation
  window it is smaller but still present (r = 0.41).
- The write-ratio (0–100 %) and conflict-ratio (0–100 %) sweeps showed no
  systematic throughput trend: conflict medians span 4,152–4,504 req/s (Raft)
  and 5,079–5,392 req/s (EPaxos).
- The same nominal configuration measured in four experiment families differs
  by about 2–3 % for both protocols once medians are used; the mean-based 21 %
  Raft band was an artefact of the stalled repetitions.
- EPaxos at seven and nine replicas showed request timeouts (30–250 per run
  at the 2,000 ms client timeout) that account for its reduced throughput via
  the closed-loop concurrency accounting; the successful requests themselves
  completed in 5.4–5.6 ms, the same as at three replicas. Per-replica CPU
  telemetry does not show saturation at these sizes (see the caveats above).
- The conflict-ratio sweep is flat in aggregate throughput, which does **not**
  mean the workload created no contention: the EPaxos fast/slow-path counters
  recorded per run show that a large conflict fraction does push commands onto
  the slow path. The conflict-validation family re-runs the sweep and reports
  the *realized* hot-key fraction and those counters, so the two questions
  ("did throughput move?" and "did contention happen?") are answered
  separately.
- The communication-cost figures below come from the **legacy** in-adapter
  injection and are retained as an archived dataset: at 10 ms, HashiCorp Raft
  lost 72 % of its throughput, nvb EPaxos 81 %, the original EPaxos 96 %, and
  etcd Raft collapsed by 94 % at 1 ms already. Jitter at a 5 ms cost reduced
  throughput by 24–36 % at 100 % jitter. Because the delay was applied inside
  each implementation's send path, these numbers rank injection points as well
  as implementations; they are not pooled with the clean network-delay family.
- A correctness harness (18 deterministic tests per implementation family:
  concurrency 1/8/32 plus leader, follower, and replica failures) passes for
  all four implementations: every replied request is applied exactly once,
  surviving replicas agree on the final state, and Raft restarts recover the
  committed state.

These are measurements of one implementation per protocol on a single
containerised host under the conditions in `docs/experiment-design.md`, not
universal properties of leader-based or leaderless consensus.

## Status

| Item | State |
|------|-------|
| Experiment matrix | run: 129 configurations, seven families, 10 repetitions each |
| Measured runs | 1,302 raw runs, 1,290 included; 12 excluded by the documented host-telemetry contamination rule and replaced |
| Implementation sensitivity | 4 implementations (HashiCorp/etcd Raft, original/nvb EPaxos); sensitivity + commcost datasets in `lab/results/sensitivity/`, `lab/results/commcost/` |
| Persistence matching | configured (`configs/persistence.json`); results land in `lab/results/persistence_control/` — durable vs in-memory within each implementation; nvb offers no durable mode and that cell is reported as a gap |
| Clean network delay | configured (`configs/networkdelay.json`); results land in `lab/results/network_delay_clean/` — 0/1/3/5/10 ms, no jitter, applied with `tc/netem` outside the implementations |
| Conflict validation | configured (`configs/conflictvalidation.json`); results land in `lab/results/conflict_validation/` — configured hot-key fraction × number of hot keys, with the realized fraction and the EPaxos counters |
| Correctness validation | 18/18 tests pass across all 4 implementations; `lab/results/correctness/correctness.json` |
| Configuration reports | `lab/results/failure-configuration.md` (failure-detection budget beside every measured availability gap), `lab/results/resource-contention.md` (per-replica CPU saturation at each replica count) |
| Processed measurements | `lab/results/processed/` (per-run metrics, per-configuration summaries, failure and run indexes) |
| Figures and report | `lab/results/figures/`, `lab/results/report/index.html` |
| Paper | `paper/release/The-Effects-of-Leadership-in-Distributed-Consensus.pdf` (English) and `...-ko.pdf` (Korean translation); sources in `paper/main.tex`, `paper/sections/`, `paper/tables/`, figures in `paper/figures/` |

`lab/results/` is generated and is not tracked by git; `make matrix`
regenerates the dataset, and `make report` regenerates every figure and the
HTML report from it.

## Structure

```
consensus-comparison/
├── README.md
├── CITATION.cff
├── LICENSE
├── NOTICE
├── THIRD-PARTY-NOTICES.md
├── paper/                  # ACM-format paper sources
│   ├── main.tex
│   ├── references.bib
│   ├── sections/
│   ├── tables/
│   ├── figures/            # figure PDFs generated by lab/report/figures.py
│   └── release/            # published PDF (English + Korean translation)
├── lab/                    # the Consensus Lab
│   ├── Makefile            # build / smoke / matrix / report entry points
│   ├── README.md           # lab documentation
│   ├── docs/               # benchmark protocol, audit, reproducibility notes
│   ├── upstream/           # vendored upstream implementations
│   │   ├── epaxos/         # efficient/epaxos (pinned commit, Apache-2.0, CMU 2013)
│   │   ├── nvb-epaxos/     # nvanbenschoten/epaxos (pinned commit, vendored)
│   │   └── raft/           # github.com/hashicorp/raft (pinned v1.7.3, module dep)
│   ├── adapters/raft/      # thin adapter around HashiCorp Raft (no consensus logic)
│   ├── adapters/etcdraft/  # adapter around go.etcd.io/raft (no consensus logic)
│   ├── adapters/nvbepaxos/ # adapter around nvanbenschoten/epaxos (no consensus logic)
│   ├── master/raft/        # coordination service for Raft runs (leader reporting only)
│   ├── client/             # common benchmark client (both protocols, one wire protocol)
│   ├── monitor/            # host-side per-container resource sampler
│   ├── runner/             # experiment runner: build, run, matrix, smoke, correctness
│   ├── report/             # processing + figure + HTML report generation (Python)
│   ├── configs/            # explicit experiment configurations (matrix, smoke,
│   │                       # sensitivity, commcost, persistence, networkdelay,
│   │                       # conflictvalidation)
│   ├── docker/             # lab runtime image
│   ├── internal/           # shared packages (wire protocol, state, config)
│   └── results/            # raw/processed/figures/report (generated)
└── docs/
    ├── methodology.md
    ├── experiment-design.md
    ├── reproducibility.md
    └── notes/
```

## Requirements

- Docker (with BuildKit)
- Docker Compose v2
- Go 1.26+ (to build the lab binaries)
- Python 3.10+ (runner/report only; installed into `lab/.venv`)

## Quickstart

```sh
cd lab
make build        # build Go binaries + upstream EPaxos + docker image
make smoke        # staged smoke tests (build, clusters, requests, workload, failures, metrics)
make correctness  # correctness-validation harness (18 deterministic tests, all 4 implementations)
make matrix       # run the full experiment matrix (hours)
make report       # process raw results + generate figures + HTML report
```

Then open `lab/results/report/index.html` in a browser. The report is fully
self-contained (charts are embedded as base64 PNG data) and works without an
internet connection.

## Experiments after the recorded suite

Three families sit on top of the recorded dataset. Each has its own
configuration and its own results subtree (`--results-base`), so the recorded
dataset is never read or written by them, and each has a `-dry-run` target
that expands and validates the configuration and prints the run IDs without
executing anything. `lab/docs/experiment-design.md` §6a documents the design.

```sh
make persistence-dry-run     # expand + validate, run nothing
make persistence             # durable vs in-memory, 4 implementations, r3+r5
make network-delay           # OS-level tc/netem delay, no jitter
make conflict-validation     # configured vs realized conflict

make persistence-report      # each family reports from its own subtree
make network-delay-report
make conflict-validation-report

make failure-configuration   # configuration reports over the recorded data
make resource-contention
```

- **Persistence matching** varies *only* how each implementation persists
  consensus state, and uses only modes the implementation provides: HashiCorp
  `raft-boltdb` vs `raft.NewInmemStore`, etcd write-ahead log vs the adapter's
  `-no-wal`, efficient/epaxos in-memory vs the upstream `-durable`. The nvb
  library exposes only an in-memory `Storage`, so no durable mode is offered
  for it; that cell is skipped during expansion and reported as a gap rather
  than filled with a mechanism the lab would have had to write.
- **Clean network delay** applies a fixed one-way latency (0/1/3/5/10 ms, no
  jitter) with `tc/netem` inside each replica's network namespace, scoped to
  inter-replica traffic so client-observed latency is not polluted. No adapter
  sleeps and no consensus source is modified. The runner verifies the
  installed qdisc against the kernel and stores the commands, their output and
  that verification in the run's `network.json`; a run whose emulation did not
  apply fails instead of being recorded as a delay measurement.
- **Conflict validation** sweeps both the configured hot-key fraction and the
  number of distinct hot keys, and reports the *realized* hot-key fraction
  (exact, from the per-request `hot` flag, since hot and cold key ranges are
  disjoint) alongside the EPaxos conflict/fast-path/slow-path counters where
  the implementation provides them — reported as unavailable, never as zero,
  for nvb. Conflict remains entirely client-side: nothing is injected into the
  EPaxos implementation.

## Manual execution

### Running a single experiment

```sh
cd lab
make run CFG=configs/smoke.json   # one experiment from a config
make run CFG=configs/matrix.json REP=1
```

### Running the experiment suite

```sh
make matrix       # full experiment matrix (seven families: workload, scaling,
                  # conflict, concurrency, election, failure, communication cost)
```

Those seven families are the recorded dataset. The three families added after
it are run separately and write elsewhere — see *Experiments after the recorded
suite* above.

### Correctness validation

```sh
make correctness  # short deterministic consensus-execution checks per
                  # implementation (sequential/concurrent/fault scenarios)
./bin/runner correctness --only nvb   # one implementation only
./bin/runner correctness --requests 5000   # more requests per test
```

Results land in `lab/results/correctness/` (`correctness.json` plus per-test
raw directories). The harness is independent of the performance benchmark:
it issues a fixed number of deterministic requests, collects every replica's
state snapshot over a read-only admin RPC, and checks for missing or
duplicate application, agreement among surviving replicas, and recovery of
the committed state after Raft restarts.

### Generating the report

```sh
make report       # raw -> processed -> figures -> HTML report -> validation
make html         # regenerate the HTML report from already-processed results
make validate     # validate the report against the underlying data
```

The report generator reads all available `lab/results/*.csv` files. Missing
experiments are shown as "Experiment not yet run." instead of failing.

## Experimental limitations (Threats to Validity)

- **Single-machine execution**: all replicas run on one host; results may not
  generalise to multi-machine deployments.
- **Docker/container scheduling effects**: container overhead, CPU sharing and
  network namespacing can influence measurements.
- **Closed-loop client behaviour**: the upstream client is closed-loop and
  batch-oriented; throughput and latency are measured per batch, not as a
  precise open-loop load.
- **Approximate failure recovery measurement**: recovery duration is
  approximate (batch-granularity), not a precise availability measurement.
- **Local bridge networking**: no WAN latency; wide-area behaviour is not
  captured.
- **Unmatched persistence in the recorded suite**: the recorded Raft
  implementations persist state while the recorded EPaxos implementations do
  not, so the recorded throughput comparison combines consensus structure with
  a storage-path difference. The persistence-matching family addresses this;
  until it is reported, the recorded difference must not be read as a
  protocol-level effect.
- **Legacy in-adapter delay injection**: the archived `commcost` family
  injected delay (and jitter) inside each implementation's send path, so its
  ranking reflects the injection point as well as the implementation. Those
  results are kept as a separate dataset and are not pooled with the clean
  OS-level network-delay family.
- **Single-implementation-per-family readings are configuration-bound**: a
  measured availability gap is bounded by the configured failure-detection
  budget (heartbeat 1000 ms, election 2000 ms) and, for the election runs, by
  the injected isolation window. Gaps are reported as observed under that
  configuration, not as generic recovery times.
- **Resource contention is not assumed**: where the large replica counts are
  discussed, whether local CPU contention actually bounded the run is decided
  from the recorded per-replica telemetry (`make resource-contention`), not
  from the requested CPU quota.
- **Implementation-specific behaviour**: results reflect the upstream
  `efficient/epaxos`, `nvanbenschoten/epaxos`, `hashicorp/raft`, and
  `go.etcd.io/raft` implementations, not the protocols in general. The
  implementation-sensitivity and communication-cost experiments quantify how
  much of the measured difference is implementation-level rather than
  protocol-level.
- **Representative protocols**: Raft and EPaxos are representative of
  leader-based and leaderless consensus respectively; they are not universal
  representatives of all protocols in each category.

No claims are made about behaviour that was not measured: every reported
number comes from `lab/results/` under the conditions documented in
`docs/experiment-design.md`. These limitations apply to the measured dataset
and to the results reported in `paper/`.

## License

This repository is licensed under the Apache License 2.0 (see `LICENSE`).
The vendored upstream source under `lab/upstream/epaxos/` is Copyright 2013
Carnegie Mellon University, also Apache-2.0 (see `THIRD-PARTY-NOTICES.md`).
HashiCorp Raft (`github.com/hashicorp/raft`) is MPL-2.0.
