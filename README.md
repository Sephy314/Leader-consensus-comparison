# The Cost of Centralised Leadership in Distributed Consensus

> An Experimental Comparison of Leader-Based and Leaderless Consensus

Research repository.

<https://github.com/Sephy314/Leader-consensus-comparison>

> Status: the experiment matrix has been run and processed, the correctness
> harness passes for all four implementations, and the paper is written from
> abstract to conclusion (13 pages). The released PDF lives in
> `paper/release/`; `paper/` rebuilds it from source.

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
  completed in 5.4–5.6 ms, the same as at three replicas.
- Under added one-way communication cost the four implementations differed
  sharply: at 10 ms, HashiCorp Raft lost 72 % of its throughput, nvb EPaxos
  81 %, the original EPaxos 96 %, and etcd Raft collapsed by 94 % at 1 ms
  already. Jitter at a 5 ms cost reduced throughput by 24–36 % at 100 %
  jitter, a much weaker effect than the cost itself.
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
| Correctness validation | 18/18 tests pass across all 4 implementations; `lab/results/correctness/correctness.json` |
| Processed measurements | `lab/results/processed/` (per-run metrics, per-configuration summaries, failure and run indexes) |
| Figures and report | `lab/results/figures/`, `lab/results/report/index.html` |
| Paper | `paper/release/The-Cost-of-Centralised-Leadership-in-Distributed-Consensus.pdf`; sources in `paper/main.tex`, `paper/sections/`, `paper/tables/`, figures in `paper/figures/` |

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
│   └── release/            # published PDF
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
│   ├── configs/            # explicit experiment configurations (matrix.json, smoke.json, sensitivity.json, commcost.json)
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
