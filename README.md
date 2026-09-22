# The Cost of Centralised Leadership in Distributed Consensus

> An Experimental Comparison of Leader-Based and Leaderless Consensus

Research repository.

<https://github.com/Sephy314/Leader-consensus-comparison>

> Status: the experiment matrix has been run and processed, and the paper is
> written from abstract to conclusion. The released PDF lives in
> `paper/release/`; `paper/` rebuilds it from source.

This repository contains a reproducible benchmark infrastructure (the
**Consensus Lab**) for comparing the write path of two consensus protocols in
the same execution environment:

| Protocol | Category | Implementation |
|----------|----------|----------------|
| Raft | Leader-based | `github.com/hashicorp/raft` (pinned v1.7.3, module dep) |
| EPaxos | Leaderless / Egalitarian | `efficient/epaxos` (pinned commit, vendored) |

Raft and EPaxos are used as representative implementations of leader-based
and leaderless consensus respectively. The lab does **not** modify the
upstream consensus protocol logic. It vendors the upstream source, builds it
into a Docker image, and drives it with a controlled client harness to
measure write throughput, write latency, write scalability, per-replica
resource utilisation, and write behaviour during failures.

> The lab does not modify the upstream consensus logic and does not favour
> either protocol: it measures, validates, and reports. The interpretation of
> the measurements is in `paper/`.

## Headline results

All values are means over ten independent repetitions computed from
`lab/results/processed/`; the table is the scaling family at 100 % writes and
concurrency 32. The paper in `paper/` reports the full evaluation.

| Replicas | Raft (req/s) | EPaxos (req/s) | EPaxos/Raft | Raft p50 (ms) | EPaxos p50 (ms) |
|---------:|-------------:|---------------:|------------:|--------------:|----------------:|
| 3 | 3,531 | 5,330 | 1.51× | 12.3 | 5.6 |
| 5 | 2,841 | 4,969 | 1.75× | 16.8 | 5.6 |
| 7 | 2,108 | 2,864 | 1.36× | 25.5 | 6.0 |
| 9 | 1,768 | 2,283 | 1.29× | 22.8 | 6.2 |

- Throughput fell as replicas were added for **both** protocols (3 → 9
  replicas: −57 % EPaxos, −50 % Raft), so the scaling penalty is not specific
  to leader-based ordering. The 95 % intervals of the means are separated at 3
  and 5 replicas and overlap at 7 and 9.
- EPaxos recorded the higher mean throughput and the lower median latency at
  every replica count tested, but not at every concurrency level: at
  concurrency 1 the Raft mean was 257 req/s against 174 req/s for EPaxos, and
  the gap in EPaxos's favour grows with load (17,845 vs 33,971 req/s at
  concurrency 256).
- Aggregate per-replica CPU utilisation was similar (35.8 % Raft, 35.0 %
  EPaxos), but its distribution was not: the 9-replica Raft means range from
  16.3 % to 29.4 % because leadership rotates between repetitions, while EPaxos
  stays flat (22.4–23.0 %). Peak per-replica RSS was ~1.8× higher for EPaxos
  (46.8 MB vs 26.4 MB).
- Failure cost follows the failed **role**: killing the Raft leader left a
  1.81–4.74 s write-availability gap and 2,169–4,133 failed requests, while
  killing a Raft follower or an EPaxos replica stayed below 0.09 s and 106
  failed requests. In the election experiment the gap tracks the measured
  failed-election count (r = 0.90); measured from the end of the isolation
  window it is smaller but still present (r = 0.41).
- The write-ratio (0–100 %) and conflict-ratio (0–100 %) sweeps showed no
  systematic throughput trend: conflict means span 3,514–4,425 req/s (Raft) and
  4,662–5,235 req/s (EPaxos).
- The same nominal configuration measured in four experiment families differs
  by up to 21 % (Raft) and 8 % (EPaxos); differences below that band are not
  read out of a single family.

These are measurements of one implementation per protocol on a single
containerised host under the conditions in `docs/experiment-design.md`, not
universal properties of leader-based or leaderless consensus.

## Status

| Item | State |
|------|-------|
| Experiment matrix | run: 129 configurations, six families, 10 repetitions each |
| Measured runs | 1,302 raw runs, 1,290 included; 12 excluded by the documented host-telemetry contamination rule and replaced |
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
│   │   └── raft/           # github.com/hashicorp/raft (pinned v1.7.3, module dep)
│   ├── adapters/raft/      # thin adapter around HashiCorp Raft (no consensus logic)
│   ├── master/raft/        # coordination service for Raft runs (leader reporting only)
│   ├── client/             # common benchmark client (both protocols, one wire protocol)
│   ├── monitor/            # host-side per-container resource sampler
│   ├── runner/             # experiment runner: build, run, matrix, smoke
│   ├── report/             # processing + figure + HTML report generation (Python)
│   ├── configs/            # explicit experiment configurations (matrix.json, smoke.json)
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
make matrix       # full experiment matrix (six families: workload, scaling,
                  # conflict, concurrency, election, failure)
```

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
  `efficient/epaxos` and `hashicorp/raft` implementations, not the protocols
  in general.
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
