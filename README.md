# The Cost of Centralised Leadership in Distributed Consensus

> An Experimental Comparison of Leader-Based and Leaderless Consensus

Research repository.

> Status: Research in progress.

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

> This is an experiment infrastructure, not a research paper. No conclusions
> are drawn here; the experiments must be run before any claims can be made.

## Structure

```
consensus-comparison/
├── README.md
├── LICENSE
├── NOTICE
├── THIRD-PARTY-NOTICES.md
├── paper/                  # ACM-format paper sources
│   ├── main.tex
│   ├── references.bib
│   ├── sections/
│   └── tables/
├── lab/                    # the Consensus Lab
│   ├── Makefile            # build / smoke / matrix / report entry points
│   ├── README.md           # lab documentation
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
make matrix       # full experiment matrix (scaling, workload, conflict, failure)
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

No claims are made that the experiments have demonstrated anything before the
experiments are actually run.

## License

This repository is licensed under the Apache License 2.0 (see `LICENSE`).
The vendored upstream source under `lab/upstream/epaxos/` is Copyright 2013
Carnegie Mellon University, also Apache-2.0 (see `THIRD-PARTY-NOTICES.md`).
HashiCorp Raft (`github.com/hashicorp/raft`) is MPL-2.0.
