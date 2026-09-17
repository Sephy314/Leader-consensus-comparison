# The Cost of Centralised Leadership in Distributed Consensus

> An Experimental Comparison of Leader-Based and Leaderless Consensus

Research repository.

> Status: Research in progress.

This repository contains a reproducible benchmark infrastructure (the
**Consensus Lab**) for comparing the write path of two consensus protocols in
the same execution environment:

| Protocol | Category | Implementation |
|----------|----------|----------------|
| Classic Paxos | Leader-based | `efficient/epaxos` (upstream) |
| EPaxos | Leaderless / Egalitarian | `efficient/epaxos` (upstream) |

The lab does **not** modify the upstream consensus protocol logic. It vendors
the upstream source, builds it into a Docker image, and drives it with a
controlled client harness to measure write throughput, write latency, write
scalability, per-replica resource utilisation, and write behaviour during
failures.

> This is an experiment infrastructure, not a research paper. No conclusions
> are drawn here; the experiments must be run before any claims can be made.

## Structure

```
consensus-study/
├── Makefile
├── README.md
├── LICENSE
├── NOTICE
├── THIRD-PARTY-NOTICES.md
├── paper/                  # (placeholder) paper sources
│   ├── main.tex
│   ├── references.bib
│   ├── figures/
│   └── tables/
├── lab/
│   ├── docker/Dockerfile   # multi-stage build of the upstream binaries
│   ├── scripts/
│   │   ├── gen_compose.py  # Docker Compose generator
│   │   ├── orchestrate.py  # scaling / workload / conflict / concurrency experiments
│   │   ├── failure_test.py # failure behaviour experiment
│   │   └── report.py       # self-contained HTML report generator
│   ├── experiments/        # experiment definitions / notes
│   ├── compose/generated/  # generated compose files (gitignored)
│   ├── results/            # CSV results + report.html (gitignored)
│   └── vendor/epaxos/      # vendored upstream source (Apache-2.0, CMU 2013)
└── docs/
    ├── methodology.md
    ├── experiment-design.md
    └── notes/
```

## Requirements

- Docker (with BuildKit)
- Docker Compose v2
- Python 3.10+ (only if orchestration / report generation is run locally)
- `pandas` and `matplotlib` (installed into `lab/.venv` by `make setup`)

## Quickstart

```sh
make build        # build the Docker image
make exp-all      # run all experiments (scaling, workload, conflict, failure)
make report       # generate results/report.html
```

Then open `lab/results/report.html` in a browser. The report is fully
self-contained (charts are embedded as base64 PNG data) and works without an
internet connection.

## Manual execution

### Starting a cluster

```sh
make up-paxos N=5   # 5-node Classic Paxos cluster
make up-epaxos N=5  # 5-node EPaxos cluster
make down           # tear down the active cluster
```

### Running individual experiments

```sh
make exp-scaling    # write throughput vs replica count (3,5,7,9)
make exp-workload   # write throughput vs write ratio (10,50,90,100 %)
make exp-conflict   # write throughput vs conflict ratio (0,25,50,75,100 %)
make exp-failure    # failure behaviour (paxos leader kill, epaxos random kill)
```

All experiments accept `Q` (requests per run) and `REPS` (repetitions):

```sh
make exp-scaling Q=200 REPS=3
```

### Generating the report

```sh
make report
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
  `efficient/epaxos` implementation, not the protocols in general.
- **Representative protocols**: Classic Paxos and EPaxos are representative
  of leader-based and leaderless consensus respectively; they are not
  universal representatives of all protocols in each category.

No claims are made that the experiments have demonstrated anything before the
experiments are actually run.

## License

This repository is licensed under the Apache License 2.0 (see `LICENSE`).
The vendored upstream source under `lab/vendor/epaxos/` is Copyright 2013
Carnegie Mellon University, also Apache-2.0 (see `THIRD-PARTY-NOTICES.md`).
