# Reproducing the Lab

Everything runs from `lab/`. Results are written under `lab/results/`.

## Prerequisites

- Go 1.26.x (host)
- Docker 29.x with Compose v5.x
- Python 3.14 with matplotlib + pandas (`lab/.venv`)

## Build

```sh
cd lab
make build
```

`make build` does three things:

1. builds the lab Go binaries into `lab/bin/` (`raftadapter`, `raftmaster`,
   `client`, `monitor`, `runner`);
2. builds the upstream EPaxos binaries into `lab/bin/` (`epaxos-master`,
   `epaxos-server`) in GOPATH mode against the vendored source;
3. builds the lab runtime image `conslab:lab` containing those binaries.

## Smoke tests

```sh
make smoke
```

This runs the six staged smoke tests and stops at the first failure. It does
**not** start the full matrix.

| Stage | What it verifies |
|-------|------------------|
| 1 | All required binaries build. |
| 2 | A 3-replica cluster starts for Raft and EPaxos. |
| 3 | Basic PUT and GET requests succeed (0% and 100% writes). |
| 4 | A small mixed read/write workload runs. |
| 5 | Raft leader failure, Raft follower failure, EPaxos replica failure are injected and observed. |
| 6 | Request metrics, latency, CPU, network, and event timestamps are present. |

## One experiment

```sh
./bin/runner run configs/smoke.json --rep 1
# or a custom config:
./bin/runner run /path/to/config.json
```

## Full matrix

```sh
make matrix      # or: ./bin/runner matrix --config configs/matrix.json
```

Useful flags:

```sh
./bin/runner matrix --only workload   # one experiment family
./bin/runner matrix --limit 5         # first 5 configured runs
```

## Processing and figures

```sh
make report
```

This validates every run under `results/raw/`, aggregates valid runs into
`results/processed/`, and generates figures into `results/figures/`. Failed
runs are listed in `results/processed/run-index.json` and excluded from
aggregation but never deleted.

## Verify upstream provenance

```sh
make check-upstream
```

Fails loudly if the copied wire protocol or state machine drifts from the
vendored upstream sources.

## Result layout

```
lab/results/
├── run-index.csv              every run outcome (append-only, includes failures)
├── smoke-raw/                 archived smoke-test runs (evidence)
├── raw/<run-id>/
│   ├── metadata.json          exact config, versions, host, status
│   ├── requests.csv           per-request raw measurements
│   ├── resources.csv          per-replica raw resource samples
│   ├── events.csv             failure/leader/phase timeline
│   ├── phase.json             measured-phase start timestamp
│   ├── client-summary.json    client-side phase summary
│   ├── compose.yaml           exact orchestration used
│   └── container-logs.txt     all container logs (post-mortem)
├── processed/
│   ├── metrics.csv            aggregated per-run metrics
│   ├── resources.csv          aggregated per-replica resources
│   ├── failures.json          failure/recovery timelines
│   └── run-index.json         validation results per run
└── figures/                   generated figures (only for valid data)
```