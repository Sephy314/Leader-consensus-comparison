# Reproducibility

This document describes how to reproduce the consensus benchmark comparison from
a known repository state. The operational commands live in `lab/`; this
document is the research-level reproduction procedure.

## 1. Prerequisites

- Go 1.26.x (host)
- Docker 29.x with Compose v5.x
- Python 3.14 with matplotlib + pandas (`lab/.venv`)

## 2. Pinned versions

Every important component is pinned and recorded per run in
`metadata.json`:

| Component | Version | Where pinned |
|-----------|---------|--------------|
| `github.com/hashicorp/raft` | v1.7.3 | `lab/go.mod` / `lab/go.sum` |
| `github.com/hashicorp/raft-boltdb/v2` | v2.3.0 | `lab/go.mod` / `lab/go.sum` |
| `github.com/efficient/epaxos` | commit `791b115669fca472d3136f6a2eda46c00b3f8251` | vendored at `lab/upstream/epaxos/` |
| Go | 1.26.x | recorded per run |
| Docker / Compose | 29.x / v5.x | recorded per run |
| Lab code | repository commit | recorded per run (`git rev-parse HEAD`) |

`make check-upstream` verifies that the copied wire protocol
(`lab/internal/proto`, `lab/internal/state`) still matches the vendored
upstream source byte-for-byte.

## 3. Build

```sh
cd lab
make build
```

`make build`:

1. builds the lab Go binaries into `lab/bin/` (`raftadapter`, `raftmaster`,
   `client`, `monitor`, `runner`);
2. builds the upstream EPaxos binaries into `lab/bin/` (`epaxos-master`,
   `epaxos-server`) in GOPATH mode against the vendored source;
3. builds the lab runtime image `conslab:lab` containing those binaries.

## 4. Smoke tests

```sh
make smoke
```

Runs the six staged smoke tests and stops at the first failure. It does
**not** start the full matrix.

| Stage | What it verifies |
|-------|------------------|
| 1 | All required binaries build. |
| 2 | A 3-replica cluster starts for Raft and EPaxos. |
| 3 | Basic PUT and GET requests succeed (0% and 100% writes). |
| 4 | A small mixed read/write workload runs. |
| 5 | Raft leader failure, Raft follower failure, EPaxos replica failure are injected and observed. |
| 6 | Request metrics, latency, CPU, network, and event timestamps are present. |

## 5. Unit and pipeline tests

```sh
make test
```

Runs the Go unit tests (client workload generation, configuration
validation, experiment classification, FSM, election-failure transport
isolation) and the Python pipeline tests (process → html → validate on a
synthetic dataset).

## 6. Full experiment matrix

```sh
make matrix
# or, to run one family:
./bin/runner matrix --config configs/matrix.json --only conflict
```

The full matrix is defined in `lab/configs/matrix.json`:

- **Workload**: 5 mixes × 8 concurrency levels × 2 protocols × 10 reps.
- **Scaling**: 4 replica counts × 2 protocols × 10 reps.
- **Conflict**: 7 conflict rates × 2 protocols × 10 reps.
- **Concurrency**: 9 concurrency levels × 2 protocols × 10 reps.
- **Election**: 6 failed-election targets × 10 reps (Raft).
- **Failure**: 3 failure types × 10 reps.

Every measured configuration runs at least 10 independent repetitions; the
runner refuses to under-sample. Each repetition is a fresh Docker Compose
project (new containers, networks, volumes), torn down and verified after
the run. Conditions are executed in reproducibly randomized blocks
(`--schedule-seed`), and each repetition uses an independent workload seed
(`seed_used = base + rep`, recorded in `metadata.json`).

The suite takes many hours, so it is interrupted by host restarts and
suspends. Resuming is safe:

```sh
make matrix            # same as: runner matrix --skip-existing
```

`--skip-existing` skips runs that already completed and continues the rest
in their original schedule positions (the schedule is a function of the
configuration list and the recorded seed). `metadata.json` is written last,
so "metadata.json exists" defines a completed run: a directory left by a
killed runner is not part of the dataset and is re-run in place. A dataset
may therefore span several batches; each run records its `batch_id`, and the
execution manifest and report list every batch plus the wall-clock gap.

```sh
make manifest           # rebuild results/execution-manifest.json from the completed runs
make rerun-contaminated # re-run attempts flagged contaminated by host anomalies
```

Host suspension (a laptop sleeping) is detected per run as a telemetry gap.
Such runs are excluded from every aggregate and replaced by
`rerun-contaminated`, so each condition keeps its full set of repetitions.
The replacement is a new run recording `rerun_of`/`rerun_reason`; the
original attempt is kept in the dataset and in the manifest.

## 7. Report generation

```sh
make report
```

Runs the full pipeline `raw → processed → figures → HTML report → validate`.
The HTML report is a single self-contained offline file:

```
lab/results/report/index.html
```

To regenerate only the HTML report from already-processed results:

```sh
make html
```

To validate the report against the underlying data:

```sh
make validate
```

## 8. Result layout

```
lab/results/
├── run-index.csv              every run outcome (append-only, includes failures)
├── execution-manifest.json    actual execution order, batches, gaps, anomalies
├── logs/                      runner logs of each batch (survive a host restart)
├── archive/                   pre-audit dataset and interrupted runs (kept as evidence)
├── raw/<run-id>/
│   ├── metadata.json          exact config, versions, host, status, read semantics
│   ├── requests.csv           per-request raw measurements
│   ├── resources.csv          per-replica raw resource samples
│   ├── events.csv             failure/leader/phase timeline
│   ├── stats.json             EPaxos fast/slow path counters (per replica)
│   ├── phase.json             measured-phase start timestamp
│   ├── client-summary.json    client-side phase summary
│   ├── compose.yaml           exact orchestration used
│   └── container-logs.txt     all container logs (post-mortem)
├── processed/
│   ├── metrics.csv            aggregated per-run metrics (included=1/0 per run)
│   ├── resources.csv          aggregated per-replica resources
│   ├── failures.json          failure/recovery timelines
│   ├── config-summary.csv     per-condition n, mean, median, SD, 95% CI
│   └── run-index.json         validation results per run
├── figures/                   generated figures (only for valid data)
└── report/index.html          self-contained HTML report
```

## 9. Data integrity rules

- Raw results are never overwritten; each run gets a unique identifier. A
  replacement run for a contaminated repetition gets a new identifier
  (`<id>-2`) rather than overwriting the attempt it replaces.
- Failed runs are recorded in `results/run-index.csv` with their reason and
  are never silently discarded.
- Host anomalies (suspend, CPU starvation, clock jump) are detected per run,
  recorded in `metrics.csv`/`run-index.json`/the execution manifest, and
  excluded from aggregates by one rule (`process.py:mark_included`), which
  prefers a replacement run when one exists. Excluded runs are reported in
  the report's Execution Integrity section, never hidden.
- Warm-up data is never mixed into steady-state results.
- All figures and the HTML report are generated programmatically from
  measured data; no benchmark result is hardcoded.
- If a run fails, its status is `failed` with the reason recorded. Fix
  infrastructure problems and rerun where appropriate; keep the failed run
  metadata.

## 10. Known deviations and limitations

- **EPaxos dependency-set size**: the vendored upstream hardcodes `DS = 5`;
  the lab extends it to 9 via a documented wire-format extension so that
  7- and 9-replica clusters work. All replicas run the same patched binary;
  the protocol's phases, quorums, and decision rules are unchanged. See
  `lab/upstream/epaxos/UPSTREAM.md`.
- **EPaxos single-port design**: the upstream server serves peer and client
  connections on the same port; the client waits for master-reported
  readiness plus a settle period instead of probing.
- **Recovery timing is approximate** (~200 ms monitor resolution).
- **EPaxos state is in-memory** (upstream default); Raft persists to BoltDB
  volumes.
- **Single host**: all replicas run on one machine over a bridge network.
- **Host-sleep contamination**: one earlier run was contaminated by a host
  suspend; it was removed and cleanly rerun. The historical record remains
  in `results/run-index.csv`.