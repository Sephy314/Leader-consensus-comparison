# Reproducibility

This document describes how to reproduce the consensus benchmark study from
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

- **Workload**: 5 mixes × 8 concurrency levels × 2 protocols × 3 reps.
- **Scaling**: 4 replica counts × 2 protocols × 3 reps.
- **Conflict**: 7 conflict rates × 2 protocols × 3 reps.
- **Concurrency**: 9 concurrency levels × 2 protocols × 3 reps.
- **Election**: 6 failed-election targets × 3 reps (Raft).
- **Failure**: 3 failure types × 3 reps.

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
│   ├── metrics.csv            aggregated per-run metrics
│   ├── resources.csv          aggregated per-replica resources
│   ├── failures.json          failure/recovery timelines
│   └── run-index.json         validation results per run
├── figures/                   generated figures (only for valid data)
└── report/index.html          self-contained HTML report
```

## 9. Data integrity rules

- Raw results are never overwritten; each run gets a unique identifier.
- Failed runs are recorded in `results/run-index.csv` with their reason and
  are never silently discarded.
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