# Consensus Benchmark Lab

A reproducible benchmark laboratory comparing **Raft** (leader-based
ordering) and **EPaxos** (leaderless ordering) using established upstream
implementations. The lab is neutral: it measures, validates, and visualizes;
it does not conclude which protocol is "better".

## Layout

```
lab/
├── README.md              this file
├── Makefile               build / smoke / matrix / report entry points
├── upstream/              vendored upstream consensus implementations
│   ├── epaxos/            efficient/epaxos (pinned commit, see VERSIONS.md)
│   └── raft/              github.com/hashicorp/raft (pinned v1.7.3, module dep)
├── adapters/raft/         thin adapter around HashiCorp Raft (no consensus logic)
├── master/raft/           coordination service for Raft runs (leader reporting only)
├── client/                common benchmark client (both protocols, one wire protocol)
├── monitor/               host-side per-container resource sampler
├── runner/                experiment runner: build, run, matrix, smoke
├── report/                processing + figure + HTML report generation (Python)
├── configs/               explicit experiment configurations (matrix.json, smoke.json)
├── docker/                lab runtime image
├── internal/              shared packages (wire protocol, state, config)
└── results/
    ├── raw/<run-id>/      raw measurements (never overwritten)
    ├── processed/         aggregated metrics (generated)
    ├── figures/           figures (generated from measured data)
    └── report/            self-contained HTML benchmark report (generated)
```

## Architecture

```
                  ┌──────────────┐
                  │ Common Client│
                  └──────┬───────┘
                         │
                 benchmark protocol
              (upstream EPaxos/genericsmr wire protocol)
                         │
             ┌───────────┴───────────┐
             │                       │
       ┌─────▼─────┐           ┌─────▼─────┐
       │   Raft    │           │  EPaxos   │
       │  Adapter  │           │  Upstream │
       └─────┬─────┘           └─────┬─────┘
             │                       │
       HashiCorp Raft          efficient/epaxos
```

- **One client** speaks the upstream EPaxos `genericsmr` wire protocol
  (`PROPOSE` → `ProposeReplyTS`) to both protocols. The workload generator
  (op mix, key distribution, timing) is identical; only endpoint selection
  differs and that is protocol-intrinsic: Raft sends every request to the
  leader reported by HashiCorp Raft; EPaxos spreads requests round-robin
  across replicas.
- **Raft adapter** (`adapters/raft/`) only converts benchmark requests into
  state-machine commands, calls `raft.Raft.Apply`, waits for the future, and
  returns the response. HashiCorp Raft performs leader election, log
  replication, quorum, commit, term management, and state transitions. The
  adapter reports the leader chosen by HashiCorp Raft (`raft.Raft.Leader`)
  to the raft master; it never elects or selects a leader.
- **EPaxos** uses the upstream `efficient/epaxos` server and master
  unchanged (`-e -exec -dreply`). Only external orchestration is added.
- **Resource monitoring** runs on the host: each replica is its own
  container (own network namespace), so per-replica network RX/TX is read
  from `/proc/<pid>/net/dev`, CPU from cgroup v2 `cpu.stat`, and RSS from
  `/proc/<pid>/status`. Raw samples are stored per replica; nothing is
  aggregated before storage.

## Quick start

```sh
cd lab
make build        # Go binaries + upstream EPaxos + docker image
make smoke        # staged smoke tests (build, clusters, requests, workload, failures, metrics)
make matrix       # full experiment matrix (hours)
make report       # process raw results + generate figures + HTML report
```

`make report` runs the full pipeline `raw → processed → figures → HTML report`
and validates the report against the underlying data. The HTML report is a
single self-contained offline file:

```
results/report/index.html
```

To regenerate only the HTML report from already-processed results:

```sh
make html
```

To validate the report without regenerating:

```sh
make validate
```

To open it in the default browser (optional):

```sh
make open-report
```

The report is entirely data-driven: every number, run, figure reference, and
failure record is derived from the actual result files at generation time
(`report/html.py`). No benchmark outcome is hardcoded. If `results/` is
replaced with a different valid dataset, the report describes the new dataset
without source changes. `report/validate_html.py` checks the report against
the underlying data (counts, sections, figure references, failed runs).

Run one experiment independently:

```sh
./bin/runner run configs/smoke.json --rep 1
```

## Experiments

- **Workload**: 100R/0W, 90R/10W, 50R/50W, 10R/90W, 0R/100W × concurrency
  1,2,4,8,16,32,64,128 × Raft, EPaxos × 3 repetitions.
- **Scaling**: 3,5,7,9 replicas, 100% write, both protocols.
- **Failure**: Raft leader failure, Raft follower failure, EPaxos replica
  failure, under sustained write load.

Every run records `metadata.json` (exact config, versions, host), raw
`requests.csv` (per-request metadata), raw `resources.csv` (per-replica
samples), and `events.csv` (failure/leader/phase timeline). Failed runs are
recorded in `results/run-index.csv` and never silently discarded.

## Fairness controls

All containers share one image and identical CPU/memory allocations.
`GOMAXPROCS` is identical for every replica in both protocols. Reads are
routed through consensus in both protocols (no Raft local-read
optimization). Protocol integration details that differ are recorded in each
run's `metadata.json` under `notes`.

## Provenance

Exact versions of every component are pinned and recorded per run:

- Raft: `github.com/hashicorp/raft` v1.7.3 (see `upstream/VERSIONS.md`)
- EPaxos: `github.com/efficient/epaxos` commit `791b115` (vendored)
- Go, Docker, Docker Compose, Python: recorded in `metadata.json`

`make check-upstream` verifies the copied wire protocol still matches the
vendored upstream source.

## No fabricated results

If a run fails, its status is `failed` with the reason recorded. Figures are
only produced for experiments with valid data. The lab never fills in
missing values or draws conclusions.

## Dependency-set size (DS = 9)

The vendored `efficient/epaxos` hardcodes its dependency-set size to 5,
including in the inter-replica wire format, which limits a cluster to 5
replicas. The lab extends it to 9 so that 7- and 9-replica clusters work.
This is a wire-format extension, not a consensus change: the protocol's
phases, quorums, and decision rules are untouched, and all replicas in a
cluster run the same patched binary. See `upstream/epaxos/UPSTREAM.md`.