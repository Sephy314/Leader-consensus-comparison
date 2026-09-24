# Lab Experiment Design

This document describes how the benchmark lab (`lab/`) implements the
experimental design. It is the operational counterpart of the research
methodology; it does not draw scientific conclusions.

## 1. What is measured

The lab compares two consensus protocols under identical logical workloads:

- **Raft** — leader-based ordering, via `github.com/hashicorp/raft` v1.7.3.
- **EPaxos** — leaderless ordering, via `github.com/efficient/epaxos`
  (pinned commit `791b115`).

The experimental dimensions are: read/write workload, client concurrency,
replica scaling, per-replica resource distribution, leader failure, replica
failure, and recovery/availability behavior. Conflict-rate experiments are
out of scope.

## 2. Common benchmark interface

Both protocols expose the same benchmark-facing interface: the upstream
EPaxos `genericsmr` wire protocol (`PROPOSE` → `ProposeReplyTS`). One client
binary drives both protocols with the same workload generator (op mix, key
distribution, timing). The only protocol-intrinsic difference is endpoint
selection:

- Raft: every request is sent to the leader reported by HashiCorp Raft
  (`raft.Raft.Leader()`), via the raft master.
- EPaxos: requests are spread round-robin across replicas (leaderless).

Reads (GET) are routed through consensus in both protocols. There is no
Raft-only local-read optimization, so the comparison is of equivalent
consistency behavior.

## 3. Consensus implementations

### Raft

`lab/adapters/raft/` is a thin adapter. It:

1. accepts benchmark client requests on the `genericsmr` wire protocol,
2. converts them into state-machine commands (`state.Command`),
3. calls `raft.Raft.Apply`,
4. waits for the resulting future,
5. returns the benchmark response.

HashiCorp Raft performs leader election, log replication, quorum calculation,
commit logic, term management, and state transitions. The adapter reports the
leader chosen by HashiCorp Raft to the raft master; it never elects or
selects a leader. The application state machine (`fsm`) is separate from
Raft and implements the same logical semantics as the EPaxos state machine
(`state.Command.Execute`).

### EPaxos

The upstream `efficient/epaxos` server and master are used unchanged
(`-e -exec -dreply`). The lab only adds external orchestration (containers,
monitoring, failure injection). The upstream client is not used; the lab's
common client speaks the same wire protocol.

## 4. Cluster orchestration

Every run is a Docker Compose project with one container per role:

- `master` — coordination service (upstream EPaxos master, or the lab's raft
  master which mirrors its RPC surface).
- `replica0..N-1` — one container per replica (own network namespace).
- `client` — the benchmark client.

All containers share one image (`conslab:lab`) and identical CPU/memory
allocations. `GOMAXPROCS` is identical for every replica in both protocols.
Raft replicas persist state to named volumes (boltdb); EPaxos replicas keep
state in memory (upstream default). These integration differences are
recorded in each run's `metadata.json` under `notes`.

## 5. Measurement

### Request metrics

Every request is recorded with: run ID, protocol, replica count, workload
ratio, concurrency, worker, sequence, request ID, operation, key, target
replica, start/end timestamps, latency, success/failure, and error message.
From these the processing pipeline computes throughput, success/failure
rate, and p50/p95/p99 latency distributions.

### Resource metrics

Per-replica resource samples are collected from the host at ~200ms
resolution:

- CPU: cgroup v2 `cpu.stat` (usage/user/system, cumulative).
- Network RX/TX: `/proc/<pid>/net/dev` in the container's network namespace
  (cumulative bytes).
- Memory: `/proc/<pid>/status` VmRSS.

Raw samples are stored per replica with run ID, protocol, replica ID, and
timestamp. Nothing is aggregated before storage; the purpose is to determine
whether workload is distributed evenly or concentrated on particular
replicas.

## 6. Failure experiments

Failure injection is deterministic: the runner kills a container with
`docker kill` at a configured offset into the measured phase.

- **Raft leader failure**: the runner queries the raft master for the leader
  chosen by HashiCorp Raft, maps the leader index to the container hostname
  via the master's node list, and kills it. The client observes the election
  and reconnects to the new leader.
- **Raft follower failure**: a non-leader replica is killed.
- **EPaxos replica failure**: replica 0 is killed; the client moves to the
  next replica.
- **Election-failure (Raft)**: the leader is killed and the surviving
  replicas' Raft transport is isolated (drops `RequestVote`/`RequestPreVote`)
  for `target × election_timeout` ms, inducing repeated failed election
  attempts. The actual number of failed elections is measured from the
  adapters' dropped-message counters, never assumed from the target.

The failure timeline is reconstructed from `events.csv` (failure injected,
leader observed, restart issued) and per-request timestamps. Recovery timing
is approximate: it is derived from request timestamps and the ~200ms monitor
resolution, and is labeled as such.

Two recovery metrics are reported for election runs:

- **Availability gap**: the largest interval with no successful completion
  after the leader kill. This includes the isolation duration, which is
  proportional to the target, so its correlation with the measured election
  count is partly mechanical.
- **Recovery from isolation end**: the time from the end of the isolation
  window to the first successful request. This is decoupled from the
  injection duration and measures only the recovery behaviour after the
  injection has expired.

The detection budget these measurements are conditioned on is recorded per
run (`raft_heartbeat_ms`, `raft_election_ms`; 1000 ms and 2000 ms in the
recorded suite, identical for both Raft implementations).
`make failure-configuration` prints the configuration next to every measured
gap, so a gap is never quoted without it.

## 6a. Persistence matching, clean network delay, conflict validation

Three families added on top of the recorded suite. Each has its own
configuration file and its own results subtree (`--results-base`), and none of
them reads or writes the recorded dataset. Each has a `-dry-run` make target
that expands the configuration, validates every run and prints the run IDs
without executing anything.

### Persistence matching (`configs/persistence.json`, `make persistence`)

Holds workload, replica count, concurrency, durations, repetitions and network
constant and varies only how each implementation persists consensus state, so
a leader-based/leaderless difference can be checked against a
persistence-matched comparison instead of being attributed to the consensus
structure by default.

Only persistence modes the implementation itself provides are used; no
mechanism is added by the lab:

| implementation | durable | memory |
|---|---|---|
| HashiCorp Raft | `raft-boltdb` BoltStore (the recorded baseline) | `raft.NewInmemStore` + `raft.NewInmemSnapshotStore` |
| etcd Raft | adapter write-ahead log (batched write + fsync) | `raft.NewMemoryStorage` only (adapter `-no-wal`) |
| EPaxos (efficient) | upstream `-durable` (stable-store file + fsync) | upstream default (in-memory replica) |
| EPaxos (nvb) | **not offered** — the library exposes only the `Storage` interface with an in-memory implementation | `epaxos.NewMemoryStorage` |

The missing cell (nvb / durable) is skipped during expansion with a printed
notice and reported as a gap; it is never filled with a new mechanism. The
effective mode is recorded in every run's `metadata.json` under
`persistence.mode`, derived from the configuration, together with the
mechanism and the state-destruction path actually used.

### Clean network delay (`configs/networkdelay.json`, `make network-delay`)

A fixed one-way latency (0, 1, 3, 5, 10 ms) is applied to inter-replica
traffic from **outside** the implementations: the runner installs a `tc/netem`
qdisc inside each replica's own network namespace before the measured phase.
No adapter sleeps and no consensus source is modified, so the condition a run
is measured under is not produced by the code under test, and every
implementation receives the identical condition.

- Scope is `filter=peers`: only packets addressed to another replica are
  delayed, so client-observed latency is not polluted by an emulation the
  consensus path alone should receive. Every replica delays its own egress, so
  an inter-replica round trip accumulates the configured delay once per hop.
- The replicas get `NET_ADMIN` (added to the rendered compose file for this
  family only) so `tc` can be installed; the client and coordinator do not.
- There is no jitter in this family.
- The applied emulation is verified by reading the kernel's own view back
  (`tc -s qdisc show`); the commands, their output and that verification are
  stored per run in `network.json`, and a run whose emulation did not apply
  fails instead of being recorded as a delay measurement.

This family is separate from the archived `results/commcost/` dataset, which
used in-adapter per-message sleeps *with* jitter. `labcfg.Validate` refuses a
run that sets both mechanisms, and the two datasets are never pooled or
reinterpreted as one another.

### Conflict validation (`configs/conflictvalidation.json`, `make conflict-validation`)

Re-runs the conflict workload to answer a question the recorded sweep cannot:
*did the configured conflict fraction actually create protocol-level
contention?* It sweeps two independent variables: the configured hot-key
fraction (`conflict_pcts`) and the number of distinct hot keys that fraction
targets (`hot_keys`). A smaller hot-key set concentrates requests on fewer
keys and builds longer dependency chains at the same configured fraction,
which separates "how often a hot key is hit" from "how much contention that
creates".

Conflict is produced entirely in the client workload generator (disjoint
hot/cold key ranges); nothing is inserted into the EPaxos implementation — no
artificial conflicts, sleeps, injected dependencies or delays.

The report carries, per run, the **realized** hot-key fraction (from the
per-request `hot` flag, exact because the ranges are disjoint), the number of
distinct keys observed and the top key's share, plus the EPaxos
`conflicted` / `fast_path` / `slow_path` counters where the implementation
provides them. The counters are instrumentation added to the efficient/epaxos
codebase; nvb exposes none, so for nvb they are reported as *unavailable*
(`epaxos_counters_available=false`), never as zero.

## 7. Reproducibility

- Exact versions of every component are pinned and recorded per run
  (`metadata.json`): Raft v1.7.3, EPaxos commit `791b115`, Go, Docker,
  Docker Compose, Python, and the repository commit.
- The wire protocol in `internal/proto` is copied verbatim from the pinned
  upstream; `make check-upstream` verifies the copies still match.
- Raw results are never overwritten; each run gets a unique ID.
- Warm-up is separated from the measured phase; warm-up requests are not
  recorded.
- Failed runs are recorded in `results/run-index.csv` with their reason and
  are never silently discarded.

## 8. Known limitations

- **EPaxos single-port design**: the upstream server serves peer and client
  connections on the same port. The client therefore waits for the master to
  report all replicas registered plus a settle period before connecting;
  probing the port directly would corrupt the peer protocol.
- **EPaxos dependency-set size (`DS`)**: the vendored `efficient/epaxos`
  hardcodes the dependency-set size to 5 (`const DS = 5` in
  `src/epaxos/epaxos.go`, `Deps [5]int32` in the `epaxosproto` messages), which
  limits a cluster to 5 replicas. The lab extends it to 9 via a documented
  wire-format extension so that 7- and 9-replica clusters work. The protocol's
  phases, quorums, and decision rules are unchanged; all replicas run the same
  patched binary. See `upstream/epaxos/UPSTREAM.md`.
- **Recovery timing precision**: availability gaps are derived from request
  timestamps and the ~200ms monitor resolution; they are approximate.
- **EPaxos state is in-memory**: a restarted EPaxos replica loses local
  state (upstream default). Raft replicas persist to boltdb volumes.
- **Host clock**: all timestamps come from the host clock; containers share
  the host clock, so cross-container timing is consistent.
- **Single host**: all replicas run on one machine; network characteristics
  are loopback/bridge, not a real network.