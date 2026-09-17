# Experiment Design

This document specifies the experiment design of the consensus benchmark
comparison. It is the research-level specification; the operational implementation
is in `lab/docs/experiment-design.md`. The design is neutral: it measures
both protocols under identical logical workloads and does not assume a
winner.

## 1. Overview

The benchmark compares **Raft** (leader-based ordering) and **EPaxos**
(leaderless ordering) across six experiment families:

| Family | Independent variable | Values |
|--------|---------------------|--------|
| Workload | read/write mix × concurrency | 5 mixes × 8 concurrency levels |
| Scaling | replica count | 3, 5, 7, 9 |
| Conflict | conflict rate (hot-key fraction) | 0, 10, 25, 50, 75, 90, 100% |
| Concurrency | client concurrency (write-heavy) | 1, 2, 4, 8, 16, 32, 64, 128, 256 |
| Election | failed election attempts | 0, 1, 2, 3, 4, 5 |
| Failure | failure type | Raft leader, Raft follower, EPaxos replica |

Each configuration is repeated 3 times by default. The full matrix is
defined in `lab/configs/matrix.json`; no experimental parameter is
hard-coded in protocol or benchmark code.

## 2. Common benchmark interface

Both protocols expose the same benchmark-facing interface: the upstream
EPaxos `genericsmr` wire protocol (`PROPOSE` → `ProposeReplyTS`). One client
binary drives both protocols with the same workload generator (op mix, key
distribution, timing). The only protocol-intrinsic difference is endpoint
selection:

- **Raft**: every request is sent to the leader reported by HashiCorp Raft
  (`raft.Raft.Leader()`), via the raft master.
- **EPaxos**: requests are spread round-robin across replicas (leaderless).

Reads (GET) are routed through consensus in both protocols. There is no
Raft-only local-read optimization, so the comparison is of equivalent
consistency behavior.

## 3. Workload experiment

**Purpose**: measure throughput and latency as a function of read/write mix
and client concurrency.

- Mixes: 100R/0W, 90R/10W, 50R/50W, 10R/90W, 0R/100W.
- Concurrency: 1, 2, 4, 8, 16, 32, 64, 128.
- Cluster: 3 replicas.
- Metrics: throughput, p50/p95/p99 latency, success rate.

## 4. Scaling experiment

**Purpose**: measure throughput and latency as the cluster grows.

- Replica counts: 3, 5, 7, 9.
- Workload: 100% write (the primary scaling workload).
- Concurrency: 32.
- Metrics: throughput, p50/p95/p99 latency, per-replica resources.

Note: the vendored EPaxos implementation hardcodes its dependency-set size
to 5; the lab extends it to 9 via a documented wire-format extension so that
7- and 9-replica clusters work (see `lab/upstream/epaxos/UPSTREAM.md`).

## 5. Conflict-rate experiment

**Purpose**: determine whether EPaxos performance changes as the command
conflict rate increases, and whether increased conflicts correlate with
increased use of its slow path.

- Conflict rates: 0, 10, 25, 50, 75, 90, 100% of requests targeting shared
  hot keys.
- Workload: 100% write, concurrency 32, 3 replicas.
- The realized hot-key fraction is recorded per request, so the actual
  conflict rate is measurable rather than assumed.
- For EPaxos, the upstream's fast/slow path counters are instrumented and
  exposed over RPC (`stats.json` per run). The fast-path ratio is the
  fraction of commands committed in one round trip; the slow-path ratio is
  the fraction requiring an extra Accept round.
- The relationship between conflict rate and performance is discovered from
  the data, not assumed.

## 6. Write-concurrency scaling experiment

**Purpose**: determine how Raft and EPaxos behave as concurrent write load
increases, and whether work becomes concentrated on a Raft leader.

- Concurrency: 1, 2, 4, 8, 16, 32, 64, 128, 256.
- Workload: 100% write, 3 replicas.
- Cluster size, command size, storage configuration, duration, and client
  behavior are kept constant between protocols; only concurrency varies.
- Per-node CPU distribution is derived from `resources.csv`. Whether work
  concentrates on a Raft leader is left to the data; the benchmark does not
  label a bottleneck.

## 7. Election-failure recovery experiment

**Purpose**: test the hypothesis that repeated failed Raft elections increase
service recovery time. This is not a generic leader-failure experiment; it
controls the number of unsuccessful election attempts before a successful
election.

- Failed elections: 0 (baseline), 1, 2, 3, 4, 5.
- Procedure: kill the current leader, then isolate the surviving replicas'
  Raft transport (vote requests dropped) for `failed_elections ×
  election_timeout`, so the next election attempt(s) fail. The isolation
  expires automatically, after which a successful election restores service.
- The injection is a fault hook around the existing transport
  (`IsolateElections` RPC); HashiCorp Raft's election algorithm is not
  modified.
- The actual number of failed elections is measured from the leader-election
  log transitions, not assumed.
- Metrics: detection time, failed-election time, successful-election time,
  total recovery time, time to first successful post-election write.
- The relationship between failed-election count and recovery time is
  measured, not assumed to be linear.

## 8. Failure experiments

**Purpose**: measure the availability impact of replica failures under
sustained write load.

- **Raft leader failure**: the leader (identified via `raft.Raft.Leader()`)
  is killed; the client observes the election and reconnects to the new
  leader.
- **Raft follower failure**: a non-leader replica is killed.
- **EPaxos replica failure**: a replica is killed; the client moves to the
  next replica.
- Metrics: failure timestamp, leader identity, request success/failure,
  throughput, latency, new leader, recovery time, approximate write
  availability gap.

The failure timeline is reconstructed from `events.csv` (failure injected,
leader observed, restart issued) and per-request timestamps. Recovery timing
is approximate (monitor resolution ~200 ms) and labeled as such.

## 9. Resource distribution

Per-replica resource samples are collected from the host at ~200 ms
resolution:

- CPU: cgroup v2 `cpu.stat`.
- Network RX/TX: `/proc/<pid>/net/dev` in the container's network namespace.
- Memory: `/proc/<pid>/status` VmRSS.

Raw samples are stored per replica with run ID, protocol, replica ID, and
timestamp. Nothing is aggregated before storage. The purpose is to determine
whether workload is distributed evenly or concentrated on particular
replicas.

## 10. Read-path / consistency validation

Before interpreting the Read benchmark, the consistency semantics of each
system are verified and recorded:

- **Raft**: linearizable, leader-confirmed, quorum-based reads (every read
  is a consensus command; no local-read optimization).
- **EPaxos**: linearizable, quorum-based, dependency-ordered reads (reads
  and writes share the same consensus path).

These are recorded per run in `metadata.json` under `read_semantics` and
displayed in the HTML report. Both systems provide equivalent consistency
guarantees, so the Read comparison is valid.

## 11. Run configuration

Every run records its exact configuration in `metadata.json`:

```
experiment, protocol, replica_count, read_pct, write_pct, concurrency,
conflict_pct, duration_s, warmup_s, repetitions, failure_mode,
failed_elections, seed, keyspace, timeout_ms, resource allocations,
raft_heartbeat_ms, raft_election_ms
```

Warm-up is separated from the measured phase; warm-up requests are not
recorded. Each run has a unique identifier; raw results are never
overwritten.

## 12. Result pipeline

```
raw results (results/raw/<run-id>/)
    ↓
validation (report/process.py)
    ↓
aggregated metrics (results/processed/)
    ↓
figures (results/figures/)
    ↓
HTML report (results/report/index.html)
```

All figures and the HTML report are generated programmatically from measured
data. No benchmark result is hardcoded; if `results/` is replaced with a
different valid dataset, the report describes the new dataset without source
changes.