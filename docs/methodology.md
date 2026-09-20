# Methodology

This document describes the research methodology of the consensus benchmark
comparison. It is the scientific counterpart of the operational lab documentation
(`lab/docs/`); it defines what is measured, how it is measured, and how the
measurements are interpreted. It does **not** draw conclusions about which
protocol is "better" — the data decides.

## 1. Research questions

The comparison compares two consensus protocols with fundamentally different
ordering architectures:

- **Raft** — leader-based ordering: a single elected leader orders all
  commands and replicates them to followers.
- **EPaxos** — leaderless ordering: any replica can order commands, and
  conflicts are resolved via dependency sets.

The research questions are:

1. How does throughput and latency scale with client concurrency under
   different read/write mixes?
2. How does throughput and latency change as the cluster grows?
3. How is per-replica resource consumption distributed — is work
   concentrated on a Raft leader, or spread evenly in EPaxos?
4. How does the conflict rate affect EPaxos, and does increased conflict
   correlate with increased use of its slow path?
5. What is the cost of leader failure and election recovery in Raft, and of
   replica failure in EPaxos?
6. Are the two systems being compared under equivalent consistency
   semantics?

The comparison does **not** assume either protocol is universally superior. The
benchmark is neutral and reproducible.

## 2. Systems under test

| System | Implementation | Version | Role in the lab |
|--------|---------------|---------|-----------------|
| Raft | `github.com/hashicorp/raft` | v1.7.3 | All consensus (election, replication, quorum, commit, terms) performed by the library; the lab's adapter only converts benchmark requests into `raft.Apply` calls |
| EPaxos | `github.com/efficient/epaxos` | commit `791b115` | Upstream server used unchanged (`-e -exec -dreply`); the lab adds only orchestration and instrumentation |

Both systems expose the same benchmark-facing interface: the upstream EPaxos
`genericsmr` wire protocol (`PROPOSE` → `ProposeReplyTS`). One common client
drives both protocols with the same workload generator.

## 3. Experimental dimensions

The primary independent variables are:

1. **Read/write workload** — the fraction of reads vs writes.
2. **Client concurrency** — the number of independent client workers.
3. **Replica scaling** — the cluster size.
4. **Conflict rate** — the fraction of requests targeting shared hot keys.
5. **Failure type** — leader failure, follower failure, replica failure, and
   controlled election failure.

The dependent variables are:

- **Throughput** (requests/second over the measured phase).
- **Latency** — p50, p95, p99, and maximum, computed from per-request
  timestamps.
- **Success/failure rate** — every request is recorded as success or failure
  with the error message.
- **Per-replica resources** — CPU utilisation, network RX/TX, and resident
  memory, sampled at ~200 ms resolution.
- **Fast/slow path ratio** (EPaxos) — the fraction of commands committed on
  the fast path vs the slow path, from the upstream's own counters.
- **Recovery/availability** — the interval without successful completions
  after a failure, and the time to a new leader.

## 4. Measurement methodology

### 4.1 Request-level measurement

Every request is recorded with: run ID, protocol, replica count, workload
ratio, concurrency, worker, sequence, request ID, operation, key, hot-key
flag, target replica, start/end timestamps, latency, success/failure, and
error message. From these raw records the processing pipeline computes
throughput, success/failure rate, and latency percentiles.

### 4.2 Resource measurement

Each replica runs in its own container (its own network namespace), so
per-replica network RX/TX is only measurable from the host. The host-side
monitor reads:

- **CPU**: cgroup v2 `cpu.stat` (cumulative usage/user/system microseconds).
- **Network**: `/proc/<pid>/net/dev` in the container's network namespace
  (cumulative bytes).
- **Memory**: `/proc/<pid>/status` VmRSS.

Values are stored raw (cumulative); rates are derived in the processing
pipeline. No replica data is aggregated before storage.

### 4.3 Warm-up separation

Each run has a warm-up phase (not recorded) followed by a measured phase
(recorded). Warm-up requests never enter the steady-state statistics.

### 4.4 Repetitions

Each configuration is repeated at least 10 times (the runner refuses to
under-sample). The report shows the mean over repetitions with the sample
size, standard deviation, and 95\,\% confidence interval; latency is
reported as percentiles, not only the mean. Each repetition is an
independent run: fresh containers, networks, and volumes, an independent
workload seed, and a randomized position in the execution schedule.

## 5. Fairness controls

The following are kept constant across protocols:

- Hardware, OS, container runtime.
- CPU and memory allocation per container (identical image for all roles).
- `GOMAXPROCS` for every replica in both protocols.
- Payload size, key distribution, workload, client implementation,
  concurrency, duration, warm-up, repetitions, monitoring method.

Protocol-specific integration details that differ are recorded in each run's
`metadata.json` under `notes`. The lab does not force both protocols to use
identical internal mechanisms.

## 6. Consistency semantics

Both protocols route reads through consensus: there is no Raft-only
local-read optimization. The comparison is therefore of equivalent
consistency behavior. The coordination mechanism differs:

- **Raft**: every read is a consensus command applied at the leader and
  committed to a quorum — leader-confirmed, quorum-based, linearizable.
- **EPaxos**: every read is a consensus command executed at all replicas in
  dependency order — quorum-based, dependency-ordered, linearizable.

These semantics are recorded per run in `metadata.json` under
`read_semantics` and displayed in the HTML report.

## 7. Statistical treatment

- **Latency**: p50, p95, p99, and maximum are computed from the full
  per-request distribution; the arithmetic mean is also reported but is not
  the primary latency metric.
- **Throughput**: central tendency (mean over repetitions) and variability
  are reported.
- **Recovery timing**: availability gaps are derived from request timestamps
  and the ~200 ms monitor resolution; they are labeled approximate.

## 8. Interpretation rules

- The lab measures, validates, processes, and visualizes. It does not
  conclude "Raft is better" or "EPaxos is better".
- Measured observations (e.g., "Raft leader failure shows a ~1.7–2.2 s
  write-availability gap") are reported as observations, not causal claims.
- Failed runs are recorded as failed with their reason; they are never
  presented as successful results or as zero throughput.
- No benchmark result is hardcoded in the report generator; every number is
  derived from the actual result files at generation time.

## 9. Threats to validity

The main threats are documented in the paper's threats-to-validity section
and summarized here:

- **Single host**: all replicas run on one machine over a bridge network;
  network characteristics are not those of a real deployment.
- **Recovery timing precision**: limited by the ~200 ms monitor resolution.
- **EPaxos in-memory state**: a restarted EPaxos replica loses local state
  (upstream default); Raft persists to BoltDB volumes.
- **Host-sleep contamination**: one earlier run was contaminated by a host
  suspend; it was removed and cleanly rerun. The historical record remains
  in `results/run-index.csv`.