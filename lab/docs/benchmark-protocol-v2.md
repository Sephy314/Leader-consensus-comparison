# Benchmark Protocol v2 — Reliability Redesign

This document describes the revised experimental protocol implemented after
the reliability audit (`docs/audit-2026-09-19.md`). It is the operational
specification of how the benchmark is run, isolated, randomized, and
verified. It supersedes the implicit protocol of the pre-audit dataset.

## 1. Mandatory invariants

Every measured experiment must satisfy all of the following. The runner
enforces them; a configuration that violates them is refused, not silently
adjusted.

| Invariant | Rule | Enforcement |
|---|---|---|
| Repetitions | ≥ 10 independent repetitions per configuration | `runConfigs` refuses `repetitions < 10` |
| Isolation | Fresh process/container/volume/network state per repetition | Per-run Compose project; `down -v`; pre-flight + post-teardown verification |
| Randomization | Conditions interleaved in reproducibly randomized blocks | `buildSchedule` (seeded shuffle per block) |
| Seeds | Per-repetition seeds, recorded | `seed_used = base + rep` in `metadata.json` |
| Persistence | Explicitly classified; destroyed between reps | `persistence` block in `metadata.json`; volume removal verified |
| Failed runs | Recorded, never deleted | `run-index.csv` + `metadata.json` status |
| Anomalies | Detected, marked, reported; never silently deleted | `host-telemetry.csv` + `detectHostAnomalies` |
| Raw data | Preserved; report generated from it | `results/raw/` untouched by processing |

## 2. Blocked randomization (condition-order confounding)

The pre-audit runner executed all repetitions of one condition consecutively
(e.g. `target=2 × 3`, then `target=3 × 3`, …), confounding the treatment
variable with wall-clock execution time. The revised runner executes a
**blocked randomized schedule**:

- Block *b* contains every (configuration, repetition *b*) pair exactly once.
- Within a block, the order is a Fisher–Yates shuffle from a seeded RNG.
- The schedule seed is a CLI flag (`--schedule-seed`, default 1) and is
  recorded in every run's `metadata.json` (`schedule.schedule_seed`) and in
  `results/execution-manifest.json`.

This guarantees every condition is represented throughout the experiment
timeline, and the exact order is reproducible from the recorded seed.

## 3. Execution manifest

After a batch, the runner reconstructs the **actual** execution order from
per-run timestamps (not the intended schedule) and writes
`results/execution-manifest.json`:

- run_id, repetition_id, condition, random_seed, block_id, sequence_index
- started_at, ended_at, duration, setup_completed_at (phase start),
  measurement_started_at, teardown_completed_at
- exit_status, anomalies

The manifest also computes: min/max inter-run gap, overlapping-run
detection, cleanup failures, and host anomalies. The HTML report's
**Execution Integrity** section renders it, including a condition-over-time
timeline figure.

## 4. Isolation lifecycle

Each repetition follows the standard lifecycle:

1. **Pre-flight** (`preflightIsolation`): fixed host ports free; no
   container/volume/network with this run ID exists. Failure to establish a
   clean environment fails the run.
2. **Setup**: render compose, `docker compose up -d`.
3. **Ready**: wait for `phase.json` (client writes it when the measured
   phase starts, after warm-up).
4. **Warm-up**: client warm-up phase (no recording).
5. **Measurement**: measured phase; requests recorded per-request.
6. **Condition injection** (failure/election runs): kill/isolate at the
   configured offset into the measured phase.
7. **Collection**: per-replica counters (`stats.json`), resource samples,
   host telemetry.
8. **Final-state verification** (`validateFinalState`): failure runs must
   show a valid leader after the injection (Raft) or a successful request
   after the injection (EPaxos); otherwise the run is failed.
9. **Teardown**: `docker compose down -v`, then `verifyTeardown` checks
   containers, volumes, and networks are gone; the result is recorded in
   `teardown.json`.
10. **Metadata**: written with schedule, seed, persistence class, versions,
    host, timestamps.

## 5. Persistence classification

Persistence is explicitly classified per run in `metadata.json`:

- **Raft**: class B (state that MUST be destroyed between repetitions).
  HashiCorp Raft persists term, votedFor, log entries, snapshots, and
  cluster membership to per-replica BoltDB files in named volumes
  (`raftdata0..N-1`). These volumes are removed by `down -v`, and removal is
  verified post-teardown. Persistence is not part of any experiment.
- **EPaxos**: class B. State is in-memory (upstream default, no `-durable`);
  no WAL/database files exist.

No experiment measures recovery from persisted state; if one is added, it
must be a dedicated experiment with its own persistence class.

## 6. Randomness

- **Workload seeds**: `seed_used = config.seed + repetition`. Each
  repetition of a configuration uses an independent seed, so no two
  repetitions replay the same workload sequence. The seed actually used is
  recorded, so a failed run can be reproduced exactly.
- **Schedule seed**: `--schedule-seed` (default 1), recorded in the manifest
  and per-run metadata.
- **Worker RNGs**: each client worker gets `rand.New(rand.NewSource(seed +
  worker_id))`; no shared RNG.

## 7. Anomaly detection

Host telemetry (`host-telemetry.csv`, ~200 ms) records load1/5/15 and
available memory per run. The runner and the processing pipeline detect:

- **host_pause**: telemetry gap > 5 s (host suspend or runner pause)
- **clock_jump**: telemetry timestamp went backwards
- **cpu_starvation**: `load1 > 0.9 x NumCPU`

The starvation threshold is relative to the host's CPU count rather than a
fixed absolute value, because the benchmark itself legitimately drives the
host to roughly 9 cores (3 replicas at 2.0 CPUs + client 2.0 + master 1.0)
on the 12-core measurement host; an absolute threshold would flag the
benchmark's own load. On that host the threshold is 10.8, and the same
`0.9 x NumCPU` rule is applied both by `runner/manifest.go` (reading the
live host CPU count) and by `report/process.py` (reading `host.cpus` from
the run's metadata).

Anomalous runs are **marked** (`contaminated` in `run-index.json`, an
anomaly list in `metrics.csv`) and **reported** in the Execution Integrity
section. They are never deleted. The exclusion rule is fixed in advance and
applied uniformly to every condition by `mark_included` in
`report/process.py`:

1. A run whose host telemetry was flagged contaminated is excluded from
   every aggregate (mean, SD, CI, correlation, figures).
2. A lost observation is restored with
   `runner matrix --rerun-contaminated`, which re-runs exactly the
   contaminated or failed attempts. The replacement gets a new run ID
   (`<id>-2`) recording `rerun_of` and `rerun_reason`; the original attempt
   is kept. When several attempts exist for the same (condition,
   repetition), the non-contaminated attempt with the highest attempt number
   is used, so every condition keeps exactly one observation per planned
   repetition.
3. Excluded runs stay visible: `included=0` in `metrics.csv`, and
   `n_observed` vs `n` in `config-summary.csv`.

Host suspend (the WSL2 host sleeping) is the anomaly actually observed: the
wall clock jumps while the process is frozen, so it appears as a multi-second
(or longer) telemetry gap *inside* a run. Those runs' measured phases are not
trustworthy and are replaced as above.

## 8. Statistical treatment

- Every aggregate reports **n** (number of independent observations).
- Mean, median, sample SD, and 95% CI (Student's t) are computed from the
  raw per-run observations (`report/stats.py`, no scipy dependency).
- Latency distributions report p50/p95/p99 from per-request latencies.
- The election experiment reports the **Pearson correlation** of the
  availability gap with the **measured** number of failed elections, with n
  and a t-based p-value, and plots every observation.
- Outliers are not removed; if an observation is excluded, the exclusion
  rule is predefined and recorded.

## 8a. Election recovery metrics (decoupled from the injection)

The election experiment's availability gap **includes the isolation
duration by construction**: the injection isolates the surviving replicas'
Raft transport for `isoMS = target × election_timeout` ms, so a run with a
larger target has a longer gap regardless of the cluster's recovery
behaviour. A positive gap correlation with the measured election count is
therefore partly mechanical.

To separate the injection duration from the recovery behaviour, the
processing pipeline computes a second metric per election run:

- **`recovery_from_isolation_end_s`**: time from the end of the isolation
  window (latest `election_isolation_confirmed` timestamp + `isoMS`) to the
  first successful request. This is decoupled from the injection duration
  and measures only the recovery behaviour after the injection has expired
  (election + client reconnect).

The report and the publication figure show **both** metrics with their
correlations. If `recovery_from_isolation_end` is roughly constant across
the measured election count, the gap correlation is explained by the
injection mechanism; if it grows, the cluster needs extra time to stabilise
after many failed elections. The interpretation is left to the data; the
benchmark does not assume either outcome.

## 9. Measured failed elections

The election experiment's independent variable is the **measured** number of
failed elections, never the configured target. HashiCorp Raft v1.7.3 enables
pre-vote by default. A failed election attempt is either (a) a pre-vote round
that receives no response (the candidate sends `RequestPreVote` to every
peer, all dropped while the transport is isolated), or (b) a real vote round
whose pre-vote succeeded but whose `RequestVote` was dropped. Each failed
attempt therefore produces (replicas − 1) dropped messages of one kind. The
Raft adapter counts dropped pre-votes and dropped votes per replica
(`stats.json`); the report sums both across replicas and divides by
(replicas − 1) to recover the number of failed attempts. The target is a
nominal setting only; the measured count varies because HashiCorp Raft
randomizes election timeouts.

The isolation targets the correct containers: the master's node list is in
registration order (not container numbering), so the runner maps each master
index to its container hostname before isolating.

The election experiment's measured phase is long enough for the longest
isolation to expire and recovery to complete: `labcfg.Validate` requires
`at_s + isolation + 2×election_timeout + 2s ≤ duration_s`. A run whose
cluster cannot recover within the phase is recorded as failed (final-state
verification), never silently accepted.

## 10. Validation gate

`runner validate` runs a lightweight randomized-order experiment (3
conditions × 3 repetitions, short durations) and verifies automatically:

- every condition appears in every block; no condition is systematically
  early/late
- every run starts from a clean state (pre-flight passed)
- teardown is complete and verified
- persistent state does not survive between runs
- run metadata is complete (schedule, seed, persistence, timestamps)
- no host anomalies, no overlapping runs

The full suite must not be run until this validation passes.

## 11. Dataset scoping

- Measured runs go to `results/raw/`; smoke runs go to `results/smoke-raw/`
  so functional gates never pollute the measured dataset.
- Each batch records a `batch_id` (timestamp) in every run's metadata and in
  the execution manifest, so the report can be scoped to a dataset.
- The pre-audit dataset is archived (not deleted) before the revised suite
  runs, so old and new results can be compared.

## 12. Interrupted runs and batch resumption

The suite runs for ~20 hours; the host can be interrupted (reboot, crash,
kill). The dataset must survive that without either losing observations or
silently duplicating them.

- `metadata.json` is written **last**, on every exit path, so "metadata.json
  exists" is the definition of a completed run. A directory without it was
  left by a killed runner: it is not part of the dataset, no report stage
  summarises it, and it is re-run **in place** (the run ID is reused, never
  suffixed), so a duplicate of the same run ID cannot enter the dataset.
- `runner matrix --skip-existing` resumes an interrupted suite: completed
  runs are skipped and the rest continue. The schedule is a pure function of
  the configuration list and the recorded seed, so a resumed batch executes
  exactly the missing runs at exactly their original positions. Verified on
  2026-09-20: after a kill at run 956, the resumed batch restarted at
  schedule index 956/1290.
- A dataset may therefore span several batches. Each run records its
  `batch_id`; the execution manifest lists every batch, and the report's
  Execution Integrity section shows them plus the resulting wall-clock gap,
  so a multi-batch dataset is visible rather than hidden.
- `runner manifest` re-derives `results/execution-manifest.json` from the
  completed runs, so the manifest is reproducible without re-running
  anything.
- The truncated run and its compose project are archived/torn down before
  resuming (`results/archive/`), keeping the evidence without polluting the
  dataset or leaking containers into the next run.