# Leader CPU share in the Raft scaling runs (2026-09-26)

## Why this exists

Section 4.4 originally reported that the elected Raft leader consumed *less*
CPU than the followers (a leader/follower ratio of 0.61--0.64 at three
replicas rising to 0.73--0.75 at seven and nine). That is inverted.

The lab master assigns node ids in registration order, so the node id in the
client's `raft leader is replica N` line is not the container index used by
`resources.csv`: in `scaling-raft-r3-w100-c32-1` the mapping is
`replica0-1 -> 0`, `replica1-1 -> 2`, `replica2-1 -> 1`, so the reported leader
(node 2) is container `replica1`. Reading the node id as a container index
selects a *follower* and inverts the ratio. Any analysis that joins the
cluster's leader report to container-indexed telemetry must first read the
`registered with master: id=N` line of each container, which is what
`report/leader_cpu.py` does.

## Result

`python3 report/leader_cpu.py` over the 40 recorded Raft scaling runs:

| replicas | runs | median leader/follower CPU |
|---|---|---|
| 3 | 10 | 2.19 |
| 5 | 10 | 2.78 |
| 7 | 10 | 3.27 |
| 9 | 10 | 3.79 |

The leader is the CPU hotspot, and its share grows with the number of peers it
serves. The paper reports these as 2.2, 2.8, 3.3 and 3.8.

## Independent check: CPU profile

`lab/diag/leader-cpu.yaml` runs a three-replica Raft cluster with the
profiling hook enabled (`PPROF_DIR`, `PPROF_SECONDS` read by
`adapters/raft`; the hook is inert unless the variable is set, so recorded
runs are unaffected).

```
mkdir -p /tmp/diag-cpu/profiles /tmp/diag-cpu/out
docker compose -f lab/diag/leader-cpu.yaml up --abort-on-container-exit --exit-code-from client
go tool pprof -top -nodecount=10 /tmp/diag-cpu/profiles/cpu-replica1-*.pprof   # leader
go tool pprof -top -nodecount=10 /tmp/diag-cpu/profiles/cpu-replica0-*.pprof   # follower
```

One run (leader was `replica1`, confirmed by its own Raft log):

- leader: 10.17 s of CPU samples over a 22 s window (46.2 % of one core)
- follower: 4.52 s (20.5 %), i.e. a ratio of 2.3, matching the cgroup figure
- both roles have the same profile shape: `Syscall6` 30--35 %, `futex`
  16--18 %, timer functions ~13 %, the rest spread over transport and
  state-machine work. Both spend about two thirds of their samples in system
  calls and lock waits on the persistence path; the leader simply performs
  more of that work.

The instrumentation does not separate persistence, application and transport
cost, so the paper states the direction and the growth with cluster size, not
a per-stage split.
