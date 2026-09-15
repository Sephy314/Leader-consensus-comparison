# Raft upstream: github.com/hashicorp/raft

- Repository: https://github.com/hashicorp/raft
- Pinned version: `v1.7.3` (released 2025-03-20)
- License: MPL-2.0

The lab does **not** vendor the HashiCorp Raft source. It is a Go module
dependency of the lab module `conslab` (see `lab/go.mod` / `lab/go.sum`),
pinned to the exact version above. `go.sum` pins the module hashes.

HashiCorp Raft performs all consensus operations used by the lab:

- leader election (`raft.Raft.Leader()`, `raft.Raft.LeaderCh()`)
- log replication (`raft.Raft.Apply`)
- quorum calculation, commit logic, term management, state transitions

The lab's Raft adapter (`lab/adapters/raft/`) only:

1. accepts benchmark client requests on the upstream `genericsmr` wire
   protocol,
2. converts them into state-machine commands,
3. calls `raft.Raft.Apply`,
4. waits for the resulting future,
5. returns the benchmark response.

It also reports the leader chosen by HashiCorp Raft to the lab's
coordination service (`lab/master/raft/`). It never elects or selects a
leader itself.