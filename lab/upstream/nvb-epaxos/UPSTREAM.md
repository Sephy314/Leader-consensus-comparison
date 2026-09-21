# Vendored upstream: nvb/epaxos (EPaxos B)

- Repository: https://github.com/nvb/epaxos
  (formerly `github.com/nvanbenschoten/epaxos`; the source's internal import
  path is still the old name, and GitHub redirects the old path)
- Pinned commit: `425bd36502ecc810ef7f08943896f957673102bc` (2018-03-01,
  "Fix link to epaxos paper in README") — the head of `master`
- No releases or tags exist; the commit above is the latest state of the
  repository
- License: Apache-2.0, Copyright 2017 Nathan VanBenschoten (see `LICENSE`)
- Vendored at `lab/upstream/nvb-epaxos/` with `.git` removed

## What is vendored, and what is not

The upstream repository at the pinned commit is vendored in full **except**
`vendor/` (133 MB), which is listed in the repository `.gitignore`. The library
predates Go modules and pins its dependencies through `dep` (`Gopkg.toml` /
`Gopkg.lock`), which is committed. To restore the dependency tree:

```sh
git clone https://github.com/nvb/epaxos /tmp/nvb-epaxos
git -C /tmp/nvb-epaxos checkout 425bd36502ecc810ef7f08943896f957673102bc
cp -r /tmp/nvb-epaxos/vendor lab/upstream/nvb-epaxos/vendor
```

`runner build` fails loudly with the same instructions if `vendor/` is absent.

The vendored tree is large because the library's dependency-minimisation code
uses CockroachDB's interval package (`vendor/github.com/cockroachdb`, 51 MB)
and its gRPC transport pulls `vendor/golang.org` (30 MB). Neither is published
as a resolvable module for this commit, which is why the adapter is built in
GOPATH mode rather than module mode.

## Build and integration

- `lab/adapters/nvbepaxos/` is the adapter. `runner build` compiles it in
  GOPATH mode (`GO111MODULE=off`) against the vendored tree, producing
  `bin/nvbepaxos-server`. The lab's `internal/proto` and `internal/state`
  packages are symlinked into the temporary GOPATH, so the adapter speaks
  exactly the same wire protocol as every other adapter (verified by
  `make check-upstream`).
- `lab/adapters/nvbepaxos/go.mod` exists only to exclude that package from the
  parent module's `./...` wildcard; it is not built as a module.
- The upstream `demo/` (a Badger-backed key-value server exposing gRPC
  `KVService`) is **not** built or used. The lab supplies its own client
  protocol, storage, and lifecycle, exactly as it does for the other three
  implementations.
- Replica state is kept in memory: `epaxos.Config.Storage` is left nil, so the
  library uses its own `NewMemoryStorage`. This matches EPaxos A, whose upstream
  default is also in-memory.
- Peer transport uses the library's own `transport` package (gRPC) on a
  dedicated port (`-peer-port`, default 6000), because the benchmark client
  protocol needs the client port (7070) for itself. EPaxos A serves peers and
  clients on the same port; this is an integration difference, not a protocol
  difference.
- Tick interval: 5 ms, chosen to match the clock rate of the original EPaxos
  implementation used for EPaxos A (whose fast clock is a 5 ms sleep). The
  library's own demo uses 10 ms. The library's only timer is the slow-path
  grace timer, `slowPathTimout = 2` ticks.

## Protocol correspondence

| Aspect | efficient/epaxos (EPaxos A) | nvb/epaxos (EPaxos B) |
|--------|-----------------------------|------------------------|
| PreAccept / Accept / Commit | yes | yes (`InstanceState_*` states) |
| Dependency tracking | fixed `Deps [DS]` array, `DS = 5` extended to 9 by the lab | `Deps []InstanceID` slice, minimised with a CockroachDB `interval.RangeGroup` |
| Dependency-set size limit | 5 upstream, 9 after the lab's extension | none — slices, no fixed bound |
| Sequence numbers | yes | yes (`SeqNum`, Lamport clock) |
| Execution | dependency-ordered, SCC tie-break by seq | same (`executor`, `SeqNum` tie-break) |
| **Fast-path quorum** | `preAcceptOKs >= N/2` (a bare majority) | `val >= N-1`, where `val` counts the leader (all but one replica) |
| **Slow-path trigger** | `nacks >= N/2` (replicas that disagree send NACKs) | divergence observed at the leader (`differentReplies`), then a full quorum; a 2-tick grace timer delays Accept when the fast path is still possible |
| Recovery | Prepare / TryPreAccept | yes (`prepare.go`); the README states failure recovery is implemented |
| Replica membership | fixed replica set | fixed replica set |
| Batching | none in use (`-exec -dreply`) | not implemented (README) |
| Persistence | in-memory by default | pluggable `Storage`; in-memory default used here |
| Snapshots / compaction | in-memory; no snapshots | command compaction implemented; snapshots **not** implemented (README) |

### The fast-path quorum is not the same, and this matters

`epaxos/epaxos.go` in this library defines:

```go
func (p *epaxos) quorum(val int) bool     { return val > len(p.nodes)/2 }
func (p *epaxos) fastQuorum(val int) bool { return val >= len(p.nodes)-1 }
```

The commented-out alternative in the same function is the optimised EPaxos
fast quorum (`ceil(3N/4)`), which the README lists as **not implemented**. The
consequence is that the fast path requires *every replica except one* to reply,
whereas EPaxos A commits on the fast path with a bare majority:

- 3 replicas: both require 2 replies including the leader — identical
- 5 replicas: EPaxos A needs 3; EPaxos B needs 4
- 7 replicas: EPaxos A needs 4; EPaxos B needs 6

At the replica counts in the scaling experiment (3 and 5) the two EPaxos
implementations therefore do **not** present the same fast-path quorum, and the
difference grows with cluster size. This is reported as a protocol-level
deviation in the paper's threats to validity, not treated as equivalent.

### Other documented differences

- The slow path is entered by different signals (NACK quorum in EPaxos A,
  observed reply divergence plus a grace timer in EPaxos B). Both implementations
  are "EPaxos" in the sense of the PreAccept/Accept/Commit protocol with
  dependency-based ordering, but they do not make identical decisions about when
  to leave the fast path.
- EPaxos B exposes no fast/slow-path counters. The counters reported for EPaxos
  A are instrumentation the lab added to that codebase; the sensitivity analysis
  therefore does not use fast/slow-path ratios.
- EPaxos B does not implement membership changes or snapshots. Neither is
  exercised by the sensitivity experiment (fixed replica sets, no
  restart-from-persistence), so they are recorded as limitations rather than
  emulated.

## What the lab builds from this tree

`bin/nvbepaxos-server` — the adapter. The upstream `demo`, `epaxos_test.go`
network tests, and the library's own `Makefile` are not used.
