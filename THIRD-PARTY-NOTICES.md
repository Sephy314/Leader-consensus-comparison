# Third-Party Notices

## efficient/epaxos (vendored under lab/upstream/epaxos/)

- Source: https://github.com/efficient/epaxos
- Pinned commit: `791b115669fca472d3136f6a2eda46c00b3f8251`
- Authors: Iulian Moraru, David G. Andersen (Carnegie Mellon University),
  Michael Kaminsky (Intel Labs)
- License: Apache License, Version 2.0
- Copyright: 2013 Carnegie Mellon University

The upstream source is vendored with two documented modifications: an arm64
port of the `rdtsc` package and a dependency-set size extension. See
lab/upstream/epaxos/UPSTREAM.md. The upstream LICENSE text is preserved at
lab/upstream/epaxos/LICENSE.

## Modifications to vendored source

1. arm64 build port (no protocol logic touched):

- `src/rdtsc/rdtsc.s` - added `//go:build amd64` build tag (content unchanged)
- `src/rdtsc/rdtsc_arm64.s` - new file: arm64 implementation of `Cputicks`
  using the ARMv8 generic counter (CNTVCT_EL0)

2. dependency-set size extension (`DS = 5` -> `9`), a wire-format extension
   that lets clusters larger than 5 replicas run. The protocol's phases,
   quorums, and decision rules are unchanged; all replicas run the same
   patched binary. Touched files:

- `src/epaxos/epaxos.go` - `const DS = 5` -> `9`, and two `[]int32{-1,...}`
  literals extended from 5 to 9 elements
- `src/epaxosproto/epaxosproto.go` - `Deps [5]int32` -> `[9]int32`
- `src/epaxosproto/epaxosprotomarsh.go` - buffer sizes, `BinarySize` values,
  and per-index `Deps[i]` Marshal/Unmarshal blocks extended from 5 to 9

No consensus algorithm was modified.

## github.com/hashicorp/raft

- Source: https://github.com/hashicorp/raft
- Pinned version: `v1.7.3`
- License: Mozilla Public License 2.0 (MPL-2.0)

Not vendored; used as a Go module dependency of the lab module `conslab`
(pinned in lab/go.mod / lab/go.sum). Also used:
`github.com/hashicorp/raft-boltdb/v2 v2.3.0` (MPL-2.0) for Raft's log store.

## Lab infrastructure deviations from the original spec

- The runtime base image is `debian:bookworm-slim` instead of
  `debian:bullseye-slim`: bullseye is EOL and its arm64 security repository
  returns 404s for `net-tools`/`iproute2`. bookworm provides the same
  packages and is still supported.
- `clientlat` (and `client-ol-lat`) do not compile against the current
  upstream `genericsmrproto.Propose` struct (they reference removed fields).
  The lab builds `master` and `server` from upstream; the benchmark client is
  the lab's own (`lab/client/`).
