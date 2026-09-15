# Third-Party Notices

## efficient/epaxos (vendored under lab/upstream/epaxos/)

- Source: https://github.com/efficient/epaxos
- Pinned commit: `791b115669fca472d3136f6a2eda46c00b3f8251`
- Authors: Iulian Moraru, David G. Andersen (Carnegie Mellon University),
  Michael Kaminsky (Intel Labs)
- License: Apache License, Version 2.0
- Copyright: 2013 Carnegie Mellon University

The upstream source is vendored unmodified, except for a platform port of the
`rdtsc` package to support arm64 builds (see
lab/upstream/epaxos/src/rdtsc/). The upstream LICENSE text is preserved at
lab/upstream/epaxos/LICENSE.

## github.com/hashicorp/raft

- Source: https://github.com/hashicorp/raft
- Pinned version: `v1.7.3`
- License: Mozilla Public License 2.0 (MPL-2.0)

Not vendored; used as a Go module dependency of the lab module `conslab`
(pinned in lab/go.mod / lab/go.sum). Also used:
`github.com/hashicorp/raft-boltdb/v2 v2.3.0` (MPL-2.0) for Raft's log store.

## Modifications to vendored source

The following files under lab/upstream/epaxos/ were modified from upstream to
make the lab buildable on arm64 hosts:

- `src/rdtsc/rdtsc.s` - added `//go:build amd64` build tag (content unchanged)
- `src/rdtsc/rdtsc_arm64.s` - new file: arm64 implementation of `Cputicks`
  using the ARMv8 generic counter (CNTVCT_EL0)

No consensus protocol logic was modified.

## Lab infrastructure deviations from the original spec

- The runtime base image is `debian:bookworm-slim` instead of
  `debian:bullseye-slim`: bullseye is EOL and its arm64 security repository
  returns 404s for `net-tools`/`iproute2`. bookworm provides the same
  packages and is still supported.
- `clientlat` (and `client-ol-lat`) do not compile against the current
  upstream `genericsmrproto.Propose` struct (they reference removed fields).
  The lab builds `master` and `server` from upstream; the benchmark client is
  the lab's own (`lab/client/`).
