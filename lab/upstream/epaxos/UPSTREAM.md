# Vendored upstream: efficient/epaxos

- Source: https://github.com/efficient/epaxos
- Pinned commit: `791b115669fca472d3136f6a2eda46c00b3f8251`
- License: Apache-2.0, Copyright 2013 Carnegie Mellon University
  (see `LICENSE` in this directory).

This directory is a checkout of the upstream repository at the pinned
commit, with `.git` removed. The consensus protocol logic is **not**
modified. Two documented changes are applied: an arm64 build port and a
dependency-set size extension (both described below).

## Compatibility patch (arm64 hosts)

The upstream `src/rdtsc/rdtsc.s` is x86 assembly. The lab host is aarch64, so
the following build-environment compatibility changes are applied (no
protocol logic touched):

- `src/rdtsc/rdtsc.s` — added `//go:build amd64` build tag (content unchanged).
- `src/rdtsc/rdtsc_arm64.s` — new file: arm64 implementation of `Cputicks`
  using the ARMv8 generic counter (`CNTVCT_EL0`).

## Dependency-set size extension (DS = 5 → 9)

Upstream hardcodes the EPaxos dependency-set size to 5 in three places, which
limits a cluster to 5 replicas:

- `src/epaxos/epaxos.go` — `const DS = 5` (and `[DS]int32{-1, ...}` literals,
  plus two `[]int32{-1, -1, -1, -1, -1}` LeaderBookkeeping initializers).
- `src/epaxosproto/epaxosproto.go` — `Deps [5]int32` in the message structs.
- `src/epaxosproto/epaxosprotomarsh.go` — fixed-size buffers, `BinarySize`
  values, and per-index `Deps[i]` marshaling blocks.

The lab extends the dependency-set size to 9 so that 7- and 9-replica
clusters work. This is a **wire-format extension**: every inter-replica
message that carries a dependency set now carries 9 entries instead of 5.
All replicas in a cluster run the same patched binary, so the format is
internally consistent.

No consensus algorithm is changed. The dependency-set representation is
extended; the protocol's phases, quorums, and decision rules are untouched.
The change was applied mechanically (`extend_ds3.py`, kept out of the tree)
and verified: every Marshal/Unmarshal function for the seven dependency-
carrying messages reads/writes 9 entries, and buffer sizes match.

These changes are also documented in the repository root
`THIRD-PARTY-NOTICES.md`.

## What the lab builds from this tree

- `master` — the upstream coordination service (replica registration,
  replica list, leader bookkeeping). Used unchanged for EPaxos.
- `server` — the upstream EPaxos replica (`-e -exec -dreply`). Used
  unchanged; the lab only controls it via command-line flags.

The upstream `client` binary is **not** used; the lab has its own common
benchmark client (`lab/client/`) that speaks the same wire protocol.