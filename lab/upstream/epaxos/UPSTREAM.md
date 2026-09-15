# Vendored upstream: efficient/epaxos

- Source: https://github.com/efficient/epaxos
- Pinned commit: `791b115669fca472d3136f6a2eda46c00b3f8251`
- License: Apache-2.0, Copyright 2013 Carnegie Mellon University
  (see `LICENSE` in this directory).

This directory is a verbatim checkout of the upstream repository at the
pinned commit, with `.git` removed. The consensus protocol logic is **not**
modified.

## Compatibility patch (arm64 hosts)

The upstream `src/rdtsc/rdtsc.s` is x86 assembly. The lab host is aarch64, so
the following build-environment compatibility changes are applied (no
protocol logic touched):

- `src/rdtsc/rdtsc.s` — added `//go:build amd64` build tag (content unchanged).
- `src/rdtsc/rdtsc_arm64.s` — new file: arm64 implementation of `Cputicks`
  using the ARMv8 generic counter (`CNTVCT_EL0`).

These changes are also documented in the repository root
`THIRD-PARTY-NOTICES.md`.

## What the lab builds from this tree

- `master` — the upstream coordination service (replica registration,
  replica list, leader bookkeeping). Used unchanged for EPaxos.
- `server` — the upstream EPaxos replica (`-e -exec -dreply`). Used
  unchanged; the lab only controls it via command-line flags.

The upstream `client` binary is **not** used; the lab has its own common
benchmark client (`lab/client/`) that speaks the same wire protocol.