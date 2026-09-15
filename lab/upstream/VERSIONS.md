# Upstream Dependencies & Versions

This file records the exact upstream versions used by the lab. The benchmark
is only reproducible from a known repository state; every component below is
pinned.

## EPaxos — `efficient/epaxos`

- Repository: https://github.com/efficient/epaxos
- Pinned commit: `791b115669fca472d3136f6a2eda46c00b3f8251`
  ("Fixed 3-replica execution bug that caused crash.")
- Vendored at: `lab/upstream/epaxos/` (`.git` removed; see
  `lab/upstream/epaxos/UPSTREAM.md` for provenance and the documented arm64
  compatibility patch).
- License: Apache-2.0 (Copyright 2013 Carnegie Mellon University).

## Raft — `github.com/hashicorp/raft`

- Repository: https://github.com/hashicorp/raft
- Pinned version: `v1.7.3` (released 2025-03-20)
- Pinned in: `lab/go.mod` / `lab/go.sum` (module `conslab`).
- License: MPL-2.0 (Mozilla Public License 2.0).

## Toolchain

| Component | Version | How it is pinned |
|-----------|---------|------------------|
| Go | 1.26.x (builder image `golang:1.26-bookworm`) | `lab/docker/Dockerfile.*` |
| Docker | 29.7.1 (host) | recorded in `lab/results/raw/*/metadata.json` |
| Docker Compose | v5.3.1 (host) | recorded in `lab/results/raw/*/metadata.json` |
| Python | 3.14 (host, runner/report only) | `lab/runner/`, `lab/report/` |

## Lab code

- The lab's own Go module (`conslab`) is versioned by the repository commit
  that contains it. Record the repository commit in every run's
  `metadata.json` (the runner does this automatically via `git rev-parse`).

## Wire protocol provenance

The client and the Raft adapter speak the upstream EPaxos `genericsmr`
wire protocol. The protocol structs and their binary marshaling are copied
verbatim from the pinned upstream commit into `lab/internal/proto/` (see the
header of each file). `make check-upstream` verifies the copies still match
the vendored upstream source.