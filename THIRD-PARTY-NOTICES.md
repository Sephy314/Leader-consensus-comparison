# Third-Party Notices

## efficient/epaxos (vendored under lab/vendor/epaxos/)

- Source: https://github.com/efficient/epaxos
- Authors: Iulian Moraru, David G. Andersen (Carnegie Mellon University),
  Michael Kaminsky (Intel Labs)
- License: Apache License, Version 2.0
- Copyright: 2013 Carnegie Mellon University

The upstream source is vendored unmodified (except for a platform port of the
`rdtsc` package to support arm64 builds; see lab/vendor/epaxos/src/rdtsc/).

The upstream LICENSE text is preserved at lab/vendor/epaxos/LICENSE.

## Modifications to vendored source

The following files under lab/vendor/epaxos/ were modified from upstream to
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
  The lab builds `master`, `server`, and `client` only.
