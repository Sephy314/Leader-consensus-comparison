# Notes

Working notes for the Consensus Lab. Add dated entries as the lab evolves.

## 2026-09-14 - Initial lab construction

- Vendored `efficient/epaxos` (Apache-2.0, CMU 2013) under `lab/vendor/epaxos/`.
- Host is aarch64; upstream `rdtsc.s` is x86 assembly. Added build tags and an
  arm64 implementation of `Cputicks` (CNTVCT_EL0). No protocol logic touched.
- `clientlat` and `client-ol-lat` do not compile against the current upstream
  `genericsmrproto.Propose` struct (they reference removed fields). The lab
  uses the maintained `client` binary; `clientlat` is skipped in the build.
- Docker image builds `master`, `server`, `client`.
- Experiments: scaling, workload, conflict, concurrency, failure.
- Report generator produces a self-contained `results/report.html`.