# Lab build & experiment entry points.
#
# Everything runs from this directory. Binaries land in bin/, the runtime
# image is tagged conslab:lab, and all measurements land under results/raw/.

GO      ?= go
IMAGE   ?= conslab:lab
RUNNER  := ./bin/runner
PY      ?= .venv/bin/python

# Execution-schedule seed: recorded in every run's metadata and in the
# execution manifest, and the reason a resumed suite replays the same order.
SEED    ?= 20260919

.PHONY: help build image smoke run matrix rerun-contaminated manifest report html open-report validate test figures paper-figures paper-numbers compare-datasets sensitivity sensitivity-report correctness check-upstream tidy upstream-hash clean clean-results persistence persistence-dry-run network-delay network-delay-dry-run conflict-validation conflict-validation-dry-run persistence-report network-delay-report conflict-validation-report scaling-conc scaling-conc-dry-run scaling-conc-report rerun-family failure-configuration resource-contention

help:
	@echo "targets:"
	@echo "  build           build all Go binaries + EPaxos + docker image"
	@echo "  smoke           staged smoke tests (build, clusters, requests, workload, failures, metrics)"
	@echo "  run             run one experiment:  make run CFG=configs/smoke.json"
	@echo "  matrix          run the full experiment matrix (hours); resumes an interrupted suite"
	@echo "  rerun-contaminated  re-run the attempts flagged contaminated by host anomalies"
	@echo "  rerun-stalled   re-run the unflagged stalled repetitions listed by report/stalled_runs.py"
	@echo "  rerun-family    clean one family: process, then replace flagged attempts and unflagged stalls"
	@echo "  manifest        rebuild results/execution-manifest.json from the completed runs"
	@echo "  validate        methodology validation (randomized-order invariants) before the full suite"
	@echo "  report          process raw results + generate figures + HTML report + paper figures"
	@echo "  html            generate the HTML report from already-processed results"
	@echo "  validate-html   validate the HTML report against the underlying data"
	@echo "  test            run Go unit tests + Python pipeline tests"
	@echo "  open-report     open the HTML report in the default browser (optional)"
	@echo "  figures         generate figures only"
	@echo "  paper-figures   copy publication figures into ../paper/figures/"
	@echo "  paper-numbers   print every number the paper's Evaluation section cites"
	@echo "  compare-datasets  pre-audit vs revised dataset methodology-change report"
	@echo "  sensitivity      run the implementation-sensitivity experiment (4 implementations)"
	@echo "  sensitivity-report  analyse the sensitivity dataset into a markdown report"
	@echo "  persistence      run the persistence-matching experiment (durable vs memory, 4 impls)"
	@echo "  network-delay    run the OS-level (tc/netem) network-delay experiment, no jitter"
	@echo "  conflict-validation  re-run the conflict sweep and measure realized contention"
	@echo "  <family>-dry-run validate a family's config and print its run IDs without running"
	@echo "  resource-contention  report host resource contention for the large replica counts"
	@echo "  failure-configuration  document the failure experiment's configured detection budget"
	@echo "  correctness     run the correctness-validation harness (deterministic consensus checks)"
	@echo "  check-upstream  verify vendored wire protocol matches upstream"
	@echo "  clean-results   delete results/raw, processed, figures and report"

build:
	$(GO) build -o $(RUNNER) ./runner
	$(RUNNER) build

smoke: build
	$(RUNNER) smoke --config configs/smoke.json

CFG ?= configs/smoke.json
REP ?= 0
run:
	$(RUNNER) run --rep $(REP) $(CFG)

matrix:
	$(RUNNER) matrix --config configs/matrix.json --schedule-seed $(SEED) --skip-existing

# Replacement runs: re-run every attempt that host telemetry flagged
# contaminated (or that failed) so each condition keeps its full set of
# repetitions. The originals are kept and excluded by the report.
rerun-contaminated:
	$(RUNNER) matrix --rerun-contaminated --schedule-seed $(SEED)

# The execution manifest is derived from the completed runs; regenerate it
# without running anything.
manifest:
	$(RUNNER) manifest --schedule-seed $(SEED)

# Methodology validation: a lightweight randomized-order experiment that
# verifies the invariants the full benchmark depends on (blocked
# randomization, isolation, teardown, persistence, metadata). Run this
# before the full suite.
validate:
	$(RUNNER) validate

# Correctness validation: short deterministic consensus-execution checks per
# implementation (sequential/concurrent/fault scenarios), independent of the
# performance benchmark. Results land in results/correctness/.
correctness: build
	$(RUNNER) correctness

# Full pipeline: raw -> processed -> figures -> HTML report -> validation
# -> analysis extras -> paper figures.
report: manifest
	$(PY) report/process.py --raw results/raw --processed results/processed
	$(PY) report/figures.py --processed results/processed --figures results/figures --raw results/raw --manifest results/execution-manifest.json
	$(PY) report/html.py --processed results/processed --raw results/raw --figures results/figures --config configs/matrix.json --out results/report
	$(PY) report/validate_html.py --report results/report/index.html --processed results/processed --raw results/raw --figures results/figures
	$(MAKE) compare-datasets
	$(MAKE) paper-figures

# HTML report from already-processed results (no reprocessing).
html:
	$(PY) report/html.py --processed results/processed --raw results/raw --figures results/figures --config configs/matrix.json --out results/report
	$(PY) report/validate_html.py --report results/report/index.html --processed results/processed --raw results/raw --figures results/figures

validate-html:
	$(PY) report/validate_html.py --report results/report/index.html --processed results/processed --raw results/raw --figures results/figures

# Go unit tests + Python pipeline tests.
test:
	$(GO) test ./...
	$(PY) -m unittest report.test_pipeline

# Optional: open the report in the default browser (portable on macOS/Linux).
open-report:
	@test -f results/report/index.html || (echo "report not found; run 'make report' first" && exit 1)
	@(command -v xdg-open >/dev/null 2>&1 && xdg-open results/report/index.html) || \
	 (command -v open >/dev/null 2>&1 && open results/report/index.html) || \
	 echo "no browser opener found; open results/report/index.html manually"

figures:
	$(PY) report/figures.py --processed results/processed --figures results/figures --raw results/raw --manifest results/execution-manifest.json

# Mechanical extraction of every number cited in the paper's Evaluation
# section, straight from the processed data. Tee'd to a file so the LaTeX
# edits can be cross-checked without re-running.
paper-numbers:
	$(PY) report/paper_numbers.py --processed results/processed | tee results/paper-numbers.txt

# Pre-audit vs revised dataset: what the protocol change actually moved.
compare-datasets:
	$(PY) report/compare_datasets.py --old results/archive/pre-audit-2026-09-19/processed --new results/processed --out results/methodology-change-report.md

# Implementation sensitivity: an independent implementation of each protocol
# (etcd Raft beside HashiCorp Raft, nvb/epaxos beside efficient/epaxos). The
# dataset lives under results/sensitivity/ and never touches the primary one.
sensitivity: build
	$(RUNNER) matrix --config configs/sensitivity.json --results-base sensitivity --schedule-seed $(SEED) --skip-existing
	$(MAKE) sensitivity-report

# Analyse the sensitivity dataset with the primary pipeline's definitions.
sensitivity-report:
	$(PY) report/sensitivity.py --raw results/sensitivity/raw \
		--primary-processed results/processed \
		--out results/sensitivity/sensitivity-report.md \
		--json results/sensitivity/sensitivity.json | tee results/sensitivity/paper-numbers.txt

# --- Persistence matching, network delay, conflict validation ---
#
# Three experiments added on top of the recorded suite. Each writes to its own
# results subtree; the recorded dataset under results/ is never read or
# written by them, and each is reported with its own pipeline run so the
# families are never pooled.
#
# Every target has a -dry-run companion that expands the configuration,
# validates every run, prints the run IDs and checks for collisions without
# executing anything. Use it before committing hours to a suite.

# Persistence matching: hold workload, replica count, concurrency and network
# constant and vary only how each implementation persists consensus state, so a
# leader-based/leaderless difference can be checked against a persistence-
# matched comparison. Modes are the implementations' own; nvb has no durable
# mode and that cell is reported as a gap, not filled.
persistence: build
	$(RUNNER) matrix --config configs/persistence.json --results-base persistence_control --schedule-seed $(SEED) --skip-existing

persistence-dry-run: build
	$(RUNNER) matrix --config configs/persistence.json --results-base persistence_control --dry-run

# Network delay: applied at the OS level by the runner (tc/netem inside each
# replica's network namespace), identically for every implementation. No
# adapter sleeps, no jitter. Requires Docker containers to be allowed
# NET_ADMIN, which the rendered compose file adds for this family only.
network-delay: build
	$(RUNNER) matrix --config configs/networkdelay.json --results-base network_delay_clean --schedule-seed $(SEED) --skip-existing

network-delay-dry-run: build
	$(RUNNER) matrix --config configs/networkdelay.json --results-base network_delay_clean --dry-run

# Conflict validation: does the configured conflict fraction actually create
# protocol-level contention? Sweeps the hot-key fraction and the number of
# distinct hot keys, and reports the REALIZED hot fraction plus the EPaxos
# path/conflict counters where the implementation provides them.
conflict-validation: build
	$(RUNNER) matrix --config configs/conflictvalidation.json --results-base conflict_validation --schedule-seed $(SEED) --skip-existing

conflict-validation-dry-run: build
	$(RUNNER) matrix --config configs/conflictvalidation.json --results-base conflict_validation --dry-run

# Raft-only high-concurrency scaling: does the replica-scaling penalty at
# c32 persist at c128/c256? 4 replica counts x 2 concurrency levels x 5
# repetitions = 40 runs, under results/scaling-conc/. The --reps 5 override
# is explicit: the runner's default minimum is 10 repetitions, and the
# config alone cannot under-sample.
scaling-conc: build
	$(RUNNER) matrix --config configs/scaling-conc.json --results-base scaling-conc --reps 5 --schedule-seed $(SEED) --skip-existing

scaling-conc-dry-run: build
	$(RUNNER) matrix --config configs/scaling-conc.json --results-base scaling-conc --reps 5 --dry-run

scaling-conc-report:
	$(PY) report/process.py --raw results/scaling-conc/raw --processed results/scaling-conc/processed

# Report for the three families above, each from its own raw subtree.
persistence-report:
	$(PY) report/process.py --raw results/persistence_control/raw --processed results/persistence_control/processed
	$(PY) report/figures.py --processed results/persistence_control/processed --figures results/persistence_control/figures --raw results/persistence_control/raw --manifest results/persistence_control/execution-manifest.json

network-delay-report:
	$(PY) report/process.py --raw results/network_delay_clean/raw --processed results/network_delay_clean/processed
	$(PY) report/figures.py --processed results/network_delay_clean/processed --figures results/network_delay_clean/figures --raw results/network_delay_clean/raw --manifest results/network_delay_clean/execution-manifest.json

conflict-validation-report:
	$(PY) report/process.py --raw results/conflict_validation/raw --processed results/conflict_validation/processed
	$(PY) report/figures.py --processed results/conflict_validation/processed --figures results/conflict_validation/figures --raw results/conflict_validation/raw --manifest results/conflict_validation/execution-manifest.json

# Make one family's dataset clean. Two replacement rules apply, and both keep
# the original attempt in results/<base>/raw:
#
#  1. an attempt that failed, or that host telemetry flagged contaminated
#     (--rerun-contaminated);
#  2. a repetition that stalled without being flagged: it completes at a small
#     fraction of its configuration's median with no failed request and below
#     every host-telemetry threshold, so rule 1 cannot see it. Those are listed
#     by a rule declared in report/stalled_runs.py (included repetitions below
#     STALL_BELOW times their own condition's median) and re-run through the
#     same mechanism.
#
# The list is derived from the processed data, so the dataset is processed
# first (stalled_runs.py refuses a stale list: it would name an attempt that is
# no longer included). The replacement is a new attempt recording rerun_of and
# rerun_reason, and report/process.py:mark_included keeps the highest attempt
# whatever its value, so nothing is chosen on its outcome.
BASE ?= conflict_validation
STALL_BELOW ?= 0.7
rerun-family: build
	$(PY) report/process.py --raw results/$(BASE)/raw --processed results/$(BASE)/processed
	$(PY) report/stalled_runs.py --processed results/$(BASE)/processed \
		--below $(STALL_BELOW) --out results/$(BASE)/rerun-ids.txt
	$(RUNNER) matrix --rerun-contaminated --results-base $(BASE) --schedule-seed $(SEED)
	$(RUNNER) matrix --rerun-ids results/$(BASE)/rerun-ids.txt \
		--results-base $(BASE) --schedule-seed $(SEED)

# Document the failure experiment's configuration next to its measured
# timings, so no availability gap is quoted without the failure-detection
# parameters it was observed under.
failure-configuration:
	$(PY) report/failure_configuration.py --processed results/processed --raw results/raw --out results/failure-configuration.md

# Host resource contention for the large replica counts, computed from the
# recorded per-replica telemetry. States explicitly whether the measurements
# support reading a configuration as CPU-constrained.
resource-contention:
	$(PY) report/resource_contention.py --processed results/processed --raw results/raw --out results/resource-contention.md

# Copy the publication figures into the paper's figures/ directory so the
# LaTeX build can include them.
paper-figures:
	mkdir -p ../paper/figures
	cp results/figures/scaling/latency-replica-scaling.pdf ../paper/figures/
	cp results/figures/workload/throughput-concurrency.pdf ../paper/figures/
	cp results/figures/workload/latency-concurrency.pdf ../paper/figures/
	cp results/figures/resources/resource-utilisation.pdf ../paper/figures/
	cp results/figures/resources/per-replica-cpu.pdf ../paper/figures/
	cp results/figures/failure/failure-recovery.pdf ../paper/figures/
	cp results/figures/election/election-failure.pdf ../paper/figures/
	# The merged write/conflict ratio figure (report/ratio_merged_figure.py)
	# replaces the two separate null-result figures.
	cp results/figures/throughput-ratios.pdf ../paper/figures/
	cp results/figures/throughput-ratios.png ../paper/figures/
	# The merged scaling figure (EPaxos c32 + Raft c32/c128/c256) replaces the
	# separate throughput-scaling figure; it is generated by
	# report/scaling_conc_figure.py into the scaling-conc subtree.
	cp results/scaling-conc/figures/throughput-scaling-conc.pdf ../paper/figures/
	cp results/scaling-conc/figures/throughput-scaling-conc.png ../paper/figures/
	# Figures of the three families added after the recorded suite. They come
	# from their own subtrees, so the corresponding -report targets must have
	# been run; the copies fail loudly if they have not.
	cp results/persistence_control/figures/persistence/throughput-persistence-matched.pdf \
	   ../paper/figures/persistence-throughput-matched.pdf
	cp results/network_delay_clean/figures/networkdelay/throughput-networkdelay.pdf \
	   ../paper/figures/networkdelay-throughput.pdf
	cp results/conflict_validation/figures/conflictvalidation/conflict-validation.pdf \
	   ../paper/figures/conflict-validation.pdf
	@echo "paper figures copied to ../paper/figures/"

# The wire protocol in internal/proto is copied verbatim from the vendored
# upstream. This target fails loudly if the copies drift.
check-upstream:
	@set -e; \
	tmp=$$(mktemp -d); \
	sed 's/^package proto$$/package genericsmrproto/' internal/proto/proto.go > $$tmp/proto.go; \
	sed 's/^package proto$$/package genericsmrproto/' internal/proto/marsh.go > $$tmp/marsh.go; \
	sed 's/^package proto$$/package masterproto/' internal/proto/master.go > $$tmp/master.go; \
	diff -q $$tmp/proto.go upstream/epaxos/src/genericsmrproto/genericsmrproto.go; \
	diff -q $$tmp/marsh.go upstream/epaxos/src/genericsmrproto/gsmrprotomarsh.go; \
	diff -q $$tmp/master.go upstream/epaxos/src/masterproto/masterproto.go; \
	sed 's/^package state$$/package state/' internal/state/state.go > $$tmp/state.go; \
	diff -q $$tmp/state.go upstream/epaxos/src/state/state.go; \
	rm -rf $$tmp; \
	echo "internal/proto and internal/state match the vendored upstream sources"

tidy:
	$(GO) mod tidy

upstream-hash:
	@find upstream/epaxos/src -type f | sort | xargs sha256sum | sha256sum

clean:
	rm -rf bin
	docker rmi -f $(IMAGE) 2>/dev/null || true

clean-results:
	rm -rf results/raw/* results/processed/* results/figures/* results/report results/run-index.csv