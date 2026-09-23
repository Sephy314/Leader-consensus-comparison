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

.PHONY: help build image smoke run matrix rerun-contaminated manifest report html open-report validate test figures paper-figures paper-numbers compare-datasets sensitivity sensitivity-report check-upstream tidy upstream-hash clean clean-results

help:
	@echo "targets:"
	@echo "  build           build all Go binaries + EPaxos + docker image"
	@echo "  smoke           staged smoke tests (build, clusters, requests, workload, failures, metrics)"
	@echo "  run             run one experiment:  make run CFG=configs/smoke.json"
	@echo "  matrix          run the full experiment matrix (hours); resumes an interrupted suite"
	@echo "  rerun-contaminated  re-run the attempts flagged contaminated by host anomalies"
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

# Copy the publication figures into the paper's figures/ directory so the
# LaTeX build can include them.
paper-figures:
	mkdir -p ../paper/figures
	cp results/figures/scaling/throughput-scaling.pdf ../paper/figures/
	cp results/figures/scaling/latency-replica-scaling.pdf ../paper/figures/
	cp results/figures/workload/throughput-concurrency.pdf ../paper/figures/
	cp results/figures/workload/latency-concurrency.pdf ../paper/figures/
	cp results/figures/workload/throughput-write-ratio.pdf ../paper/figures/
	cp results/figures/conflict/throughput-conflict-ratio.pdf ../paper/figures/
	cp results/figures/resources/resource-utilisation.pdf ../paper/figures/
	cp results/figures/resources/per-replica-cpu.pdf ../paper/figures/
	cp results/figures/failure/failure-recovery.pdf ../paper/figures/
	cp results/figures/election/election-failure.pdf ../paper/figures/
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