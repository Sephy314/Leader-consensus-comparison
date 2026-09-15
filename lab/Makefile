# Lab build & experiment entry points.
#
# Everything runs from this directory. Binaries land in bin/, the runtime
# image is tagged conslab:lab, and all measurements land under results/raw/.

GO      ?= go
IMAGE   ?= conslab:lab
RUNNER  := ./bin/runner
PY      ?= .venv/bin/python

.PHONY: help build image smoke run matrix report figures check-upstream tidy upstream-hash clean clean-results

help:
	@echo "targets:"
	@echo "  build           build all Go binaries + EPaxos + docker image"
	@echo "  smoke           staged smoke tests (build, clusters, requests, workload, failures, metrics)"
	@echo "  run             run one experiment:  make run CFG=configs/smoke.json"
	@echo "  matrix          run the full experiment matrix (hours)"
	@echo "  report          process raw results + generate figures"
	@echo "  figures         generate figures only"
	@echo "  check-upstream  verify vendored wire protocol matches upstream"
	@echo "  clean-results   delete results/raw, processed and figures"

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
	$(RUNNER) matrix --config configs/matrix.json

report:
	$(PY) report/process.py --raw results/raw --processed results/processed --figures results/figures

figures:
	$(PY) report/figures.py --processed results/processed --figures results/figures

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
	rm -rf results/raw/* results/processed/* results/figures/* results/run-index.csv