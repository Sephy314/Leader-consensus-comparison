# Consensus Lab Makefile (root)
#
# All targets are delegated to lab/Makefile, which is the authoritative
# entry point for the benchmark lab. Run any lab target from the repository
# root:
#
#   make build          build all Go binaries + EPaxos + docker image
#   make smoke          staged smoke tests
#   make run CFG=...    run one experiment
#   make matrix         run the full experiment matrix
#   make report         process raw results + figures + HTML report
#   make html           HTML report from already-processed results
#   make validate       validate the HTML report against the data
#   make open-report    open the HTML report in the default browser
#   make check-upstream verify vendored wire protocol matches upstream
#   make clean-results  delete generated results

.PHONY: help build smoke run matrix report html validate open-report figures \
        check-upstream tidy upstream-hash clean clean-results

help:
	$(MAKE) -C lab help

build:
	$(MAKE) -C lab build

smoke:
	$(MAKE) -C lab smoke

run:
	$(MAKE) -C lab run

matrix:
	$(MAKE) -C lab matrix

report:
	$(MAKE) -C lab report

html:
	$(MAKE) -C lab html

validate:
	$(MAKE) -C lab validate

open-report:
	$(MAKE) -C lab open-report

figures:
	$(MAKE) -C lab figures

check-upstream:
	$(MAKE) -C lab check-upstream

tidy:
	$(MAKE) -C lab tidy

upstream-hash:
	$(MAKE) -C lab upstream-hash

clean:
	$(MAKE) -C lab clean

clean-results:
	$(MAKE) -C lab clean-results