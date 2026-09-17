# Consensus Lab Makefile
#
# Targets:
#   build          build the Docker image
#   up-paxos N=5   start a 5-node Classic Paxos cluster
#   up-epaxos N=5  start a 5-node EPaxos cluster
#   down           remove the active Compose deployment
#   exp-scaling    run the scaling experiment
#   exp-workload   run the workload experiment
#   exp-conflict   run the conflict experiment
#   exp-failure    run the failure experiment
#   exp-all        run all experiments sequentially
#   report         generate results/report.html
#   paper          build the ACM-format paper (paper/main.pdf)
#   clean          remove generated compose files and experiment results

LAB_DIR      := lab
SCRIPTS      := $(LAB_DIR)/scripts
COMPOSE_DIR  := $(LAB_DIR)/compose/generated
RESULTS_DIR  := $(LAB_DIR)/results
PY           := $(LAB_DIR)/.venv/bin/python
PAPER_DIR    := paper
N            ?= 5
Q            ?= 10000
REPS         ?= 3

.PHONY: build up-paxos up-epaxos down \
        exp-scaling exp-workload exp-conflict exp-failure exp-all \
        report paper clean

build:
	docker build -f $(LAB_DIR)/docker/Dockerfile -t conslab:latest .

up-paxos:
	$(PY) $(SCRIPTS)/gen_compose.py --mode paxos --n $(N) --out $(COMPOSE_DIR)/paxos-$(N).yml
	docker compose -f $(COMPOSE_DIR)/paxos-$(N).yml up -d --wait

up-epaxos:
	$(PY) $(SCRIPTS)/gen_compose.py --mode epaxos --n $(N) --out $(COMPOSE_DIR)/epaxos-$(N).yml
	docker compose -f $(COMPOSE_DIR)/epaxos-$(N).yml up -d --wait

down:
	-docker compose -f $(COMPOSE_DIR)/paxos-$(N).yml down -v 2>/dev/null || true
	-docker compose -f $(COMPOSE_DIR)/epaxos-$(N).yml down -v 2>/dev/null || true

exp-scaling:
	$(PY) $(SCRIPTS)/orchestrate.py scaling --q $(Q) --reps $(REPS)

exp-workload:
	$(PY) $(SCRIPTS)/orchestrate.py workload --n $(N) --q $(Q) --reps $(REPS)

exp-conflict:
	$(PY) $(SCRIPTS)/orchestrate.py conflict --n $(N) --q $(Q) --reps $(REPS)

exp-failure:
	$(PY) $(SCRIPTS)/failure_test.py --mode paxos --n $(N) --target leader --reps 1
	$(PY) $(SCRIPTS)/failure_test.py --mode epaxos --n $(N) --target random --reps 1

exp-all: exp-scaling exp-workload exp-conflict exp-failure

report:
	$(PY) $(SCRIPTS)/report.py

# ACM-format paper build (acmart, sigconf).
# Requires: pdflatex, bibtex, acmart.cls (texlive-publishers on Debian/Ubuntu).
paper:
	cd $(PAPER_DIR) && pdflatex -interaction=nonstopmode main.tex
	cd $(PAPER_DIR) && bibtex main || true
	cd $(PAPER_DIR) && pdflatex -interaction=nonstopmode main.tex
	cd $(PAPER_DIR) && pdflatex -interaction=nonstopmode main.tex
	@echo "Paper built: $(PAPER_DIR)/main.pdf"

clean:
	rm -rf $(COMPOSE_DIR) $(RESULTS_DIR)
	@echo "Removed generated compose files and experiment results."
	@echo "Vendored upstream source under $(LAB_DIR)/vendor/ was kept."