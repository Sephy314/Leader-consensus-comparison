#!/usr/bin/env python3
"""Validate the generated HTML benchmark report against the underlying data.

The validator derives expected values from the actual result files and
checks that the HTML report is consistent with them. It does not hardcode
any expected benchmark outcome: if the dataset changes, the validator
computes the new expectations automatically.

Checks:
  1. results/report/index.html exists and is non-empty.
  2. All expected report sections are present.
  3. The executive summary counts (total/successful/failed runs, figure
     count, protocols, replica counts) match the underlying data.
  4. Every <img src> reference resolves to an existing file.
  5. Failed runs from the run index appear in the run explorer.
  6. No required dataset-derived section is empty unexpectedly.

Usage:
    python3 report/validate_html.py [--report results/report/index.html]
"""
import argparse
import csv
import json
import os
import re
import sys


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_json(path):
    with open(path) as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", default="results/report/index.html")
    ap.add_argument("--processed", default="results/processed")
    ap.add_argument("--raw", default="results/raw")
    ap.add_argument("--figures", default="results/figures")
    args = ap.parse_args()

    errors = []

    # 1. HTML exists and non-empty.
    if not os.path.exists(args.report):
        print(f"FAIL: {args.report} does not exist")
        return 1
    size = os.path.getsize(args.report)
    if size == 0:
        print("FAIL: report is empty")
        return 1
    with open(args.report) as f:
        html = f.read()
    print(f"report: {args.report} ({size} bytes)")

    # 2. Expected sections. Sections that depend on data (failure, resources)
    # are only required when the underlying data exists; the generator omits
    # them otherwise.
    for section in ("summary", "config", "workload", "scaling",
                    "figures", "runs", "repro", "limitations"):
        if f'id="{section}"' not in html:
            errors.append(f"missing section #{section}")

    if os.path.exists(os.path.join(args.processed, "failures.json")):
        failures = load_json(os.path.join(args.processed, "failures.json"))
        if failures and 'id="failure"' not in html:
            errors.append("missing section #failure (failure data exists)")
    if os.path.exists(os.path.join(args.processed, "resources.csv")):
        resources = read_csv(os.path.join(args.processed, "resources.csv"))
        if resources and 'id="resources"' not in html:
            errors.append("missing section #resources (resource data exists)")

    # Execution Integrity section is required when the manifest exists.
    manifest_path = os.path.join(os.path.dirname(args.processed), "execution-manifest.json")
    if os.path.exists(manifest_path):
        if 'id="integrity"' not in html:
            errors.append("missing section #integrity (execution manifest exists)")
        with open(manifest_path) as f:
            manifest = json.load(f)
        if "Randomization seed" not in html:
            errors.append("execution integrity section missing the randomization seed")
        if manifest.get("batch_ids") and "Batch IDs" not in html:
            errors.append("execution integrity section does not list the dataset batches")
        if "Excluded from aggregates" not in html:
            errors.append("execution integrity section does not report excluded runs")

    # 3. Summary consistency with underlying data.
    run_index = load_json(os.path.join(args.processed, "run-index.json"))
    total = len(run_index)
    successful = sum(1 for r in run_index if r.get("valid"))
    failed = total - successful

    metrics = read_csv(os.path.join(args.processed, "metrics.csv"))
    protocols = sorted({m["protocol"] for m in metrics if m.get("protocol")})
    replicas = sorted({int(m["replicas"]) for m in metrics if m.get("replicas")})

    figure_count = 0
    if os.path.isdir(args.figures):
        for name in os.listdir(args.figures):
            sub = os.path.join(args.figures, name)
            if os.path.isdir(sub):
                figure_count += len([f for f in os.listdir(sub)
                                     if f.endswith((".png", ".svg"))])

    def html_has(text):
        return text in html

    if '<div class="cards">' not in html:
        errors.append("summary cards not found")
    # Counts appear in the card markup: <div class="num">N</div>
    if not re.search(r'<div class="num">' + str(total) + r'</div>', html):
        errors.append(f"total run count {total} not found in summary")
    if not re.search(r'<div class="num">' + str(successful) + r'</div>', html):
        errors.append(f"successful count {successful} not found in summary")
    if not re.search(r'<div class="num">' + str(failed) + r'</div>', html):
        errors.append(f"failed count {failed} not found in summary")
    if not re.search(r'<div class="num">' + str(figure_count) + r'</div>', html):
        errors.append(f"figure count {figure_count} not found in summary")
    for p in protocols:
        if not re.search(r'<div class="num">[^<]*' + re.escape(p), html):
            errors.append(f"protocol {p} not found in summary")
    for n in replicas:
        if not re.search(r'<div class="num">[^<]*' + str(n), html):
            errors.append(f"replica count {n} not found in summary")

    # 4. Figure references resolve.
    for m in re.finditer(r'<img src="([^"]+)"', html):
        src = m.group(1)
        # src is relative to results/report/.
        resolved = os.path.normpath(os.path.join(os.path.dirname(args.report), src))
        if not os.path.exists(resolved):
            errors.append(f"figure reference does not resolve: {src}")

    # 5. Failed runs appear in the run explorer.
    failed_ids = [r["run_id"] for r in run_index if not r.get("valid")]
    for rid in failed_ids:
        if rid not in html:
            errors.append(f"failed run {rid} not present in report")

    # 6. Required dataset-derived sections not empty.
    if not metrics:
        errors.append("metrics.csv is empty; workload/scaling sections will be empty")
    if not os.path.exists(os.path.join(args.processed, "failures.json")):
        errors.append("failures.json missing; failure section will be empty")

    if errors:
        print("FAIL:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("PASS: report is consistent with the underlying dataset")
    return 0


if __name__ == "__main__":
    sys.exit(main())