// Command runner builds, launches, measures, and tears down experiment runs.
//
// Subcommands:
//
//	build    build all binaries and the lab Docker image
//	run      run one experiment (config file + repetition)
//	matrix   run the full experiment matrix
//	smoke    run the staged smoke tests (build, minimal cluster, requests,
//	         small workload, failure injection, metrics)
//	b	validate run the lightweight methodology validation (randomized-order
//	         invariants) before the full suite
//	manifest rebuild results/execution-manifest.json from the completed runs
//	correctness run the correctness-validation harness (short deterministic
//	         consensus-execution checks per implementation)
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "build":
		err = cmdBuild()
	case "run":
		err = cmdRun(os.Args[2:])
	case "matrix":
		err = cmdMatrix(os.Args[2:])
	case "smoke":
		err = cmdSmoke(os.Args[2:])
	case "validate":
		err = cmdValidate(os.Args[2:])
	case "manifest":
		err = cmdManifest(os.Args[2:])
	case "correctness":
		err = cmdCorrectness(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: runner <command> [args]

commands:
  build
  run     <config.json> [repetition]
  matrix  [--config configs/matrix.json] [--only <experiment>] [--limit <n>]
          [--schedule-seed <n>] [--skip-existing] [--rerun-contaminated]
  smoke   [--config configs/smoke.json]
  validate
  manifest [--schedule-seed <n>] [--root results/raw]
  correctness [--only <impl>] [--requests <n>] [--results-base <dir>]
`)
}
