// Command runner builds, launches, measures, and tears down experiment runs.
//
// Subcommands:
//
//	build   build all binaries and the lab Docker image
//	run     run one experiment (config file + repetition)
//	matrix  run the full experiment matrix
//	smoke   run the staged smoke tests (build, minimal cluster, requests,
//	        small workload, failure injection, metrics)
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
  run    <config.json> [repetition]
  matrix [--config configs/matrix.json] [--only <experiment>] [--limit <n>]
  smoke  [--config configs/smoke.json]
`)
}
