package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"conslab/internal/labcfg"
)

// ---- run ----

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	rep := fs.Int("rep", 0, "single repetition to run (0 = all configured repetitions)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: runner run <config.json> [--rep N]")
	}
	cfg, err := labcfg.Load(rest[0])
	if err != nil {
		return err
	}
	results := runConfigs([]labcfg.Run{cfg}, *rep)
	return summarize(results)
}

// ---- matrix ----

// MatrixSpec is the compact description of the full experiment matrix.
type MatrixSpec struct {
	Defaults labcfg.Run     `json:"defaults"`
	Workload *WorkloadSpec  `json:"workload"`
	Scaling  *ScalingSpec   `json:"scaling"`
	Failure  *FailureMatrix `json:"failure"`
}

type WorkloadSpec struct {
	ReadPcts      []int    `json:"read_pcts"`
	Concurrencies []int    `json:"concurrencies"`
	Protocols     []string `json:"protocols"`
	Replicas      int      `json:"replicas"`
}

type ScalingSpec struct {
	Replicas    []int    `json:"replicas"`
	WritePct    int      `json:"write_pct"`
	Concurrency int      `json:"concurrency"`
	Protocols   []string `json:"protocols"`
}

type FailureMatrix struct {
	Cases []FailureCase `json:"cases"`
}

type FailureCase struct {
	Protocol      string             `json:"protocol"`
	Mode          labcfg.FailureMode `json:"mode"`
	Replicas      int                `json:"replicas"`
	WritePct      int                `json:"write_pct"`
	Concurrency   int                `json:"concurrency"`
	AtS           float64            `json:"at_s"`
	RestartAfterS float64            `json:"restart_after_s"`
}

func cmdMatrix(args []string) error {
	fs := flag.NewFlagSet("matrix", flag.ContinueOnError)
	configPath := fs.String("config", "configs/matrix.json", "matrix config")
	only := fs.String("only", "", "run only this experiment family (workload|scaling|failure)")
	limit := fs.Int("limit", 0, "limit number of runs (0 = no limit)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := loadMatrix(*configPath)
	if err != nil {
		return err
	}
	configs := expandMatrix(spec, *only)
	if *limit > 0 && len(configs) > *limit {
		configs = configs[:*limit]
	}
	fmt.Printf("matrix: %d run configurations\n", len(configs))
	results := runConfigs(configs, 0)
	return summarize(results)
}

func loadMatrix(path string) (MatrixSpec, error) {
	spec := MatrixSpec{Defaults: labcfg.Default()}
	data, err := os.ReadFile(path)
	if err != nil {
		return spec, err
	}
	// Merge defaults: start from Default(), then overlay the file's defaults.
	if err := json.Unmarshal(data, &spec); err != nil {
		return spec, err
	}
	return spec, nil
}

// expandMatrix turns the compact spec into a flat list of run configurations.
func expandMatrix(spec MatrixSpec, only string) []labcfg.Run {
	var out []labcfg.Run
	base := spec.Defaults

	if spec.Workload != nil && (only == "" || only == "workload") {
		for _, proto := range spec.Workload.Protocols {
			for _, rp := range spec.Workload.ReadPcts {
				for _, c := range spec.Workload.Concurrencies {
					cfg := base
					cfg.Protocol = proto
					cfg.Replicas = spec.Workload.Replicas
					cfg.ReadPct = rp
					cfg.WritePct = 100 - rp
					cfg.Concurrency = c
					cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
					out = append(out, cfg)
				}
			}
		}
	}
	if spec.Scaling != nil && (only == "" || only == "scaling") {
		for _, proto := range spec.Scaling.Protocols {
			for _, n := range spec.Scaling.Replicas {
				cfg := base
				cfg.Protocol = proto
				cfg.Replicas = n
				cfg.WritePct = spec.Scaling.WritePct
				cfg.ReadPct = 100 - spec.Scaling.WritePct
				cfg.Concurrency = spec.Scaling.Concurrency
				cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
				out = append(out, cfg)
			}
		}
	}
	if spec.Failure != nil && (only == "" || only == "failure") {
		for _, c := range spec.Failure.Cases {
			cfg := base
			cfg.Protocol = c.Protocol
			cfg.Replicas = c.Replicas
			cfg.WritePct = c.WritePct
			cfg.ReadPct = 100 - c.WritePct
			cfg.Concurrency = c.Concurrency
			cfg.Failure = labcfg.Failure{Mode: c.Mode, AtS: c.AtS, RestartAfterS: c.RestartAfterS}
			out = append(out, cfg)
		}
	}
	return out
}

// runConfigs executes a list of configurations, applying the given repetition
// selection (0 = run every configured repetition).
func runConfigs(configs []labcfg.Run, singleRep int) []runResult {
	var results []runResult
	total := 0
	for _, cfg := range configs {
		reps := cfg.Repetitions
		if singleRep > 0 {
			reps = 1
		}
		total += reps
	}
	done := 0
	for _, cfg := range configs {
		reps := cfg.Repetitions
		startRep := 1
		if singleRep > 0 {
			reps = singleRep
			startRep = singleRep
		}
		for rep := startRep; rep < startRep+reps; rep++ {
			done++
			fmt.Printf("\n=== [%d/%d] %s %s replicas=%d w=%d c=%d failure=%s (rep %d) ===\n",
				done, total, cfg.Protocol, experimentName(cfg), cfg.Replicas, cfg.WritePct, cfg.Concurrency, cfg.Failure.Mode, rep)
			start := time.Now()
			res := executeRun(cfg, rep)
			fmt.Printf("[%s] %s (%s) in %.1fs\n", res.RunID, res.Status, res.Reason, time.Since(start).Seconds())
			results = append(results, res)
		}
	}
	return results
}

func summarize(results []runResult) error {
	ok, failed := 0, 0
	for _, r := range results {
		if r.Status == "success" {
			ok++
		} else {
			failed++
		}
	}
	fmt.Printf("\nsummary: %d success, %d failed, %d total\n", ok, failed, len(results))
	for _, r := range results {
		if r.Status != "success" {
			fmt.Printf("  FAILED %s: %s\n", r.RunID, r.Reason)
		}
	}
	writeRunIndex(results)
	if failed > 0 {
		return fmt.Errorf("%d of %d runs failed", failed, len(results))
	}
	return nil
}

// writeRunIndex appends every run outcome to results/run-index.csv so failed
// runs are recorded and never silently dropped.
func writeRunIndex(results []runResult) {
	path := filepath.Join(resultsDir(), "run-index.csv")
	var w *csv.Writer
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fi, _ := f.Stat()
	w = csv.NewWriter(f)
	if fi != nil && fi.Size() == 0 {
		w.Write([]string{"ts", "run_id", "status", "reason", "dir"})
	}
	for _, r := range results {
		w.Write([]string{time.Now().UTC().Format(time.RFC3339), r.RunID, r.Status, r.Reason, r.Dir})
	}
	w.Flush()
}

// ---- smoke ----

func cmdSmoke(args []string) error {
	fs := flag.NewFlagSet("smoke", flag.ContinueOnError)
	configPath := fs.String("config", "configs/smoke.json", "smoke config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}
	var smoke SmokeSpec
	if err := json.Unmarshal(data, &smoke); err != nil {
		return err
	}
	return runSmoke(smoke)
}

type SmokeSpec struct {
	Base     labcfg.Run    `json:"base"`
	Basic    labcfg.Run    `json:"basic"`
	Small    labcfg.Run    `json:"small"`
	Failures []FailureCase `json:"failures"`
}

// applyRunDefaults fills zero-valued fields of dst from src (a default Run).
func applyRunDefaults(dst *labcfg.Run, src labcfg.Run) {
	if dst.Protocol == "" {
		dst.Protocol = src.Protocol
	}
	if dst.Replicas == 0 {
		dst.Replicas = src.Replicas
	}
	if dst.ReadPct == 0 && dst.WritePct == 0 {
		dst.ReadPct = src.ReadPct
		dst.WritePct = src.WritePct
	}
	if dst.Concurrency == 0 {
		dst.Concurrency = src.Concurrency
	}
	if dst.DurationS == 0 {
		dst.DurationS = src.DurationS
	}
	if dst.WarmupS == 0 {
		dst.WarmupS = src.WarmupS
	}
	if dst.Repetitions == 0 {
		dst.Repetitions = src.Repetitions
	}
	if dst.Failure.Mode == "" {
		dst.Failure = src.Failure
	}
	if dst.ReplicaCPUs == 0 {
		dst.ReplicaCPUs = src.ReplicaCPUs
	}
	if dst.ReplicaMemMB == 0 {
		dst.ReplicaMemMB = src.ReplicaMemMB
	}
	if dst.ClientCPUs == 0 {
		dst.ClientCPUs = src.ClientCPUs
	}
	if dst.ClientMemMB == 0 {
		dst.ClientMemMB = src.ClientMemMB
	}
	if dst.MasterCPUs == 0 {
		dst.MasterCPUs = src.MasterCPUs
	}
	if dst.MasterMemMB == 0 {
		dst.MasterMemMB = src.MasterMemMB
	}
	if dst.Keyspace == 0 {
		dst.Keyspace = src.Keyspace
	}
	if dst.TimeoutMS == 0 {
		dst.TimeoutMS = src.TimeoutMS
	}
	if dst.Seed == 0 {
		dst.Seed = src.Seed
	}
	if dst.GOMAXPROCS == 0 {
		dst.GOMAXPROCS = src.GOMAXPROCS
	}
	if dst.RaftHeartbeatMS == 0 {
		dst.RaftHeartbeatMS = src.RaftHeartbeatMS
	}
	if dst.RaftElectionMS == 0 {
		dst.RaftElectionMS = src.RaftElectionMS
	}
}
