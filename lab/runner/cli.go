package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
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
	results := runConfigs([]labcfg.Run{cfg}, *rep, 0, false)
	return summarize(results)
}

// ---- matrix ----

// MatrixSpec is the compact description of the full experiment matrix.
type MatrixSpec struct {
	Defaults    labcfg.Run     `json:"defaults"`
	Workload    *WorkloadSpec  `json:"workload"`
	Scaling     *ScalingSpec   `json:"scaling"`
	Conflict    *ConflictSpec  `json:"conflict"`
	Concurrency *ConcSpec      `json:"concurrency"`
	Failure     *FailureMatrix `json:"failure"`
	Election    *ElectionSpec  `json:"election"`
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

// ConflictSpec is the conflict-rate sensitivity experiment: vary the
// fraction of requests targeting shared hot keys.
type ConflictSpec struct {
	ConflictPcts []int    `json:"conflict_pcts"`
	Protocols    []string `json:"protocols"`
	Replicas     int      `json:"replicas"`
	WritePct     int      `json:"write_pct"`
	Concurrency  int      `json:"concurrency"`
}

// ConcSpec is the write-concurrency scaling experiment: vary concurrency
// under a write-heavy workload.
type ConcSpec struct {
	Concurrencies []int    `json:"concurrencies"`
	Protocols     []string `json:"protocols"`
	Replicas      int      `json:"replicas"`
	WritePct      int      `json:"write_pct"`
}

type FailureMatrix struct {
	Cases []FailureCase `json:"cases"`
}

// ElectionSpec is the election-failure recovery experiment: kill the Raft
// leader, then induce a target number of failed election attempts by
// isolating the survivors' transport. The actual recovery time is measured.
type ElectionSpec struct {
	Protocol        string  `json:"protocol"`
	Replicas        int     `json:"replicas"`
	WritePct        int     `json:"write_pct"`
	Concurrency     int     `json:"concurrency"`
	AtS             float64 `json:"at_s"`
	DurationS       int     `json:"duration_s"`
	FailedElections []int   `json:"failed_elections"`
}

type FailureCase struct {
	Protocol        string             `json:"protocol"`
	Mode            labcfg.FailureMode `json:"mode"`
	Replicas        int                `json:"replicas"`
	WritePct        int                `json:"write_pct"`
	Concurrency     int                `json:"concurrency"`
	AtS             float64            `json:"at_s"`
	RestartAfterS   float64            `json:"restart_after_s"`
	FailedElections int                `json:"failed_elections"`
}

func cmdMatrix(args []string) error {
	fs := flag.NewFlagSet("matrix", flag.ContinueOnError)
	configPath := fs.String("config", "configs/matrix.json", "matrix config")
	only := fs.String("only", "", "run only this experiment family (workload|scaling|conflict|concurrency|failure|election)")
	limit := fs.Int("limit", 0, "limit number of runs (0 = no limit)")
	scheduleSeed := fs.Int64("schedule-seed", 1, "seed for the blocked-randomization execution schedule (recorded in the manifest)")
	skipExisting := fs.Bool("skip-existing", false, "skip runs that already completed in an earlier batch (resume an interrupted matrix)")
	rerun := fs.Bool("rerun-contaminated", false, "re-run only the attempts flagged contaminated by host anomalies (the originals are kept)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rerun {
		return rerunFlaggedContaminated(*scheduleSeed)
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
	results := runConfigs(configs, 0, *scheduleSeed, *skipExisting)
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
					cfg.Experiment = "workload"
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
				cfg.Experiment = "scaling"
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
	if spec.Conflict != nil && (only == "" || only == "conflict") {
		for _, proto := range spec.Conflict.Protocols {
			for _, pct := range spec.Conflict.ConflictPcts {
				cfg := base
				cfg.Experiment = "conflict"
				cfg.Protocol = proto
				cfg.Replicas = spec.Conflict.Replicas
				cfg.WritePct = spec.Conflict.WritePct
				cfg.ReadPct = 100 - spec.Conflict.WritePct
				cfg.Concurrency = spec.Conflict.Concurrency
				cfg.ConflictPct = pct
				cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
				out = append(out, cfg)
			}
		}
	}
	if spec.Concurrency != nil && (only == "" || only == "concurrency") {
		for _, proto := range spec.Concurrency.Protocols {
			for _, c := range spec.Concurrency.Concurrencies {
				cfg := base
				cfg.Experiment = "concurrency"
				cfg.Protocol = proto
				cfg.Replicas = spec.Concurrency.Replicas
				cfg.WritePct = spec.Concurrency.WritePct
				cfg.ReadPct = 100 - spec.Concurrency.WritePct
				cfg.Concurrency = c
				cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
				out = append(out, cfg)
			}
		}
	}
	if spec.Failure != nil && (only == "" || only == "failure") {
		for _, c := range spec.Failure.Cases {
			cfg := base
			cfg.Experiment = "failure"
			cfg.Protocol = c.Protocol
			cfg.Replicas = c.Replicas
			cfg.WritePct = c.WritePct
			cfg.ReadPct = 100 - c.WritePct
			cfg.Concurrency = c.Concurrency
			cfg.Failure = labcfg.Failure{Mode: c.Mode, AtS: c.AtS, RestartAfterS: c.RestartAfterS, FailedElections: c.FailedElections}
			out = append(out, cfg)
		}
	}
	if spec.Election != nil && (only == "" || only == "election") {
		for _, n := range spec.Election.FailedElections {
			cfg := base
			cfg.Experiment = "election"
			cfg.Protocol = spec.Election.Protocol
			cfg.Replicas = spec.Election.Replicas
			cfg.WritePct = spec.Election.WritePct
			cfg.ReadPct = 100 - spec.Election.WritePct
			cfg.Concurrency = spec.Election.Concurrency
			// The measured phase must be long enough for the longest
			// isolation (FailedElections * election_timeout) to expire and
			// recovery to complete; otherwise the run cannot reach its
			// final state and is correctly recorded as failed.
			if spec.Election.DurationS > 0 {
				cfg.DurationS = spec.Election.DurationS
			}
			cfg.Failure = labcfg.Failure{
				Mode:            labcfg.FailureElection,
				AtS:             spec.Election.AtS,
				FailedElections: n,
			}
			out = append(out, cfg)
		}
	}
	return out
}

// minRepetitions is the minimum number of independent repetitions for a
// measured experiment. Below this the dispersion and trend claims the report
// makes are not statistically defensible. Smoke tests are functional gates
// and are exempt (they do not go through runConfigs).
const minRepetitions = 10

// schedInfo records where a run sits in the blocked-randomization schedule.
// It is recorded in the run's metadata and in the execution manifest so the
// actual execution order can be audited against the intended schedule.
type schedInfo struct {
	block int   // block (repetition pass) this run belongs to
	seq   int   // global sequence index in the schedule
	seed  int64 // schedule seed (recorded for reproducibility)
	// rerunOf is set only for replacement runs created by
	// --rerun-contaminated: the run ID of the attempt being replaced, and
	// why. The original attempt is never deleted or modified.
	rerunOf     string
	rerunReason string
}

// schedItem is one (configuration, repetition) pair in the schedule.
type schedItem struct {
	cfg labcfg.Run
	rep int
}

// buildSchedule produces the blocked-randomization execution order.
//
// Block b contains every (configuration, repetition b) pair exactly once, in
// an order shuffled with a seeded RNG. This guarantees that every condition
// is represented throughout the experiment timeline (no condition is
// concentrated at the start or end) while the exact order is reproducible
// from the recorded schedule seed. Conditions are therefore not confounded
// with wall-clock execution time.
func buildSchedule(configs []labcfg.Run, seed int64) []schedItem {
	rng := rand.New(rand.NewSource(seed))
	byRep := map[int][]labcfg.Run{}
	maxRep := 0
	for _, cfg := range configs {
		for rep := 1; rep <= cfg.Repetitions; rep++ {
			byRep[rep] = append(byRep[rep], cfg)
			if rep > maxRep {
				maxRep = rep
			}
		}
	}
	var out []schedItem
	for rep := 1; rep <= maxRep; rep++ {
		cfgs := byRep[rep]
		rng.Shuffle(len(cfgs), func(i, j int) { cfgs[i], cfgs[j] = cfgs[j], cfgs[i] })
		for _, cfg := range cfgs {
			out = append(out, schedItem{cfg: cfg, rep: rep})
		}
	}
	return out
}

// runConfigs executes a list of configurations, applying the given repetition
// selection (0 = run every configured repetition). Every measured
// configuration must have at least minRepetitions independent repetitions;
// the runner fails loudly rather than silently under-sampling.
//
// Unless a single repetition is requested, the execution order is a
// reproducibly randomized blocked schedule (see buildSchedule): conditions
// are interleaved across the timeline instead of being run in blocks, so the
// treatment variable is not confounded with wall-clock time.
//
// skipExisting is the resume path: a run that already completed in an earlier
// batch is not repeated. Its original schedule position stays recorded in its
// metadata, so the schedule remains reproducible across batches.
func runConfigs(configs []labcfg.Run, singleRep int, scheduleSeed int64, skipExisting bool) []runResult {
	env := measuredEnv()
	fmt.Printf("batch %s\n", env.batchID)
	for _, cfg := range configs {
		if singleRep == 0 && cfg.Repetitions < minRepetitions {
			fmt.Fprintf(os.Stderr, "error: %s requires %d repetitions, got %d (minimum %d); refusing to under-sample\n",
				runIDFor(cfg, 1), cfg.Repetitions, cfg.Repetitions, minRepetitions)
			return []runResult{{RunID: runIDFor(cfg, 1), Status: "failed", Reason: fmt.Sprintf(
				"repetitions %d < minimum %d", cfg.Repetitions, minRepetitions)}}
		}
	}

	var schedule []schedItem
	if singleRep > 0 {
		// A single explicitly requested repetition: no schedule needed.
		for _, cfg := range configs {
			schedule = append(schedule, schedItem{cfg: cfg, rep: singleRep})
		}
	} else {
		schedule = buildSchedule(configs, scheduleSeed)
		fmt.Printf("schedule: %d runs, seed %d, %d blocks\n", len(schedule), scheduleSeed, schedule[0].cfg.Repetitions)
	}

	var results []runResult
	skipped := 0
	for i, item := range schedule {
		cfg, rep := item.cfg, item.rep
		var sched *schedInfo
		if singleRep == 0 {
			sched = &schedInfo{block: rep, seq: i + 1, seed: scheduleSeed}
		}
		if skipExisting && runCompleted(cfg, rep, env.root) {
			skipped++
			continue
		}
		fmt.Printf("\n=== [%d/%d] %s %s replicas=%d w=%d c=%d failure=%s (rep %d) ===\n",
			i+1, len(schedule), cfg.Protocol, experimentName(cfg), cfg.Replicas, cfg.WritePct, cfg.Concurrency, cfg.Failure.Mode, rep)
		start := time.Now()
		res := executeRun(cfg, rep, env, sched)
		fmt.Printf("[%s] %s (%s) in %.1fs\n", res.RunID, res.Status, res.Reason, time.Since(start).Seconds())
		results = append(results, res)
	}
	if skipped > 0 {
		fmt.Printf("resumed: %d of %d scheduled runs already completed and were skipped\n", skipped, len(schedule))
	}
	writeExecutionManifest(env, scheduleSeed)
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
	if dst.RaftSnapshotThr == 0 {
		dst.RaftSnapshotThr = src.RaftSnapshotThr
	}
	if dst.RaftTrailingLogs == 0 {
		dst.RaftTrailingLogs = src.RaftTrailingLogs
	}
	if dst.HotKeys == 0 {
		dst.HotKeys = src.HotKeys
	}
	if dst.ConflictPct == 0 {
		dst.ConflictPct = src.ConflictPct
	}
}
