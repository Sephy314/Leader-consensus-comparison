package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"conslab/internal/labcfg"
)

// ---- run ----

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	rep := fs.Int("rep", 0, "single repetition to run (0 = all configured repetitions)")
	base := fs.String("results-base", "", "results subtree to write to (empty = the primary dataset)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	resultsBase = *base
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: runner run [flags] <config.json>\n(flags must precede the config path)")
	}
	// Go's flag package stops parsing at the first non-flag argument, so
	// `runner run config.json --rep 3 --results-base foo` silently ignores both
	// flags: the run uses the default repetition selection AND writes to the
	// primary dataset instead of the intended subtree. That is a silent write
	// into recorded results, so it is an error here.
	if err := rejectTrailingFlags("run", rest); err != nil {
		return err
	}
	cfg, err := labcfg.Load(rest[0])
	if err != nil {
		return err
	}
	results := runConfigs([]labcfg.Run{cfg}, *rep, 0, false, minRepetitions)
	return summarize(results)
}

// rejectTrailingFlags fails when an argument after the positional ones looks
// like a flag, because the flag parser has already stopped and the flag would
// be ignored.
func rejectTrailingFlags(cmd string, positional []string) error {
	for _, a := range positional {
		if strings.HasPrefix(a, "-") {
			return fmt.Errorf(
				"%s: %q came after a positional argument and was ignored; "+
					"put every flag before the positional arguments", cmd, a)
		}
	}
	return nil
}

// ---- matrix ----

// MatrixSpec is the compact description of the full experiment matrix.
type MatrixSpec struct {
	Defaults     labcfg.Run        `json:"defaults"`
	Workload     *WorkloadSpec     `json:"workload"`
	Scaling      *ScalingSpec      `json:"scaling"`
	Conflict     *ConflictSpec     `json:"conflict"`
	Concurrency  *ConcSpec         `json:"concurrency"`
	Failure      *FailureMatrix    `json:"failure"`
	Election     *ElectionSpec     `json:"election"`
	CommCost     *CommCostSpec     `json:"commcost"`
	Persistence  *PersistenceSpec  `json:"persistence"`
	NetworkDelay *NetworkDelaySpec `json:"networkdelay"`
	ConflictVal  *ConflictValSpec  `json:"conflictvalidation"`
	ScalingConc  *ScalingConcSpec  `json:"scalingconc"`
}

type ScalingSpec struct {
	Replicas        []int    `json:"replicas"`
	WritePct        int      `json:"write_pct"`
	Concurrency     int      `json:"concurrency"`
	Protocols       []string `json:"protocols"`
	Implementations []string `json:"implementations"` // optional; empty = each protocol's primary implementation
}

// ScalingConcSpec is the Raft-only high-concurrency scaling follow-up:
// replica count x client concurrency at 100% writes. It exists because the
// recorded scaling family is fixed at concurrency 32 and the recorded
// concurrency family is fixed at 3 replicas, so neither covers the
// interaction. Raft only: the question is whether the replica-scaling
// penalty observed at c32 persists under higher offered concurrency.
type ScalingConcSpec struct {
	Protocols     []string `json:"protocols"`
	Replicas      []int    `json:"replicas"`
	WritePct      int      `json:"write_pct"`
	Concurrencies []int    `json:"concurrencies"`
}

type WorkloadSpec struct {
	ReadPcts        []int    `json:"read_pcts"`
	Concurrencies   []int    `json:"concurrencies"`
	Protocols       []string `json:"protocols"`
	Implementations []string `json:"implementations"` // optional; empty = each protocol's primary implementation
	Replicas        int      `json:"replicas"`
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

// CommCostSpec is the communication-cost experiment: vary the fixed one-way
// latency added to every inter-replica message, and (at one cost level) the
// jitter around it. This simulates a real network instead of the local
// loopback the rest of the benchmark uses.
type CommCostSpec struct {
	Protocols       []string        `json:"protocols"`
	Implementations []string        `json:"implementations"` // optional; empty = each protocol's primary implementation
	Replicas        []int           `json:"replicas"`
	WritePct        int             `json:"write_pct"`
	Concurrency     int             `json:"concurrency"`
	Points          []CommCostPoint `json:"points"`
}

// CommCostPoint is one (cost, jitter) combination. Jitter is a percentage of
// the cost: each message waits cost + U(0, cost*jitter/100).
type CommCostPoint struct {
	CostMS    int `json:"cost_ms"`
	JitterPct int `json:"jitter_pct"`
}

// PersistenceSpec is the persistence-matching experiment: hold the workload,
// replica count, concurrency, durations and repetition count fixed, and vary
// only how each implementation persists consensus state. Combinations an
// implementation does not provide are skipped and reported; no persistence
// mechanism is invented to fill an empty cell.
type PersistenceSpec struct {
	Protocols       []string `json:"protocols"`
	Implementations []string `json:"implementations"`
	Modes           []string `json:"modes"` // durable | memory
	Replicas        []int    `json:"replicas"`
	WritePct        int      `json:"write_pct"`
	Concurrency     int      `json:"concurrency"`
}

// ConflictValSpec re-runs the conflict workload in order to verify whether the
// CONFIGURED conflict fraction actually produced protocol-level contention.
//
// It sweeps two independent variables: the fraction of requests sent to the hot
// key range, and the NUMBER of distinct hot keys that fraction is spread over.
// At the same configured fraction a smaller hot-key set produces longer
// dependency chains, so the pair separates "how often requests hit a hot key"
// from "how much contention that actually creates".
//
// The windowed conflict family (configs/matrix.json) is deliberately left
// untouched: this family has its own name and run-ID encoding so the recorded
// conflict runs keep their identity and are never re-run or merged.
type ConflictValSpec struct {
	Protocols       []string `json:"protocols"`
	Implementations []string `json:"implementations"`
	ConflictPcts    []int    `json:"conflict_pcts"`
	HotKeys         []int    `json:"hot_keys"` // fixed set of shared keys the hot fraction targets
	Replicas        int      `json:"replicas"`
	WritePct        int      `json:"write_pct"`
	Concurrency     int      `json:"concurrency"`
}

// NetworkDelaySpec is the network-delay experiment. The delay is applied by the
// runner at the OS level (tc/netem inside each replica's network namespace),
// identically for every implementation, so no implementation participates in
// producing the condition it is measured under.
type NetworkDelaySpec struct {
	Protocols       []string `json:"protocols"`
	Implementations []string `json:"implementations"`
	Replicas        []int    `json:"replicas"`
	WritePct        int      `json:"write_pct"`
	Concurrency     int      `json:"concurrency"`
	DelaysMS        []int    `json:"delays_ms"`
	EmulationMethod string   `json:"emulation_method"` // default tc-netem
	Interface       string   `json:"interface"`        // default eth0
	Filter          string   `json:"filter"`           // default peers
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
	only := fs.String("only", "", "run only this experiment family (workload|scaling|conflict|concurrency|failure|election|scalingconc)")
	limit := fs.Int("limit", 0, "limit number of runs (0 = no limit)")
	scheduleSeed := fs.Int64("schedule-seed", 1, "seed for the blocked-randomization execution schedule (recorded in the manifest)")
	skipExisting := fs.Bool("skip-existing", false, "skip runs that already completed in an earlier batch (resume an interrupted matrix)")
	rerun := fs.Bool("rerun-contaminated", false, "re-run only the attempts flagged contaminated by host anomalies (the originals are kept)")
	rerunIDs := fs.String("rerun-ids", "", "re-run the completed runs named in this file (one run ID per line, optional tab-separated reason) as new attempts")
	base := fs.String("results-base", "", "results subtree to write to (empty = the primary dataset)")
	dryRun := fs.Bool("dry-run", false, "expand the matrix, print the run IDs and configuration that would be executed, and exit without running anything")
	reps := fs.Int("reps", 0, "override the configured repetitions for this invocation (0 = use the config). Explicit opt-in for follow-up experiments that deliberately sample fewer than the default minimum of 10; the runner refuses to under-sample without it.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	resultsBase = *base
	if *rerun && *rerunIDs != "" {
		return fmt.Errorf("--rerun-contaminated and --rerun-ids are mutually exclusive")
	}
	if *rerunIDs != "" {
		return rerunListed(*rerunIDs, *scheduleSeed)
	}
	if *rerun {
		return rerunFlaggedContaminated(*scheduleSeed)
	}
	spec, err := loadMatrix(*configPath)
	if err != nil {
		return err
	}
	configs := expandMatrix(spec, *only)
	if *dryRun {
		return dryRunMatrix(*configPath, configs)
	}
	if *limit > 0 && len(configs) > *limit {
		configs = configs[:*limit]
	}
	minReps := minRepetitions
	if *reps > 0 {
		// Explicit designer-chosen sample size for a follow-up experiment.
		// The override is loud: the config alone cannot under-sample, and the
		// effective repetition count is recorded in every run's metadata.
		fmt.Printf("matrix: --reps %d overrides the configured repetitions (default minimum %d)\n", *reps, minRepetitions)
		minReps = 1
		for i := range configs {
			configs[i].Repetitions = *reps
		}
	}
	fmt.Printf("matrix: %d run configurations\n", len(configs))
	results := runConfigs(configs, 0, *scheduleSeed, *skipExisting, minReps)
	return summarize(results)
}

// dryRunMatrix prints what a matrix config expands to, without running it. It
// exists so a new family (persistence, network delay, conflict validation) can
// be checked end to end - expansion, validation, the effective storage and
// network settings, and the resulting run IDs - before committing hours to a
// suite. It never writes to results/.
func dryRunMatrix(path string, configs []labcfg.Run) error {
	fmt.Printf("dry run: %s would execute %d run configurations\n", path, len(configs))
	if len(configs) == 0 {
		return fmt.Errorf("the matrix expanded to zero configurations (is the family name correct?)")
	}
	invalid := 0
	seen := map[string]bool{}
	for _, cfg := range configs {
		id := runIDFor(cfg, 1)
		if seen[id] {
			fmt.Printf("  COLLISION %s: this identity is already claimed by another configuration\n", id)
		}
		seen[id] = true
		if err := cfg.Validate(); err != nil {
			fmt.Printf("  INVALID %s: %v\n", id, err)
			invalid++
		}
	}
	counts := map[string]int{}
	for _, cfg := range configs {
		counts[fmt.Sprintf("%s/%s/%s", experimentName(cfg), cfg.Protocol, cfg.Impl())]++
	}
	families := make([]string, 0, len(counts))
	for k := range counts {
		families = append(families, k)
	}
	sort.Strings(families)
	for _, k := range families {
		fmt.Printf("  %-34s %3d configurations x %2d repetitions = %4d runs\n",
			k, counts[k], configs[0].Repetitions, counts[k]*configs[0].Repetitions)
	}
	for _, cfg := range configs {
		fmt.Printf("  %-58s persistence=%-7s delay=%dms filter=%s r=%d c=%d w=%d\n",
			runIDFor(cfg, 1), cfg.Persist(), cfg.NetworkDelayMS, cfg.EmulationFilter(),
			cfg.Replicas, cfg.Concurrency, cfg.WritePct)
	}
	if invalid > 0 {
		return fmt.Errorf("%d expanded configurations are invalid", invalid)
	}
	fmt.Println("dry run: every configuration is valid, no run IDs collide")
	return nil
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
			for _, impl := range implementationsFor(proto, spec.Workload.Implementations) {
				for _, rp := range spec.Workload.ReadPcts {
					for _, c := range spec.Workload.Concurrencies {
						cfg := base
						cfg.Experiment = "workload"
						cfg.Protocol = proto
						cfg.Implementation = impl
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
	}
	if spec.Scaling != nil && (only == "" || only == "scaling") {
		for _, proto := range spec.Scaling.Protocols {
			for _, impl := range implementationsFor(proto, spec.Scaling.Implementations) {
				for _, n := range spec.Scaling.Replicas {
					cfg := base
					cfg.Experiment = "scaling"
					cfg.Protocol = proto
					cfg.Implementation = impl
					cfg.Replicas = n
					cfg.WritePct = spec.Scaling.WritePct
					cfg.ReadPct = 100 - spec.Scaling.WritePct
					cfg.Concurrency = spec.Scaling.Concurrency
					cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
					out = append(out, cfg)
				}
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
	if spec.CommCost != nil && (only == "" || only == "commcost") {
		for _, proto := range spec.CommCost.Protocols {
			for _, impl := range implementationsFor(proto, spec.CommCost.Implementations) {
				for _, n := range spec.CommCost.Replicas {
					for _, pt := range spec.CommCost.Points {
						cfg := base
						cfg.Experiment = "commcost"
						cfg.Protocol = proto
						cfg.Implementation = impl
						cfg.Replicas = n
						cfg.WritePct = spec.CommCost.WritePct
						cfg.ReadPct = 100 - spec.CommCost.WritePct
						cfg.Concurrency = spec.CommCost.Concurrency
						cfg.CommCostMS = pt.CostMS
						cfg.CommJitterPct = pt.JitterPct
						cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
						out = append(out, cfg)
					}
				}
			}
		}
	}

	if spec.Persistence != nil && (only == "" || only == "persistence") {
		for _, proto := range spec.Persistence.Protocols {
			for _, impl := range implementationsFor(proto, spec.Persistence.Implementations) {
				name := impl
				if name == "" {
					name = labcfg.PrimaryImpl(proto)
				}
				for _, mode := range spec.Persistence.Modes {
					if !labcfg.PersistSupported(name, mode) {
						// Only modes the implementation itself provides are expanded.
						// The gap is reported rather than filled with an invented
						// persistence mechanism, and it is visible in the run list.
						fmt.Printf("persistence: skipping %s/%s mode=%s (provides %v)\n",
							proto, name, mode, labcfg.PersistModes(name))
						continue
					}
					for _, n := range spec.Persistence.Replicas {
						cfg := base
						cfg.Experiment = "persistence"
						cfg.Protocol = proto
						cfg.Implementation = impl
						cfg.PersistenceMode = mode
						cfg.Replicas = n
						cfg.WritePct = spec.Persistence.WritePct
						cfg.ReadPct = 100 - spec.Persistence.WritePct
						cfg.Concurrency = spec.Persistence.Concurrency
						cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
						out = append(out, cfg)
					}
				}
			}
		}
	}

	if spec.ConflictVal != nil && (only == "" || only == "conflictvalidation") {
		for _, proto := range spec.ConflictVal.Protocols {
			for _, impl := range implementationsFor(proto, spec.ConflictVal.Implementations) {
				for _, hk := range spec.ConflictVal.HotKeys {
					for _, pct := range spec.ConflictVal.ConflictPcts {
						cfg := base
						cfg.Experiment = "conflictvalidation"
						cfg.Protocol = proto
						cfg.Implementation = impl
						cfg.Replicas = spec.ConflictVal.Replicas
						cfg.WritePct = spec.ConflictVal.WritePct
						cfg.ReadPct = 100 - spec.ConflictVal.WritePct
						cfg.Concurrency = spec.ConflictVal.Concurrency
						cfg.ConflictPct = pct
						cfg.HotKeys = hk
						cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
						out = append(out, cfg)
					}
				}
			}
		}
	}

	if spec.NetworkDelay != nil && (only == "" || only == "networkdelay") {
		for _, proto := range spec.NetworkDelay.Protocols {
			for _, impl := range implementationsFor(proto, spec.NetworkDelay.Implementations) {
				for _, n := range spec.NetworkDelay.Replicas {
					for _, d := range spec.NetworkDelay.DelaysMS {
						cfg := base
						cfg.Experiment = "networkdelay"
						cfg.Protocol = proto
						cfg.Implementation = impl
						cfg.Replicas = n
						cfg.WritePct = spec.NetworkDelay.WritePct
						cfg.ReadPct = 100 - spec.NetworkDelay.WritePct
						cfg.Concurrency = spec.NetworkDelay.Concurrency
						cfg.NetworkDelayMS = d
						cfg.NetworkEmulationMethod = spec.NetworkDelay.EmulationMethod
						cfg.NetworkInterface = spec.NetworkDelay.Interface
						cfg.NetworkFilter = spec.NetworkDelay.Filter
						// This family never uses the legacy in-adapter injection: the two
						// mechanisms must not be confounded in one run (see Validate).
						cfg.CommCostMS = 0
						cfg.CommJitterPct = 0
						cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
						out = append(out, cfg)
					}
				}
			}
		}
	}
	if spec.ScalingConc != nil && (only == "" || only == "scalingconc") {
		for _, proto := range spec.ScalingConc.Protocols {
			for _, n := range spec.ScalingConc.Replicas {
				for _, c := range spec.ScalingConc.Concurrencies {
					cfg := base
					cfg.Experiment = "scaling-conc"
					cfg.Protocol = proto
					cfg.Replicas = n
					cfg.WritePct = spec.ScalingConc.WritePct
					cfg.ReadPct = 100 - spec.ScalingConc.WritePct
					cfg.Concurrency = c
					cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
					out = append(out, cfg)
				}
			}
		}
	}
	return out
}

// implementationsFor returns the implementation names to expand for one
// protocol. An empty spec yields a single empty name, which means "the
// protocol's primary implementation", so every pre-existing matrix config is
// expanded exactly as before.
func implementationsFor(protocol string, impls []string) []string {
	var out []string
	for _, i := range impls {
		if labcfg.ImplSupported(protocol, i) {
			out = append(out, i)
		}
	}
	if len(out) == 0 {
		return []string{""}
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
func runConfigs(configs []labcfg.Run, singleRep int, scheduleSeed int64, skipExisting bool, minReps int) []runResult {
	env := measuredEnv()
	fmt.Printf("batch %s\n", env.batchID)
	for _, cfg := range configs {
		if singleRep == 0 && cfg.Repetitions < minReps {
			fmt.Fprintf(os.Stderr, "error: %s requires %d repetitions, got %d (minimum %d); refusing to under-sample\n",
				runIDFor(cfg, 1), cfg.Repetitions, cfg.Repetitions, minReps)
			return []runResult{{RunID: runIDFor(cfg, 1), Status: "failed", Reason: fmt.Sprintf(
				"repetitions %d < minimum %d", cfg.Repetitions, minReps)}}
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
	// Implementations lists every implementation to smoke-test, across both
	// protocols. Entries that do not apply to a protocol are ignored. Empty
	// means only each protocol's primary implementation, so an older smoke
	// config behaves exactly as before.
	Implementations []string `json:"implementations"`
}

// smokeTarget is one (protocol, implementation) pair the smoke stages
// exercise.
type smokeTarget struct {
	proto string
	impl  string
}

// smokeTargets returns the (protocol, implementation) pairs to exercise.
func (s SmokeSpec) smokeTargets() []smokeTarget {
	var out []smokeTarget
	for _, proto := range []string{"raft", "epaxos"} {
		for _, impl := range implementationsFor(proto, s.Implementations) {
			out = append(out, smokeTarget{proto: proto, impl: impl})
		}
	}
	return out
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
	// The persistence and network-emulation fields are carried explicitly so a
	// `defaults` block in a config file can never be silently dropped for the
	// cases that do not restate them (the failure mode that once dropped
	// hot_keys and conflict_pct).
	if dst.PersistenceMode == "" {
		dst.PersistenceMode = src.PersistenceMode
	}
	if dst.NetworkDelayMS == 0 {
		dst.NetworkDelayMS = src.NetworkDelayMS
	}
	if dst.NetworkEmulationMethod == "" {
		dst.NetworkEmulationMethod = src.NetworkEmulationMethod
	}
	if dst.NetworkInterface == "" {
		dst.NetworkInterface = src.NetworkInterface
	}
	if dst.NetworkFilter == "" {
		dst.NetworkFilter = src.NetworkFilter
	}
}
