package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"conslab/internal/labcfg"
)

// manifestRun is one entry of the execution manifest, reconstructed from the
// run's actual metadata and telemetry (not from the intended schedule).
type manifestRun struct {
	RunID                string   `json:"run_id"`
	BatchID              string   `json:"batch_id"`
	RepetitionID         int      `json:"repetition_id"`
	Condition            string   `json:"condition"`
	RandomSeed           int64    `json:"random_seed"`
	BlockID              int      `json:"block_id"`
	SequenceIndex        int      `json:"sequence_index"`
	StartedAt            string   `json:"started_at"`
	EndedAt              string   `json:"ended_at"`
	DurationS            float64  `json:"duration_s"`
	SetupCompletedAt     string   `json:"setup_completed_at"`
	MeasurementStartedAt string   `json:"measurement_started_at"`
	TeardownCompletedAt  string   `json:"teardown_completed_at"`
	ExitStatus           string   `json:"exit_status"`
	Anomalies            []string `json:"anomalies"`
}

// manifestSummary aggregates the manifest for the report's Execution
// Integrity section.
type manifestSummary struct {
	BatchID          string        `json:"batch_id"`
	BatchIDs         []string      `json:"batch_ids"`
	ScheduleSeed     int64         `json:"schedule_seed"`
	TotalRuns        int           `json:"total_runs"`
	SuccessfulRuns   int           `json:"successful_runs"`
	FailedRuns       int           `json:"failed_runs"`
	ContaminatedRuns int           `json:"contaminated_runs"`
	ExcludedRuns     int           `json:"excluded_runs"`
	MinInterRunGapS  *float64      `json:"min_inter_run_gap_s"`
	MaxInterRunGapS  *float64      `json:"max_inter_run_gap_s"`
	Overlaps         []string      `json:"overlaps"`
	CleanupFailures  []string      `json:"cleanup_failures"`
	HostAnomalies    []string      `json:"host_anomalies"`
	Runs             []manifestRun `json:"runs"`
}

// conditionString is a compact, human-readable identity of a run's
// experimental condition (all independent variables).
func conditionString(cfg labcfg.Run) string {
	c := fmt.Sprintf("%s/%s/r%d/w%d/c%d", cfg.Protocol, experimentName(cfg), cfg.Replicas, cfg.WritePct, cfg.Concurrency)
	if cfg.ConflictPct > 0 {
		c += fmt.Sprintf("/x%d", cfg.ConflictPct)
	}
	if cfg.Failure.Mode != labcfg.FailureNone {
		c += fmt.Sprintf("/%s", cfg.Failure.Mode)
		if cfg.Failure.Mode == labcfg.FailureElection {
			c += fmt.Sprintf("/e%d", cfg.Failure.FailedElections)
		}
	}
	return c
}

// cmdManifest rebuilds results/execution-manifest.json from the runs already
// present in results/raw: the manifest is a derived view of the dataset and
// must be re-derivable without running the matrix (after a resumed batch, or
// after the host was interrupted).
func cmdManifest(args []string) error {
	fs := flag.NewFlagSet("manifest", flag.ContinueOnError)
	scheduleSeed := fs.Int64("schedule-seed", 1, "schedule seed recorded in the manifest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	writeExecutionManifest(runEnv{root: filepath.Join(resultsDir(), "raw")}, *scheduleSeed)
	return nil
}

// replacementPlan is one run that must be replaced: which attempt was flagged,
// why, and the schedule position the replacement keeps.
type replacementPlan struct {
	id    string
	cfg   labcfg.Run
	rep   int
	sched schedInfo
}

// runsToReplace lists the completed runs under root that must be replaced: an
// attempt whose host telemetry was flagged contaminated, or one that failed.
// A directory without metadata.json is a run left by a killed runner and is
// re-run by the matrix itself, not here.
func runsToReplace(root string, scheduleSeed int64) ([]replacementPlan, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var out []replacementPlan
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		meta := loadMeta(dir)
		if meta == nil {
			continue
		}
		status := strField(meta, "status")
		anomalies := detectHostAnomalies(dir)
		if status == "success" && len(anomalies) == 0 {
			continue
		}
		reason := "contaminated: " + joinAnomalies(anomalies)
		if status != "success" {
			reason = "failed: " + strField(meta, "failure_reason")
		}
		sched := schedInfo{
			seed:        scheduleSeed,
			rerunOf:     e.Name(),
			rerunReason: reason,
		}
		if s, ok := meta["schedule"].(map[string]any); ok {
			sched.block = intField(s, "block")
			sched.seq = intField(s, "sequence_index")
			if v := int64Field(s, "schedule_seed"); v != 0 {
				sched.seed = v
			}
		}
		out = append(out, replacementPlan{
			id:    e.Name(),
			cfg:   metaConfig(meta),
			rep:   intField(meta, "repetition"),
			sched: sched,
		})
	}
	return out, nil
}

// rerunFlaggedContaminated re-runs every completed run in results/raw that
// host telemetry flagged as contaminated, or that failed, so that each
// condition keeps one observation per planned repetition after the
// contaminated ones are excluded. The original attempt is never deleted or
// modified: the replacement is written to a new run ID ("<id>-2") which
// records what it replaces and why, and the manifest keeps both.
func rerunFlaggedContaminated(scheduleSeed int64) error {
	plans, err := runsToReplace(filepath.Join(resultsDir(), "raw"), scheduleSeed)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		fmt.Println("no contaminated or failed runs to replace")
		return nil
	}
	env := measuredEnv()
	fmt.Printf("replacement batch %s: %d run(s)\n", env.batchID, len(plans))
	var results []runResult
	for _, p := range plans {
		fmt.Printf("\nreplacing %s: %s\n", p.id, p.sched.rerunReason)
		sched := p.sched
		results = append(results, executeRun(p.cfg, p.rep, env, &sched))
	}
	writeExecutionManifest(env, scheduleSeed)
	return summarize(results)
}

// writeExecutionManifest reconstructs the ACTUAL execution order from every
// completed run's metadata and telemetry (not from this invocation's results:
// a matrix resumed with --skip-existing must still describe the whole
// dataset), verifies it (no overlaps, no unexpected gaps, no host anomalies),
// and writes results/execution-manifest.json.
func writeExecutionManifest(env runEnv, scheduleSeed int64) {
	summary := manifestSummary{ScheduleSeed: scheduleSeed}
	// Sort by actual start time to reconstruct the real sequence.
	sorted := completedRuns(env.root)
	sort.Slice(sorted, func(i, j int) bool {
		return runStartedAt(sorted[i].Dir).Before(runStartedAt(sorted[j].Dir))
	})

	var prevEnd time.Time
	for i, r := range sorted {
		mr := buildManifestRun(r)
		summary.Runs = append(summary.Runs, mr)
		if mr.ExitStatus == "success" {
			summary.SuccessfulRuns++
		} else {
			summary.FailedRuns++
		}
		if mr.BatchID != "" && !contains(summary.BatchIDs, mr.BatchID) {
			summary.BatchIDs = append(summary.BatchIDs, mr.BatchID)
		}
		if len(mr.Anomalies) > 0 {
			summary.ContaminatedRuns++
			summary.HostAnomalies = append(summary.HostAnomalies, mr.RunID+": "+joinAnomalies(mr.Anomalies))
		}
		// Teardown verification: a run whose teardown was not verified is a
		// cleanup failure (the next run may have started from a dirty state).
		if !teardownVerified(r.Dir) {
			summary.CleanupFailures = append(summary.CleanupFailures, r.RunID)
		}
		// Overlap / gap detection from actual timestamps.
		start := parseTime(mr.StartedAt)
		end := parseTime(mr.EndedAt)
		if i > 0 && !prevEnd.IsZero() {
			if start.Before(prevEnd) {
				summary.Overlaps = append(summary.Overlaps,
					fmt.Sprintf("%s started before %s ended", mr.RunID, sorted[i-1].RunID))
			} else {
				gap := start.Sub(prevEnd).Seconds()
				if summary.MinInterRunGapS == nil || gap < *summary.MinInterRunGapS {
					summary.MinInterRunGapS = &gap
				}
				if summary.MaxInterRunGapS == nil || gap > *summary.MaxInterRunGapS {
					summary.MaxInterRunGapS = &gap
				}
			}
		}
		prevEnd = end
	}
	summary.TotalRuns = len(summary.Runs)
	summary.BatchID = strings.Join(summary.BatchIDs, ", ")

	data, _ := json.MarshalIndent(summary, "", "  ")
	os.WriteFile(filepath.Join(resultsDir(), "execution-manifest.json"), data, 0o644)
	fmt.Printf("execution manifest: %d runs across %d batch(es), %d successful, %d failed, %d contaminated, %d cleanup failures\n",
		summary.TotalRuns, len(summary.BatchIDs), summary.SuccessfulRuns, summary.FailedRuns, summary.ContaminatedRuns, len(summary.CleanupFailures))
}

// completedRuns lists every completed run in root (directories holding a
// metadata.json, which is written last on every exit path). A directory left
// by a killed runner has no metadata and is not part of the dataset.
func completedRuns(root string) []runResult {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	var out []runResult
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		meta := loadMeta(dir)
		if meta == nil {
			continue
		}
		runID := strField(meta, "run_id")
		if runID == "" {
			runID = e.Name()
		}
		out = append(out, runResult{RunID: runID, Dir: dir, Status: strField(meta, "status")})
	}
	return out
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func buildManifestRun(r runResult) manifestRun {
	mr := manifestRun{
		RunID:      r.RunID,
		ExitStatus: r.Status,
	}
	meta := loadMeta(r.Dir)
	if meta != nil {
		cfg := metaConfig(meta)
		mr.BatchID = strField(meta, "batch_id")
		mr.Condition = conditionString(cfg)
		mr.RepetitionID = intField(meta, "repetition")
		mr.RandomSeed = int64Field(meta, "seed_used")
		mr.StartedAt = strField(meta, "started_at")
		mr.EndedAt = strField(meta, "finished_at")
		mr.DurationS = floatField(meta, "duration_s")
		if sched, ok := meta["schedule"].(map[string]any); ok {
			mr.BlockID = intField(sched, "block")
			mr.SequenceIndex = intField(sched, "sequence_index")
		}
	}
	// Measurement start = phase.json phase_started_ns (client-side).
	if ns := phaseStartedNS(r.Dir); ns > 0 {
		ts := time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
		mr.SetupCompletedAt = ts
		mr.MeasurementStartedAt = ts
	}
	// Teardown completion = teardown.json ts_ns (host-side).
	if ts := teardownTS(r.Dir); ts > 0 {
		mr.TeardownCompletedAt = time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
	}
	mr.Anomalies = detectHostAnomalies(r.Dir)
	return mr
}

// detectHostAnomalies inspects a run's host-telemetry.csv for environmental
// contamination: sampling gaps (host suspend / runner pause), load spikes
// (CPU starvation), and clock jumps. Anomalies are recorded, never silently
// deleted; exclusion is a separate, predefined decision.
func detectHostAnomalies(dir string) []string {
	path := filepath.Join(dir, "host-telemetry.csv")
	f, err := os.Open(path)
	if err != nil {
		return nil // no telemetry (e.g. failed early); not an anomaly by itself
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil || len(rows) < 2 {
		return nil
	}
	var anomalies []string
	var prevTS int64
	for i, row := range rows[1:] {
		ts, err := strconv.ParseInt(row[1], 10, 64)
		if err != nil {
			continue
		}
		if i > 0 {
			dt := ts - prevTS
			if dt < 0 {
				anomalies = append(anomalies, fmt.Sprintf("clock_jump: telemetry timestamp went backwards by %dms", -dt/1e6))
			} else if dt > hostPauseThresholdNS {
				anomalies = append(anomalies, fmt.Sprintf("host_pause: telemetry gap of %.1fs (host suspended or runner paused)", float64(dt)/1e9))
			}
		}
		prevTS = ts
		if load1, err := strconv.ParseFloat(row[2], 64); err == nil && load1 > hostLoadThreshold() {
			anomalies = append(anomalies, fmt.Sprintf("cpu_starvation: host load1=%.1f exceeds threshold %.1f", load1, hostLoadThreshold()))
		}
	}
	return dedupe(anomalies)
}

// hostPauseThresholdNS is the telemetry gap that indicates the host was
// suspended or the runner paused (5s at a 200ms sampling interval).
const hostPauseThresholdNS = 5 * int64(time.Second)

// hostLoadThreshold is the load1 value above which the host is considered
// CPU-starved for the benchmark's containers. It is relative to the host's
// CPU count: the benchmark itself legitimately uses ~9 cores (3 replicas at
// 2.0 CPUs + client 2.0 + master 1.0), so a fixed absolute threshold would
// flag the benchmark's own load as starvation. Only load approaching the
// machine's total capacity is anomalous.
func hostLoadThreshold() float64 {
	return 0.9 * float64(runtime.NumCPU())
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func joinAnomalies(a []string) string {
	out := ""
	for i, s := range a {
		if i > 0 {
			out += "; "
		}
		out += s
	}
	return out
}

// ---- small metadata helpers ----

func loadMeta(dir string) map[string]any {
	data, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}

func metaConfig(meta map[string]any) labcfg.Run {
	if c, ok := meta["config"].(map[string]any); ok {
		data, _ := json.Marshal(c)
		var cfg labcfg.Run
		if json.Unmarshal(data, &cfg) == nil {
			return cfg
		}
	}
	return labcfg.Default()
}

func strField(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func intField(m map[string]any, k string) int {
	switch v := m[k].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func int64Field(m map[string]any, k string) int64 {
	switch v := m[k].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	}
	return 0
}

func floatField(m map[string]any, k string) float64 {
	if v, ok := m[k].(float64); ok {
		return v
	}
	return 0
}

func runStartedAt(dir string) time.Time {
	if m := loadMeta(dir); m != nil {
		if t, err := time.Parse(time.RFC3339Nano, strField(m, "started_at")); err == nil {
			return t
		}
	}
	return time.Time{}
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

func phaseStartedNS(dir string) int64 {
	data, err := os.ReadFile(filepath.Join(dir, "phase.json"))
	if err != nil {
		return 0
	}
	var m map[string]int64
	if json.Unmarshal(data, &m) != nil {
		return 0
	}
	return m["phase_started_ns"]
}

func teardownTS(dir string) int64 {
	data, err := os.ReadFile(filepath.Join(dir, "teardown.json"))
	if err != nil {
		return 0
	}
	var m map[string]any
	if json.Unmarshal(data, &m) != nil {
		return 0
	}
	return int64Field(m, "ts_ns")
}

func teardownVerified(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, "teardown.json"))
	if err != nil {
		return false
	}
	var m map[string]any
	if json.Unmarshal(data, &m) != nil {
		return false
	}
	v, _ := m["verified"].(bool)
	return v
}
