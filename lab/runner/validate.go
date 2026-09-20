package main

import (
	"fmt"
	"sort"

	"conslab/internal/labcfg"
)

// cmdValidate runs a lightweight methodology validation experiment BEFORE the
// expensive full suite: a small set of conditions executed in randomized
// order, then automatically verified for the invariants the full benchmark
// depends on:
//
//  1. every condition occurs throughout the timeline (blocked randomization)
//  2. no condition is systematically associated with early/late execution
//  3. every run starts from a clean state (pre-flight passed)
//  4. teardown is complete and verified (containers/volumes/networks gone)
//  5. persistent consensus state does not survive between runs
//  6. run metadata is complete (schedule, seed, persistence, timestamps)
//  7. no host anomalies (suspend / CPU starvation / clock jumps)
//
// The full suite must not be run until this validation passes.
func cmdValidate(args []string) error {
	env := smokeEnv()
	seed := int64(20260919) // fixed, recorded; the validation is reproducible

	// Three conditions spanning both protocols and a failure mode, with
	// short durations so the validation is cheap.
	base := labcfg.Default()
	base.DurationS = 5
	base.WarmupS = 2
	base.Concurrency = 4
	base.Repetitions = 3
	base.Failure = labcfg.Failure{Mode: labcfg.FailureNone}

	cond1 := base
	cond1.Protocol = "raft"
	cond1.Experiment = "workload"
	cond1.WritePct = 100
	cond1.ReadPct = 0

	cond2 := base
	cond2.Protocol = "epaxos"
	cond2.Experiment = "workload"
	cond2.WritePct = 100
	cond2.ReadPct = 0

	cond3 := base
	cond3.Protocol = "raft"
	cond3.Experiment = "election"
	cond3.WritePct = 100
	cond3.ReadPct = 0
	cond3.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 2, FailedElections: 0}

	configs := []labcfg.Run{cond1, cond2, cond3}
	schedule := buildSchedule(configs, seed)
	fmt.Printf("validate: %d runs, schedule seed %d\n", len(schedule), seed)

	var results []runResult
	for i, item := range schedule {
		sched := &schedInfo{block: item.rep, seq: i + 1, seed: seed}
		fmt.Printf("  [%d/%d] %s %s (rep %d)\n", i+1, len(schedule), item.cfg.Protocol, experimentName(item.cfg), item.rep)
		res := executeRun(item.cfg, item.rep, env, sched)
		fmt.Printf("    -> %s (%s)\n", res.Status, res.Reason)
		results = append(results, res)
	}

	failures := validateOrderInvariants(results, configs, seed)
	if len(failures) > 0 {
		for _, f := range failures {
			fmt.Printf("  FAIL: %s\n", f)
		}
		fmt.Printf("validate: %d invariant(s) failed\n", len(failures))
		return fmt.Errorf("methodology validation failed")
	}
	fmt.Println("validate: PASS (all invariants hold)")
	return nil
}

// validateOrderInvariants checks the methodology invariants and returns a
// list of violations (empty = pass).
func validateOrderInvariants(results []runResult, configs []labcfg.Run, seed int64) []string {
	var failures []string

	// 1. All runs succeeded.
	for _, r := range results {
		if r.Status != "success" {
			failures = append(failures, fmt.Sprintf("%s did not succeed: %s", r.RunID, r.Reason))
		}
	}

	// 2. Every condition appears in every block, and no condition is
	//    systematically early or late within its block.
	condOf := map[string]string{} // runID -> condition
	blockPos := map[string][]int{}
	blockOf := map[string]int{}
	for _, r := range results {
		meta := loadMeta(r.Dir)
		if meta == nil {
			failures = append(failures, r.RunID+": metadata missing")
			continue
		}
		cfg := metaConfig(meta)
		cond := conditionString(cfg)
		condOf[r.RunID] = cond
		sched, _ := meta["schedule"].(map[string]any)
		block := intField(sched, "block")
		pos := intField(sched, "sequence_index")
		blockOf[r.RunID] = block
		blockPos[cond] = append(blockPos[cond], pos)
	}
	// Per block, the set of conditions must be exactly the configured set.
	byBlock := map[int][]string{}
	for rid, b := range blockOf {
		byBlock[b] = append(byBlock[b], condOf[rid])
	}
	for b, conds := range byBlock {
		seen := map[string]bool{}
		for _, c := range conds {
			seen[c] = true
		}
		if len(seen) != len(configs) {
			failures = append(failures, fmt.Sprintf("block %d has %d distinct conditions, want %d", b, len(seen), len(configs)))
		}
	}
	// No condition may always be first or always last within its block.
	for cond, positions := range blockPos {
		sort.Ints(positions)
		if positions[0] == positions[len(positions)-1] {
			failures = append(failures, fmt.Sprintf("condition %s always at position %d (systematically early/late)", cond, positions[0]))
		}
	}

	// 3. Teardown verified for every run; no leftover resources.
	for _, r := range results {
		if !teardownVerified(r.Dir) {
			failures = append(failures, r.RunID+": teardown not verified (containers/volumes/networks may remain)")
		}
		if !verifyTeardown(r.RunID) {
			failures = append(failures, r.RunID+": leftover containers/volumes/networks detected after teardown")
		}
	}

	// 4. Metadata completeness: schedule, seed, persistence, timestamps.
	for _, r := range results {
		meta := loadMeta(r.Dir)
		if meta == nil {
			continue
		}
		if _, ok := meta["schedule"]; !ok {
			failures = append(failures, r.RunID+": schedule metadata missing")
		}
		if _, ok := meta["persistence"]; !ok {
			failures = append(failures, r.RunID+": persistence classification missing")
		}
		if int64Field(meta, "seed_used") == 0 {
			failures = append(failures, r.RunID+": seed_used missing")
		}
		if strField(meta, "started_at") == "" || strField(meta, "finished_at") == "" {
			failures = append(failures, r.RunID+": timestamps missing")
		}
	}

	// 5. No host anomalies in any run.
	for _, r := range results {
		if an := detectHostAnomalies(r.Dir); len(an) > 0 {
			failures = append(failures, fmt.Sprintf("%s: host anomalies: %v", r.RunID, an))
		}
	}

	// 6. No overlapping runs (each run must start after the previous ended).
	sorted := append([]runResult(nil), results...)
	sort.Slice(sorted, func(i, j int) bool { return runStartedAt(sorted[i].Dir).Before(runStartedAt(sorted[j].Dir)) })
	for i := 1; i < len(sorted); i++ {
		prevEnd := parseTime(strField(loadMeta(sorted[i-1].Dir), "finished_at"))
		curStart := runStartedAt(sorted[i].Dir)
		if !prevEnd.IsZero() && curStart.Before(prevEnd) {
			failures = append(failures, fmt.Sprintf("%s started before %s ended", sorted[i].RunID, sorted[i-1].RunID))
		}
	}

	return failures
}