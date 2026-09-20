package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"conslab/internal/labcfg"
)

func baseRun() labcfg.Run {
	r := labcfg.Default()
	r.Protocol = "raft"
	r.Replicas = 3
	r.ReadPct = 50
	r.WritePct = 50
	r.Concurrency = 32
	r.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
	return r
}

func TestExperimentNameExplicitWins(t *testing.T) {
	r := baseRun()
	r.Experiment = "concurrency"
	if got := experimentName(r); got != "concurrency" {
		t.Fatalf("explicit experiment: got %q, want concurrency", got)
	}
}

func TestExperimentNameDerivation(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*labcfg.Run)
		want string
	}{
		{"failure", func(r *labcfg.Run) { r.Failure = labcfg.Failure{Mode: labcfg.FailureLeader, AtS: 10} }, "failure"},
		{"conflict", func(r *labcfg.Run) { r.ConflictPct = 50 }, "conflict"},
		{"scaling", func(r *labcfg.Run) { r.Replicas = 5 }, "scaling"},
		{"workload", func(r *labcfg.Run) {}, "workload"},
	}
	for _, tc := range tests {
		r := baseRun()
		tc.mut(&r)
		if got := experimentName(r); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestRunIDUniqueness verifies that distinct configurations never produce the
// same run ID, which is what keeps raw results from colliding.
func TestRunIDUniqueness(t *testing.T) {
	seen := map[string]string{}
	configs := []struct {
		label string
		mut   func(*labcfg.Run)
	}{
		{"plain", func(r *labcfg.Run) {}},
		{"epaxos", func(r *labcfg.Run) { r.Protocol = "epaxos" }},
		{"r5", func(r *labcfg.Run) { r.Replicas = 5 }},
		{"w100", func(r *labcfg.Run) { r.WritePct = 100; r.ReadPct = 0 }},
		{"c1", func(r *labcfg.Run) { r.Concurrency = 1 }},
		{"conflict10", func(r *labcfg.Run) { r.ConflictPct = 10 }},
		{"conflict90", func(r *labcfg.Run) { r.ConflictPct = 90 }},
		{"leaderfail", func(r *labcfg.Run) { r.Failure = labcfg.Failure{Mode: labcfg.FailureLeader, AtS: 10} }},
		{"followerfail", func(r *labcfg.Run) { r.Failure = labcfg.Failure{Mode: labcfg.FailureFollower, AtS: 10} }},
		{"elections0", func(r *labcfg.Run) {
			r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: 1}
		}},
		{"elections4", func(r *labcfg.Run) {
			r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: 5}
		}},
	}
	for _, tc := range configs {
		r := baseRun()
		tc.mut(&r)
		id := runIDFor(r, 1)
		if prev, dup := seen[id]; dup {
			t.Errorf("run ID collision: %q and %q both produce %q", prev, tc.label, id)
		}
		seen[id] = tc.label
	}
}

func TestRunIDReflectsConflictRate(t *testing.T) {
	r := baseRun()
	r.ConflictPct = 75
	id := runIDFor(r, 1)
	if !strings.Contains(id, "x75") {
		t.Fatalf("run ID %q should encode conflict rate x75", id)
	}
}

func TestRunIDReflectsFailedElections(t *testing.T) {
	r := baseRun()
	r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: 3}
	id := runIDFor(r, 1)
	if !strings.Contains(id, "e3") {
		t.Fatalf("run ID %q should encode failed elections e3", id)
	}
}

// TestRunIDReflectsFailedElectionsMatrixPath verifies the run ID encodes the
// failed-elections target through the ACTUAL matrix path, where the
// experiment is set explicitly to "election" (the derived "failure" path
// alone does not exercise this).
func TestRunIDReflectsFailedElectionsMatrixPath(t *testing.T) {
	r := baseRun()
	r.Experiment = "election"
	r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: 4}
	id := runIDFor(r, 1)
	if !strings.Contains(id, "e4") {
		t.Fatalf("matrix-path run ID %q should encode failed elections e4", id)
	}
	if strings.Contains(id, "w100") {
		t.Fatalf("matrix-path run ID %q should not carry the workload suffix", id)
	}
	// Different targets must never collide, even within the same repetition.
	if runIDFor(r, 1) == runIDFor(baseRunWithElection(3), 1) {
		t.Fatal("different failed-elections targets must produce different run IDs")
	}
}

// TestRunsToReplaceSelectsFlaggedAttempts covers the replacement rule: only
// attempts that failed or whose host telemetry was flagged contaminated are
// replaced, and the replacement inherits the original schedule position.
func TestRunsToReplaceSelectsFlaggedAttempts(t *testing.T) {
	root := t.TempDir()
	clean := writeFakeRun(t, root, "workload-raft-r3-w50-c32-1", "success", "", false)
	contaminated := writeFakeRun(t, root, "workload-raft-r3-w50-c32-2", "success", "", true)
	failed := writeFakeRun(t, root, "workload-raft-r3-w50-c32-3", "failed", "requests.csv empty", false)

	plans, err := runsToReplace(root, 42)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]replacementPlan{}
	for _, p := range plans {
		got[p.id] = p
	}
	if _, ok := got[clean]; ok {
		t.Fatalf("a clean successful run must not be replaced: %v", got)
	}
	for _, id := range []string{contaminated, failed} {
		p, ok := got[id]
		if !ok {
			t.Fatalf("%s should be replaced, got %v", id, got)
		}
		if p.sched.rerunOf != id {
			t.Errorf("%s: rerun_of = %q, want %q", id, p.sched.rerunOf, id)
		}
		if p.sched.rerunReason == "" {
			t.Errorf("%s: replacement reason not recorded", id)
		}
		// The replacement keeps the original's position in the schedule,
		// including the schedule seed the original was run under.
		if p.sched.block != 5 || p.sched.seq != 7 || p.sched.seed != 1 {
			t.Errorf("%s: schedule position not inherited: %+v", id, p.sched)
		}
		if p.rep != 2 && p.rep != 3 {
			t.Errorf("%s: repetition = %d, want the original run's repetition", id, p.rep)
		}
	}
	if !strings.Contains(got[contaminated].sched.rerunReason, "host_pause") {
		t.Errorf("contaminated reason should name the anomaly, got %q", got[contaminated].sched.rerunReason)
	}
	if !strings.Contains(got[failed].sched.rerunReason, "requests.csv empty") {
		t.Errorf("failed reason should record the failure, got %q", got[failed].sched.rerunReason)
	}
}

// writeFakeRun creates a completed run directory: metadata.json plus, when
// requested, host telemetry containing a sampling gap (the host-suspend
// signature the contamination rule detects).
func writeFakeRun(t *testing.T, root, id, status, reason string, contaminated bool) string {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	meta := map[string]any{
		"run_id":         id,
		"repetition":     2,
		"status":         status,
		"failure_reason": nullable(reason),
		"config":         baseRun(),
		"schedule":       map[string]any{"block": 5, "sequence_index": 7, "schedule_seed": 1},
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if contaminated {
		gap := int64(30 * time.Second) // > hostPauseThresholdNS
		rows := fmt.Sprintf("run_id,ts_ns,load1,load5,load15,mem_available_bytes\n"+
			"%s,1000000000,1.0,1.0,1.0,1000\n%s,%d,1.0,1.0,1.0,1000\n",
			id, id, 1_000_000_000+gap)
		if err := os.WriteFile(filepath.Join(dir, "host-telemetry.csv"), []byte(rows), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func baseRunWithElection(n int) labcfg.Run {
	r := baseRun()
	r.Experiment = "election"
	r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: n}
	return r
}

// TestRunCompletedAndDirReuse covers the resume path: a completed run
// (metadata.json present) is reported as done so --skip-existing does not
// repeat it, while a directory left behind by a killed runner (no
// metadata.json) is reused in place instead of becoming a "-2" duplicate of
// the same run ID.
func TestRunCompletedAndDirReuse(t *testing.T) {
	root := t.TempDir()
	cfg, rep := baseRun(), 1
	if runCompleted(cfg, rep, root) {
		t.Fatal("no run directory yet, but runCompleted reported true")
	}

	id, dir := makeRunDir(cfg, rep, root)
	if id != runIDFor(cfg, rep) {
		t.Fatalf("interrupted run dir: got %q, want canonical %q", id, runIDFor(cfg, rep))
	}
	if runCompleted(cfg, rep, root) {
		t.Fatal("a directory without metadata.json is not a completed run")
	}
	if id2, dir2 := makeRunDir(cfg, rep, root); id2 != id || dir2 != dir {
		t.Fatalf("incomplete dir not reused: got %q %q, want %q %q", id2, dir2, id, dir)
	}

	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !runCompleted(cfg, rep, root) {
		t.Fatal("completed run not reported as done")
	}
	if id3, _ := makeRunDir(cfg, rep, root); id3 == id {
		t.Fatalf("a completed run must not be overwritten, got %q again", id3)
	}
}

// TestSeedForIndependentRepetitions verifies each repetition of a
// configuration uses an independent seed, so no two repetitions replay the
// same workload sequence.
func TestSeedForIndependentRepetitions(t *testing.T) {
	r := baseRun()
	seen := map[int64]bool{}
	for rep := 1; rep <= 20; rep++ {
		s := seedFor(r, rep)
		if seen[s] {
			t.Fatalf("seed %d reused across repetitions", s)
		}
		seen[s] = true
	}
	if seedFor(r, 1) == seedFor(r, 2) {
		t.Fatal("adjacent repetitions must not share a seed")
	}
}

// TestValidateFinalStateRaft verifies the final-state check for Raft failure
// runs: the LAST observed leader must be valid. A follower kill (leader
// unchanged) must pass even though no leader event fires after the
// injection; a leader kill with a new leader must pass; a run whose cluster
// never recovers (last leader = -1) must fail.
func TestValidateFinalStateRaft(t *testing.T) {
	writeEvents := func(t *testing.T, dir string, events [][2]string) {
		t.Helper()
		var b strings.Builder
		b.WriteString("run_id,ts_ns,event,detail\n")
		for i, e := range events {
			b.WriteString(fmt.Sprintf("r,%d,%s,%s\n", 1000+i*100, e[0], e[1]))
		}
		if err := os.WriteFile(filepath.Join(dir, "events.csv"), []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := baseRun()
	base.Failure = labcfg.Failure{Mode: labcfg.FailureFollower, AtS: 10}

	// Follower kill: leader observed before injection, unchanged after.
	dir := t.TempDir()
	writeEvents(t, dir, [][2]string{
		{"leader_observed", "0"},
		{"failure_injected", "c"},
		{"client_exit", "0"},
	})
	if reason := validateFinalState(dir, base); reason != "" {
		t.Fatalf("follower kill should pass, got: %s", reason)
	}

	// Leader kill: new leader elected after injection.
	base.Failure = labcfg.Failure{Mode: labcfg.FailureLeader, AtS: 10}
	dir2 := t.TempDir()
	writeEvents(t, dir2, [][2]string{
		{"leader_observed", "0"},
		{"failure_injected", "c"},
		{"leader_observed", "-1"},
		{"leader_observed", "1"},
	})
	if reason := validateFinalState(dir2, base); reason != "" {
		t.Fatalf("leader kill with new leader should pass, got: %s", reason)
	}

	// No recovery: last leader is -1.
	dir3 := t.TempDir()
	writeEvents(t, dir3, [][2]string{
		{"leader_observed", "0"},
		{"failure_injected", "c"},
		{"leader_observed", "-1"},
	})
	if reason := validateFinalState(dir3, base); reason == "" {
		t.Fatal("no-recovery run should fail")
	}

	// No leader event at all.
	dir4 := t.TempDir()
	writeEvents(t, dir4, [][2]string{
		{"failure_injected", "c"},
	})
	if reason := validateFinalState(dir4, base); reason == "" {
		t.Fatal("run with no leader event should fail")
	}
}

// TestValidateFinalStateEPaxos verifies the EPaxos final-state check: a
// successful request after the injection passes; none fails.
func TestValidateFinalStateEPaxos(t *testing.T) {
	writeReq := func(t *testing.T, dir string, okEnds []int64) {
		t.Helper()
		var b strings.Builder
		b.WriteString("run_id,ok,end_ns\n")
		for i, e := range okEnds {
			b.WriteString(fmt.Sprintf("r,%d,%d\n", i%2, e))
		}
		if err := os.WriteFile(filepath.Join(dir, "requests.csv"), []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := baseRun()
	base.Protocol = "epaxos"
	base.Failure = labcfg.Failure{Mode: labcfg.FailureReplica, AtS: 10}

	dir := t.TempDir()
	writeReq(t, dir, []int64{500, 1500, 2500}) // successes at 500, 2500
	os.WriteFile(filepath.Join(dir, "events.csv"),
		[]byte("run_id,ts_ns,event,detail\nr,1000,failure_injected,c\n"), 0o644)
	if reason := validateFinalState(dir, base); reason != "" {
		t.Fatalf("success after injection should pass, got: %s", reason)
	}

	dir2 := t.TempDir()
	writeReq(t, dir2, []int64{500}) // only success before injection
	os.WriteFile(filepath.Join(dir2, "events.csv"),
		[]byte("run_id,ts_ns,event,detail\nr,1000,failure_injected,c\n"), 0o644)
	if reason := validateFinalState(dir2, base); reason == "" {
		t.Fatal("no success after injection should fail")
	}
}
func TestBuildScheduleInterleavesConditions(t *testing.T) {
	cfgs := []labcfg.Run{}
	for _, proto := range []string{"raft", "epaxos"} {
		for _, target := range []int{0, 2, 4} {
			r := baseRun()
			r.Protocol = proto
			r.Experiment = "election"
			r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: target}
			r.Repetitions = 3
			cfgs = append(cfgs, r)
		}
	}
	sched := buildSchedule(cfgs, 42)
	if len(sched) != 18 {
		t.Fatalf("schedule has %d runs, want 18", len(sched))
	}
	// Block 1 must contain all 6 configurations exactly once.
	block1 := map[string]int{}
	for _, item := range sched[:6] {
		block1[runIDFor(item.cfg, item.rep)]++
	}
	if len(block1) != 6 {
		t.Fatalf("block 1 has %d distinct configs, want 6", len(block1))
	}
	// The order must differ from the input order (shuffled).
	inputOrder := []string{}
	for _, c := range cfgs {
		inputOrder = append(inputOrder, runIDFor(c, 1))
	}
	schedOrder := []string{}
	for _, item := range sched[:6] {
		schedOrder = append(schedOrder, runIDFor(item.cfg, item.rep))
	}
	same := true
	for i := range inputOrder {
		if inputOrder[i] != schedOrder[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("schedule order equals input order; conditions are not randomized")
	}
	// Reproducible: same seed, same schedule.
	sched2 := buildSchedule(cfgs, 42)
	for i := range sched {
		if runIDFor(sched[i].cfg, sched[i].rep) != runIDFor(sched2[i].cfg, sched2[i].rep) {
			t.Fatal("same seed produced a different schedule")
		}
	}
	// Different seed, different order.
	sched3 := buildSchedule(cfgs, 43)
	diff := false
	for i := range sched {
		if runIDFor(sched[i].cfg, sched[i].rep) != runIDFor(sched3[i].cfg, sched3[i].rep) {
			diff = true
			break
		}
	}
	if !diff {
		t.Fatal("different seeds produced identical schedules")
	}
}

func TestRunIDReflectsRepetition(t *testing.T) {
	r := baseRun()
	if runIDFor(r, 1) == runIDFor(r, 2) {
		t.Fatal("different repetitions must produce different run IDs")
	}
}

// TestApplyRunDefaultsFillsZeros verifies that applyRunDefaults fills only
// zero-valued fields, leaving explicit values intact.
func TestApplyRunDefaultsFillsZeros(t *testing.T) {
	src := labcfg.Default()
	var dst labcfg.Run
	applyRunDefaults(&dst, src)
	if dst.Protocol != src.Protocol {
		t.Errorf("protocol not defaulted: got %q", dst.Protocol)
	}
	if dst.Replicas != src.Replicas {
		t.Errorf("replicas not defaulted: got %d", dst.Replicas)
	}
	if dst.Concurrency != src.Concurrency {
		t.Errorf("concurrency not defaulted: got %d", dst.Concurrency)
	}
	if dst.Failure.Mode != src.Failure.Mode {
		t.Errorf("failure mode not defaulted: got %q", dst.Failure.Mode)
	}

	// An explicit value must survive.
	dst2 := labcfg.Run{Protocol: "epaxos", Replicas: 7, Concurrency: 128}
	applyRunDefaults(&dst2, src)
	if dst2.Protocol != "epaxos" || dst2.Replicas != 7 || dst2.Concurrency != 128 {
		t.Fatalf("explicit values were overwritten: %+v", dst2)
	}
}

// TestApplyRunDefaultsPreservesExplicitZeroWrite verifies that a deliberate
// 100%-read config (write_pct=0) is not mistaken for an unset field.
func TestApplyRunDefaultsPreservesExplicitZeroWrite(t *testing.T) {
	src := labcfg.Default()
	dst := labcfg.Run{ReadPct: 100, WritePct: 0}
	applyRunDefaults(&dst, src)
	if dst.ReadPct != 100 {
		t.Errorf("explicit read_pct=100 was overwritten: got %d", dst.ReadPct)
	}
}

func TestPickFollowerExcludesLeader(t *testing.T) {
	for n := 1; n <= 9; n++ {
		for leader := 0; leader < n; leader++ {
			f := pickFollower(n, leader)
			if n > 1 && f == leader {
				t.Errorf("n=%d leader=%d: follower equals leader", n, leader)
			}
			if f < 0 || f >= n {
				t.Errorf("n=%d leader=%d: follower %d out of range", n, leader, f)
			}
		}
	}
}

func TestReadSemanticsPerProtocol(t *testing.T) {
	for _, proto := range []string{"raft", "epaxos"} {
		rs := readSemantics(proto)
		if rs["system"] != proto {
			t.Errorf("%s: system mismatch: %v", proto, rs["system"])
		}
		if rs["read_consistency"] == nil || rs["read_consistency"] == "" {
			t.Errorf("%s: read_consistency not set", proto)
		}
		if rs["coordination_required"] != true {
			t.Errorf("%s: reads must require coordination (both route through consensus)", proto)
		}
		if rs["read_path"] == nil || rs["read_path"] == "" {
			t.Errorf("%s: read_path not set", proto)
		}
	}
}

// TestReadSemanticsLeaderTargeting verifies Raft targets the leader while
// EPaxos is leaderless, which is the key fairness-relevant difference.
func TestReadSemanticsLeaderTargeting(t *testing.T) {
	if rs := readSemantics("raft"); rs["target_replica"] != "leader" {
		t.Errorf("raft target_replica = %v, want leader", rs["target_replica"])
	}
	ep := readSemantics("epaxos")
	if ep["target_replica"] == "leader" {
		t.Error("epaxos must not be modelled as leader-targeting")
	}
}
