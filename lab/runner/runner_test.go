package main

import (
	"strings"
	"testing"

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
		{"elections0", func(r *labcfg.Run) { r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: 1} }},
		{"elections4", func(r *labcfg.Run) { r.Failure = labcfg.Failure{Mode: labcfg.FailureElection, AtS: 10, FailedElections: 5} }},
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
