package labcfg

import "testing"

// validBase returns a known-valid configuration to mutate in each test.
func validBase() Run {
	r := Default()
	r.Failure.Mode = FailureNone
	return r
}

func TestValidateAcceptsDefault(t *testing.T) {
	if err := validBase().Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestValidateRejectsBadProtocol(t *testing.T) {
	r := validBase()
	r.Protocol = "paxos"
	if err := r.Validate(); err == nil {
		t.Fatal("expected error for unknown protocol")
	}
}

func TestValidateConflictPctRange(t *testing.T) {
	for _, pct := range []int{-1, 101} {
		r := validBase()
		r.ConflictPct = pct
		if err := r.Validate(); err == nil {
			t.Errorf("conflict_pct=%d should be rejected", pct)
		}
	}
	for _, pct := range []int{0, 1, 50, 99, 100} {
		r := validBase()
		r.ConflictPct = pct
		if err := r.Validate(); err != nil {
			t.Errorf("conflict_pct=%d should be accepted: %v", pct, err)
		}
	}
}

func TestValidateHotKeysRange(t *testing.T) {
	r := validBase()
	r.HotKeys = 0
	if err := r.Validate(); err == nil {
		t.Error("hot_keys=0 should be rejected")
	}

	r = validBase()
	r.Keyspace = 10
	r.HotKeys = 11
	if err := r.Validate(); err == nil {
		t.Error("hot_keys > keyspace should be rejected")
	}

	r = validBase()
	r.Keyspace = 10
	r.HotKeys = 10
	if err := r.Validate(); err != nil {
		t.Errorf("hot_keys == keyspace should be accepted: %v", err)
	}
}

func TestValidateElectionFailureRequiresRaft(t *testing.T) {
	r := validBase()
	r.Protocol = "epaxos"
	r.Failure.Mode = FailureElection
	r.Failure.AtS = 10
	r.Failure.FailedElections = 1
	if err := r.Validate(); err == nil {
		t.Error("election failure mode should require raft")
	}

	r = validBase()
	r.Protocol = "raft"
	r.Failure.Mode = FailureElection
	r.Failure.AtS = 10
	r.Failure.FailedElections = 1
	if err := r.Validate(); err != nil {
		t.Errorf("raft election failure should be valid: %v", err)
	}
}

func TestValidateElectionFailureCount(t *testing.T) {
	// 0 is the baseline (no failed elections induced) and must be valid.
	r := validBase()
	r.Protocol = "raft"
	r.Failure.Mode = FailureElection
	r.Failure.AtS = 10
	r.Failure.FailedElections = 0
	if err := r.Validate(); err != nil {
		t.Errorf("failed_elections=0 (baseline) should be valid: %v", err)
	}

	// Negative counts are invalid.
	r.Failure.FailedElections = -1
	if err := r.Validate(); err == nil {
		t.Error("failed_elections=-1 should be rejected")
	}
}

func TestValidateFailureRequiresPositiveAtS(t *testing.T) {
	r := validBase()
	r.Protocol = "raft"
	r.Failure.Mode = FailureLeader
	r.Failure.AtS = 0
	if err := r.Validate(); err == nil {
		t.Error("failure injection should require at_s > 0")
	}
}

func TestValidateWorkloadPercentages(t *testing.T) {
	r := validBase()
	r.ReadPct = 60
	r.WritePct = 60
	if err := r.Validate(); err == nil {
		t.Error("read_pct+write_pct != 100 should be rejected")
	}
}

func TestValidateReplicaCountRange(t *testing.T) {
	for _, n := range []int{0, 10} {
		r := validBase()
		r.Replicas = n
		if err := r.Validate(); err == nil {
			t.Errorf("replicas=%d should be rejected", n)
		}
	}
	for _, n := range []int{1, 3, 5, 7, 9} {
		r := validBase()
		r.Replicas = n
		if err := r.Validate(); err != nil {
			t.Errorf("replicas=%d should be accepted: %v", n, err)
		}
	}
}

// TestDefaultIsSelfConsistent guards against a default that fails its own
// validation, which would break every run that relies on defaults.
func TestDefaultIsSelfConsistent(t *testing.T) {
	d := Default()
	if d.ReadPct+d.WritePct != 100 {
		t.Fatalf("default read+write = %d, want 100", d.ReadPct+d.WritePct)
	}
	if d.Repetitions < 1 {
		t.Fatalf("default repetitions = %d, want >= 1", d.Repetitions)
	}
	if d.HotKeys < 1 {
		t.Fatalf("default hot_keys = %d, want >= 1", d.HotKeys)
	}
	if d.HotKeys > d.Keyspace {
		t.Fatalf("default hot_keys %d > keyspace %d", d.HotKeys, d.Keyspace)
	}
}
