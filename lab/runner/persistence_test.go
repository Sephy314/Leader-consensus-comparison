package main

import (
	"testing"

	"conslab/internal/labcfg"
)

// TestRunIDReflectsPersistenceMode verifies that the persistence mode is part
// of a persistence run's identity: the durable and in-memory runs of one
// implementation would otherwise collide and overwrite each other's raw data.
func TestRunIDReflectsPersistenceMode(t *testing.T) {
	mk := func(mode string) labcfg.Run {
		r := baseRun()
		r.Experiment = "persistence"
		r.WritePct = 100
		r.ReadPct = 0
		r.PersistenceMode = mode
		return r
	}
	durable := runIDFor(mk(labcfg.PersistDurable), 1)
	memory := runIDFor(mk(labcfg.PersistMemory), 1)
	if durable == memory {
		t.Fatalf("durable and memory runs share the ID %q", durable)
	}
	if got, want := durable, "persistence-raft-durable-r3-w100-c32-1"; got != want {
		t.Errorf("durable run ID = %q, want %q", got, want)
	}
	if got, want := memory, "persistence-raft-memory-r3-w100-c32-1"; got != want {
		t.Errorf("memory run ID = %q, want %q", got, want)
	}
	// Stating the default explicitly is the same condition and must therefore
	// produce the same identity, not a duplicate configuration.
	explicit := mk(labcfg.PersistDurable)
	implicit := mk("")
	if runIDFor(explicit, 1) != runIDFor(implicit, 1) {
		t.Error("an explicit default mode must not create a second identity")
	}
}

// TestRunIDReflectsNetworkDelay verifies that the emulated delay level is part
// of a network-delay run's identity.
func TestRunIDReflectsNetworkDelay(t *testing.T) {
	mk := func(delay int) labcfg.Run {
		r := baseRun()
		r.Experiment = "networkdelay"
		r.WritePct = 100
		r.ReadPct = 0
		r.NetworkDelayMS = delay
		return r
	}
	seen := map[string]int{}
	for _, d := range []int{0, 1, 3, 5, 10} {
		id := runIDFor(mk(d), 1)
		if prev, dup := seen[id]; dup {
			t.Fatalf("delays %d and %d share the ID %q", prev, d, id)
		}
		seen[id] = d
	}
	if got, want := runIDFor(mk(5), 1), "networkdelay-raft-r3-w100-c32-d5-1"; got != want {
		t.Errorf("run ID = %q, want %q", got, want)
	}
}

// TestExpandMatrixPersistenceExpandsOnlyProvidedModes is the guard on the
// "do not invent a persistence mechanism" rule: the persistence matrix must
// expand exactly the (implementation, mode) pairs the implementations provide,
// and every expanded configuration must be valid.
func TestExpandMatrixPersistenceExpandsOnlyProvidedModes(t *testing.T) {
	spec := MatrixSpec{
		Defaults: labcfg.Default(),
		Persistence: &PersistenceSpec{
			Protocols:       []string{"raft", "epaxos"},
			Implementations: []string{labcfg.ImplHashicorp, labcfg.ImplEtcd, labcfg.ImplOriginal, labcfg.ImplNVB},
			Modes:           []string{labcfg.PersistDurable, labcfg.PersistMemory},
			Replicas:        []int{3},
			WritePct:        100,
			Concurrency:     32,
		},
	}
	configs := expandMatrix(spec, "persistence")
	got := map[string]map[string]bool{}
	for _, c := range configs {
		if err := c.Validate(); err != nil {
			t.Errorf("expanded %s/%s config should be valid: %v", c.Impl(), c.Persist(), err)
		}
		if got[c.Impl()] == nil {
			got[c.Impl()] = map[string]bool{}
		}
		got[c.Impl()][c.Persist()] = true
	}
	for _, impl := range []string{labcfg.ImplHashicorp, labcfg.ImplEtcd, labcfg.ImplOriginal} {
		if len(got[impl]) != 2 {
			t.Errorf("%s provides both modes but expanded to %v", impl, got[impl])
		}
	}
	nvb := got[labcfg.ImplNVB]
	if !nvb[labcfg.PersistMemory] {
		t.Error("nvb must expand its in-memory mode")
	}
	if nvb[labcfg.PersistDurable] {
		t.Error("nvb has no durable mode; the cell must be skipped, not filled")
	}
}

// TestExpandMatrixNetworkDelaySetsOnlyOSEmulation verifies that the
// network-delay family never carries the legacy in-adapter injection, so the
// two mechanisms cannot be confounded in one run.
func TestExpandMatrixNetworkDelaySetsOnlyOSEmulation(t *testing.T) {
	base := labcfg.Default()
	base.CommCostMS = 5 // deliberately poisoned: the expansion must clear it
	base.CommJitterPct = 10
	spec := MatrixSpec{
		Defaults: base,
		NetworkDelay: &NetworkDelaySpec{
			Protocols:   []string{"raft", "epaxos"},
			Replicas:    []int{3},
			WritePct:    100,
			Concurrency: 32,
			DelaysMS:    []int{0, 1, 3, 5, 10},
		},
	}
	configs := expandMatrix(spec, "networkdelay")
	if len(configs) != 10 { // 2 protocols x 5 delay levels
		t.Fatalf("expanded %d configs, want 10", len(configs))
	}
	for _, c := range configs {
		if c.CommCostMS != 0 || c.CommJitterPct != 0 {
			t.Errorf("network-delay config leaked the legacy injection: %+v", c)
		}
		if err := c.Validate(); err != nil {
			t.Errorf("expanded config should be valid: %v", err)
		}
	}
}
