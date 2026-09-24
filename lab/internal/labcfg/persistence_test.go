package labcfg

import "testing"

// TestPersistDefaultsMatchRecordedDataset pins the default persistence mode of
// every implementation to the mode the recorded runs already used. An empty
// PersistenceMode must therefore leave the recorded dataset's meaning intact.
func TestPersistDefaultsMatchRecordedDataset(t *testing.T) {
	want := map[string]string{
		ImplHashicorp: PersistDurable, // raft-boltdb
		ImplEtcd:      PersistDurable, // fsynced write-ahead log
		ImplEtcdCore:  PersistMemory,  // core-only analysis mode, no WAL
		ImplOriginal:  PersistMemory,  // upstream default, no -durable
		ImplNVB:       PersistMemory,  // library MemoryStorage only
	}
	protocol := map[string]string{
		ImplHashicorp: "raft", ImplEtcd: "raft", ImplEtcdCore: "raft",
		ImplOriginal: "epaxos", ImplNVB: "epaxos",
	}
	for impl, wantMode := range want {
		if got := DefaultPersistence(impl); got != wantMode {
			t.Errorf("DefaultPersistence(%s) = %q, want %q", impl, got, wantMode)
		}
	}
	for impl, mode := range want {
		r := validBase()
		r.Protocol = protocol[impl]
		r.Implementation = impl
		// A pre-existing config has an empty mode and must still validate.
		if err := r.Validate(); err != nil {
			t.Errorf("%s with an empty persistence_mode should validate: %v", impl, err)
		}
		if r.Persist() != mode {
			t.Errorf("%s persists in %q, want %q", impl, r.Persist(), mode)
		}
	}
}

// TestValidatePersistenceModesAreImplementationProvided verifies that only
// modes an implementation actually provides are accepted. A durable mode must
// not be obtainable for an implementation that has none: the benchmark never
// invents a persistence mechanism to fill the cell.
func TestValidatePersistenceModesAreImplementationProvided(t *testing.T) {
	raft := func(impl string) Run {
		r := validBase()
		r.Implementation = impl
		return r
	}
	epaxos := func(impl string) Run {
		r := validBase()
		r.Protocol = "epaxos"
		r.Implementation = impl
		return r
	}
	accepted := []struct {
		name string
		r    Run
		mode string
	}{
		{"hashicorp/memory", raft(ImplHashicorp), PersistMemory},
		{"etcd/memory", raft(ImplEtcd), PersistMemory},
		{"nvb/memory", epaxos(ImplNVB), PersistMemory},
		{"original/durable", epaxos(ImplOriginal), PersistDurable},
		{"original/memory", epaxos(ImplOriginal), PersistMemory},
	}
	for _, tc := range accepted {
		tc.r.PersistenceMode = tc.mode
		if err := tc.r.Validate(); err != nil {
			t.Errorf("%s should be accepted: %v", tc.name, err)
		}
	}
	rejected := []struct {
		name string
		r    Run
		mode string
	}{
		{"nvb/durable", epaxos(ImplNVB), PersistDurable},
		{"etcd-core/durable", raft(ImplEtcdCore), PersistDurable},
		{"hashicorp/unknown-mode", raft(ImplHashicorp), "wal"},
	}
	for _, tc := range rejected {
		tc.r.PersistenceMode = tc.mode
		if err := tc.r.Validate(); err == nil {
			t.Errorf("%s should be rejected (no such persistence mode)", tc.name)
		}
	}
}

// TestValidateRejectsConfoundedNetworkInjection verifies that the OS-level
// emulation and the legacy in-adapter sleep can never be combined in one run:
// a run with both would not be attributable to either mechanism.
func TestValidateRejectsConfoundedNetworkInjection(t *testing.T) {
	r := validBase()
	r.NetworkDelayMS = 5
	r.CommCostMS = 5
	if err := r.Validate(); err == nil {
		t.Fatal("network_delay_ms together with comm_cost_ms should be rejected")
	}
	r = validBase()
	r.NetworkDelayMS = 5
	r.CommJitterPct = 10
	if err := r.Validate(); err == nil {
		t.Fatal("network_delay_ms together with comm_jitter_pct should be rejected")
	}
	r = validBase()
	r.NetworkDelayMS = 5
	if err := r.Validate(); err != nil {
		t.Fatalf("OS-level emulation alone should be accepted: %v", err)
	}
}

// TestValidateNetworkEmulationBounds verifies the emulation parameters are
// bounded: an out-of-range delay or an unimplemented mechanism must be
// rejected rather than silently measured.
func TestValidateNetworkEmulationBounds(t *testing.T) {
	for _, d := range []int{-1, MaxNetworkDelayMS + 1} {
		r := validBase()
		r.NetworkDelayMS = d
		if err := r.Validate(); err == nil {
			t.Errorf("network_delay_ms=%d should be rejected", d)
		}
	}
	for _, d := range []int{0, 1, 3, 5, 10} {
		r := validBase()
		r.NetworkDelayMS = d
		if err := r.Validate(); err != nil {
			t.Errorf("network_delay_ms=%d should be accepted: %v", d, err)
		}
	}
	r := validBase()
	r.NetworkDelayMS = 5
	r.NetworkEmulationMethod = "iptables-sleep"
	if err := r.Validate(); err == nil {
		t.Error("an unimplemented emulation method should be rejected")
	}
	r = validBase()
	r.NetworkDelayMS = 5
	r.NetworkFilter = "sometimes"
	if err := r.Validate(); err == nil {
		t.Error("an unknown network_filter should be rejected")
	}
	r = validBase()
	r.NetworkEmulationMethod = "tc-netem"
	r.NetworkFilter = "peers"
	r.NetworkInterface = "eth0"
	if err := r.Validate(); err != nil {
		t.Errorf("the documented defaults should be accepted: %v", err)
	}
	for _, bad := range []string{"a b", "; rm -rf /", "0123456789abcdef", ""} {
		r := validBase()
		r.NetworkInterface = bad
		if bad == "" {
			continue // empty means "use the default"
		}
		if err := r.Validate(); err == nil {
			t.Errorf("network_interface %q should be rejected", bad)
		}
	}
}

// TestEmulationAccessorsDefaults verifies the effective emulation settings a
// run actually uses when the configuration leaves them unset.
func TestEmulationAccessorsDefaults(t *testing.T) {
	r := validBase()
	if r.NetworkEmulated() {
		t.Error("no delay configured should not report emulation")
	}
	if got := r.EmulationMethod(); got != NetEmuTCNetem {
		t.Errorf("EmulationMethod() = %q, want %q", got, NetEmuTCNetem)
	}
	if got := r.EmulationIface(); got != NetIfaceEth0 {
		t.Errorf("EmulationIface() = %q, want %q", got, NetIfaceEth0)
	}
	if got := r.EmulationFilter(); got != NetFilterPeers {
		t.Errorf("EmulationFilter() = %q, want %q (client traffic must stay unaffected)", got, NetFilterPeers)
	}
	r.NetworkDelayMS = 5
	if !r.NetworkEmulated() {
		t.Error("a configured delay should report emulation")
	}
}
