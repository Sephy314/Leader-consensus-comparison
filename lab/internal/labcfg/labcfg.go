// Package labcfg defines the experiment configuration shared by the runner,
// client, and report pipeline. Configurations are explicit JSON files; no
// experimental parameter is hard-coded into protocol or benchmark code.
package labcfg

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// FailureMode enumerates the supported failure-injection modes.
type FailureMode string

const (
	FailureNone      FailureMode = "none"
	FailureLeader    FailureMode = "leader"    // Raft: kill the current leader
	FailureFollower  FailureMode = "follower"  // Raft: kill a non-leader replica
	FailureReplica   FailureMode = "replica"   // EPaxos: kill any replica
	FailureElection  FailureMode = "election"  // Raft: N failed elections via transport isolation
	FailurePartition FailureMode = "partition" // EPaxos: isolate a replica from its peers
)

// Failure describes one failure injection during the measured phase.
type Failure struct {
	Mode FailureMode `json:"mode"`
	// AtS is the offset in seconds into the measured phase at which the
	// failure is injected.
	AtS float64 `json:"at_s"`
	// RestartAfterS restarts the killed container after this many seconds
	// (0 = never restart). Used to observe recovery behavior.
	RestartAfterS float64 `json:"restart_after_s"`
	// FailedElections (failure mode "election") is the target number of
	// unsuccessful Raft election attempts to induce. The injection isolates
	// the cluster's Raft transport for FailedElections*election_timeout
	// seconds. The ACTUAL number of failed elections is measured from the
	// Raft leader-election log transitions, not assumed.
	FailedElections int `json:"failed_elections"`
}

// Run is the full configuration of one experiment run.
type Run struct {
	// Experiment is the experiment family this run belongs to:
	// "workload", "scaling", "conflict", "concurrency", "failure",
	// "pernode". Empty means derive it from the other fields.
	Experiment string `json:"experiment"`

	Protocol    string  `json:"protocol"` // "raft" | "epaxos"
	Replicas    int     `json:"replicas"`
	ReadPct     int     `json:"read_pct"`
	WritePct    int     `json:"write_pct"`
	Concurrency int     `json:"concurrency"`
	DurationS   int     `json:"duration_s"`
	WarmupS     int     `json:"warmup_s"`
	Repetitions int     `json:"repetitions"`
	Failure     Failure `json:"failure"`

	// Resource allocation (kept identical across protocols).
	ReplicaCPUs  float64 `json:"replica_cpus"`
	ReplicaMemMB int     `json:"replica_mem_mb"`
	ClientCPUs   float64 `json:"client_cpus"`
	ClientMemMB  int     `json:"client_mem_mb"`
	MasterCPUs   float64 `json:"master_cpus"`
	MasterMemMB  int     `json:"master_mem_mb"`

	// Workload parameters.
	Keyspace  int   `json:"keyspace"`
	TimeoutMS int   `json:"timeout_ms"`
	Seed      int64 `json:"seed"`

	// ConflictPct is the probability (0-100) that a request targets the
	// shared hot key instead of a uniformly random key. It controls the
	// command contention rate presented to the protocols. The resulting
	// hot-key fraction is recorded per request and reported as the measured
	// conflict rate; it is never assumed to equal ConflictPct.
	ConflictPct int `json:"conflict_pct"`
	// HotKeys is the number of distinct hot keys requests contend on when the
	// conflict branch is taken (default 1 = maximal contention).
	HotKeys int `json:"hot_keys"`

	// Implementation selects which implementation of the protocol is under
	// test. Empty means the protocol's primary implementation (the one used by
	// the original experiment matrix), so every pre-existing configuration
	// keeps its exact meaning. The implementation-sensitivity experiment
	// additionally evaluates "etcd" (go.etcd.io/raft/v3) and "nvb"
	// (github.com/nvanbenschoten/epaxos).
	Implementation string `json:"implementation"`

	// Protocol integration details (recorded, not tuned per protocol).
	GOMAXPROCS       int `json:"gomaxprocs"`
	RaftHeartbeatMS  int `json:"raft_heartbeat_ms"`
	RaftElectionMS   int `json:"raft_election_ms"`
	RaftSnapshotThr  int `json:"raft_snapshot_threshold"`
	RaftTrailingLogs int `json:"raft_trailing_logs"`

	// CommCostMS is the fixed one-way latency (ms) added to every
	// inter-replica message, simulating a real network. 0 = the local-network
	// baseline (no added latency).
	CommCostMS int `json:"comm_cost_ms"`
	// CommJitterPct is the jitter as a percentage of CommCostMS: each message
	// additionally waits a uniform random delay in [0, cost*jitter/100].
	CommJitterPct int `json:"comm_jitter_pct"`

	// PersistenceMode selects how the implementation persists consensus
	// state, independently of which implementation it is. Empty means the
	// implementation's default (see Persist), so every pre-existing
	// configuration keeps its exact meaning. The mode actually used is
	// recorded in metadata.json and reported; it is never inferred from the
	// implementation name.
	//
	// Only modes the implementation itself provides are accepted (see
	// PersistSupported). No persistence mechanism is invented for the
	// benchmark.
	PersistenceMode string `json:"persistence_mode"`

	// NetworkDelayMS is a one-way latency (ms) applied to inter-replica
	// traffic from OUTSIDE the implementations: the runner installs it with a
	// network-emulation tool on the replica's container interface. It is
	// therefore independent of implementation code -- no adapter sleeps and no
	// consensus-code changes. 0 = no emulation (the local-network baseline).
	//
	// This is deliberately separate from CommCostMS, the legacy in-adapter
	// per-message sleep used by the archived commcost experiment. A run that
	// sets both is rejected, and the two families are never pooled or
	// reinterpreted as one another.
	NetworkDelayMS int `json:"network_delay_ms"`
	// NetworkEmulationMethod names the mechanism used to apply NetworkDelayMS.
	// Defaults to "tc-netem" when NetworkDelayMS > 0.
	NetworkEmulationMethod string `json:"network_emulation_method"`
	// NetworkInterface is the container interface the emulation is applied to
	// (default "eth0").
	NetworkInterface string `json:"network_interface"`
	// NetworkFilter scopes which traffic the emulation delays: "peers" (only
	// traffic to other replicas, so client-observed latency is not polluted)
	// or "all". Defaults to "peers".
	NetworkFilter string `json:"network_filter"`

	// Correctness is the correctness-harness mode: run exactly this many
	// requests (0 = the timed benchmark). ValueBase makes PUT values
	// deterministic (valueBase + request id) so the harness can detect
	// missing and duplicate applications.
	Correctness int   `json:"correctness"`
	ValueBase   int64 `json:"value_base"`
}

// Implementation names. The two primary names are the implementations the
// original experiment matrix used; the two independent names are the ones
// added for the implementation-sensitivity experiment.
const (
	ImplHashicorp = "hashicorp" // Raft primary:    github.com/hashicorp/raft
	ImplEtcd      = "etcd"      // Raft independent: go.etcd.io/raft/v3
	ImplEtcdCore  = "etcd-core" // Raft analysis:    go.etcd.io/raft/v3 core only (no WAL, no fsync)
	ImplOriginal  = "original"  // EPaxos primary:   efficient/epaxos
	ImplNVB       = "nvb"       // EPaxos independent: github.com/nvanbenschoten/epaxos
)

// PrimaryImpl returns the primary implementation name for a protocol: the
// implementation whose results are already in the recorded dataset.
func PrimaryImpl(protocol string) string {
	if protocol == "epaxos" {
		return ImplOriginal
	}
	return ImplHashicorp
}

// Impl returns the effective implementation name. An empty Implementation
// field means the protocol's primary implementation.
func (r Run) Impl() string {
	if r.Implementation != "" {
		return r.Implementation
	}
	return PrimaryImpl(r.Protocol)
}

// IsPrimary reports whether this run uses the protocol's primary
// implementation. Primary runs keep their historical run IDs so the recorded
// dataset and the sensitivity dataset never collide.
func (r Run) IsPrimary() bool { return r.Impl() == PrimaryImpl(r.Protocol) }

// ImplSupported reports whether an implementation name is valid for a
// protocol.
func ImplSupported(protocol, impl string) bool {
	switch protocol {
	case "raft":
		return impl == ImplHashicorp || impl == ImplEtcd || impl == ImplEtcdCore
	case "epaxos":
		return impl == ImplOriginal || impl == ImplNVB
	}
	return false
}

// Persistence modes.
const (
	PersistDurable = "durable" // state survives a process/container restart
	PersistMemory  = "memory"  // in-memory only: state is lost on restart
)

// Network-emulation constants.
const (
	// NetEmuTCNetem is Linux `tc` with the netem queueing discipline, applied
	// inside the replica's own network namespace. Linux/WSL2 only.
	NetEmuTCNetem  = "tc-netem"
	NetFilterPeers = "peers"
	NetFilterAll   = "all"
	NetIfaceEth0   = "eth0"
	// MaxNetworkDelayMS bounds the emulated delay. Beyond it the measured
	// phase is dominated by client timeouts rather than by the protocol's
	// response, and the run measures the timeout configuration instead.
	MaxNetworkDelayMS = 100
)

// DefaultPersistence returns the persistence mode an implementation uses when
// the configuration does not name one. These are exactly the modes every
// recorded run already used, so an empty PersistenceMode leaves the recorded
// dataset's meaning untouched.
func DefaultPersistence(impl string) string {
	switch impl {
	case ImplHashicorp, ImplEtcd:
		return PersistDurable // BoltDB file / fsynced write-ahead log
	default: // etcd-core, original, nvb
		return PersistMemory
	}
}

// Persist returns the effective persistence mode of a run.
func (r Run) Persist() string {
	if r.PersistenceMode != "" {
		return r.PersistenceMode
	}
	return DefaultPersistence(r.Impl())
}

// PersistSupported reports whether an implementation provides a persistence
// mode, and PersistModes lists them in a stable order.
//
//   - hashicorp: durable (raft-boltdb BoltStore) | memory (raft.NewInmemStore)
//   - etcd:      durable (fsynced write-ahead log) | memory (raft.NewMemoryStorage,
//     the adapter's -no-wal mode)
//   - etcd-core: memory only (the core-only analysis mode has no WAL path)
//   - original:  durable (upstream -durable: stable-store file + fsync) |
//     memory (the upstream default)
//   - nvb:       memory only. The library's Storage interface ships an
//     in-memory implementation; a durable backend exists only in its demo
//     server (Badger-backed) and is not part of the library interface, so no
//     durable mode is offered here. The asymmetry is reported, not hidden.
func PersistSupported(impl, mode string) bool {
	switch impl {
	case ImplHashicorp, ImplEtcd, ImplOriginal:
		return mode == PersistDurable || mode == PersistMemory
	case ImplEtcdCore, ImplNVB:
		return mode == PersistMemory
	}
	return false
}

// PersistModes lists the persistence modes an implementation supports.
func PersistModes(impl string) []string {
	switch impl {
	case ImplHashicorp, ImplEtcd, ImplOriginal:
		return []string{PersistDurable, PersistMemory}
	case ImplEtcdCore, ImplNVB:
		return []string{PersistMemory}
	}
	return nil
}

// NetworkEmulated reports whether OS-level network emulation is configured.
func (r Run) NetworkEmulated() bool { return r.NetworkDelayMS > 0 }

// EmulationMethod returns the effective network-emulation mechanism.
func (r Run) EmulationMethod() string {
	if r.NetworkEmulationMethod != "" {
		return r.NetworkEmulationMethod
	}
	return NetEmuTCNetem
}

// EmulationIface returns the effective container interface name.
func (r Run) EmulationIface() string {
	if r.NetworkInterface != "" {
		return r.NetworkInterface
	}
	return NetIfaceEth0
}

// EmulationFilter returns the effective traffic scope of the emulation.
func (r Run) EmulationFilter() string {
	if r.NetworkFilter != "" {
		return r.NetworkFilter
	}
	return NetFilterPeers
}

// Default returns a Run with documented defaults applied.
func Default() Run {
	return Run{
		Protocol:         "raft",
		Replicas:         3,
		ReadPct:          50,
		WritePct:         50,
		Concurrency:      32,
		DurationS:        60,
		WarmupS:          10,
		Repetitions:      3,
		Failure:          Failure{Mode: FailureNone, AtS: 0, RestartAfterS: 0},
		ReplicaCPUs:      2.0,
		ReplicaMemMB:     512,
		ClientCPUs:       2.0,
		ClientMemMB:      512,
		MasterCPUs:       1.0,
		MasterMemMB:      256,
		Keyspace:         1000,
		TimeoutMS:        2000,
		Seed:             42,
		ConflictPct:      0,
		HotKeys:          1,
		GOMAXPROCS:       4,
		RaftHeartbeatMS:  1000,
		RaftElectionMS:   2000,
		RaftSnapshotThr:  8192,
		RaftTrailingLogs: 1024,
	}
}

// Load reads a Run from a JSON file, applying defaults for omitted fields.
func Load(path string) (Run, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// Validate checks the configuration for internal consistency.
func (r Run) Validate() error {
	if r.Protocol != "raft" && r.Protocol != "epaxos" {
		return fmt.Errorf("protocol must be raft or epaxos, got %q", r.Protocol)
	}
	if r.Implementation != "" && !ImplSupported(r.Protocol, r.Implementation) {
		return fmt.Errorf("implementation %q is not valid for protocol %q (raft: %s|%s|%s, epaxos: %s|%s)",
			r.Implementation, r.Protocol, ImplHashicorp, ImplEtcd, ImplEtcdCore, ImplOriginal, ImplNVB)
	}
	if r.Replicas < 1 || r.Replicas > 9 {
		return fmt.Errorf("replicas must be in [1,9], got %d", r.Replicas)
	}
	if r.ReadPct+r.WritePct != 100 {
		return fmt.Errorf("read_pct+write_pct must equal 100, got %d+%d", r.ReadPct, r.WritePct)
	}
	if r.Concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1")
	}
	if r.DurationS < 1 || r.WarmupS < 0 {
		return fmt.Errorf("duration_s >= 1 and warmup_s >= 0 required")
	}
	if r.Repetitions < 1 {
		return fmt.Errorf("repetitions must be >= 1")
	}
	switch r.Failure.Mode {
	case FailureNone, FailureLeader, FailureFollower, FailureReplica,
		FailureElection, FailurePartition:
	default:
		return fmt.Errorf("unknown failure mode %q", r.Failure.Mode)
	}
	if r.Failure.Mode != FailureNone && r.Failure.AtS <= 0 {
		return fmt.Errorf("failure.at_s must be > 0 when a failure is configured")
	}
	if r.Failure.Mode == FailureLeader && r.Protocol != "raft" {
		return fmt.Errorf("failure mode %q requires protocol raft", r.Failure.Mode)
	}
	if r.Failure.Mode == FailureFollower && r.Protocol != "raft" {
		return fmt.Errorf("failure mode %q requires protocol raft", r.Failure.Mode)
	}
	if r.Failure.Mode == FailureElection && r.Protocol != "raft" {
		return fmt.Errorf("failure mode %q requires protocol raft", r.Failure.Mode)
	}
	// failed_elections = 0 is the baseline (no failed elections induced);
	// negative values are invalid.
	if r.Failure.Mode == FailureElection && r.Failure.FailedElections < 0 {
		return fmt.Errorf("failure.failed_elections must be >= 0 for election mode")
	}
	// The measured phase must be long enough for the longest isolation
	// (FailedElections * election_timeout) to expire and recovery to
	// complete (up to two more randomized election timeouts, plus margin).
	// Otherwise the cluster cannot reach its final state within the phase
	// and the run would be recorded as failed by design.
	if r.Failure.Mode == FailureElection {
		isoS := float64(r.Failure.FailedElections) * float64(r.RaftElectionMS) / 1000.0
		recoverS := 2 * float64(r.RaftElectionMS) / 1000.0
		if r.Failure.AtS+isoS+recoverS+2.0 > float64(r.DurationS) {
			return fmt.Errorf(
				"election mode: at_s(%.1f) + isolation(%.1fs) + recovery(%.1fs) + margin(2s) = %.1fs exceeds duration_s(%d); the cluster could not recover within the measured phase",
				r.Failure.AtS, isoS, recoverS, r.Failure.AtS+isoS+recoverS+2.0, r.DurationS)
		}
	}
	if r.Failure.Mode == FailureReplica && r.Protocol != "epaxos" {
		return fmt.Errorf("failure mode %q requires protocol epaxos", r.Failure.Mode)
	}
	if r.Failure.Mode == FailurePartition && r.Protocol != "epaxos" {
		return fmt.Errorf("failure mode %q requires protocol epaxos", r.Failure.Mode)
	}
	if r.ConflictPct < 0 || r.ConflictPct > 100 {
		return fmt.Errorf("conflict_pct must be in [0,100], got %d", r.ConflictPct)
	}
	if r.HotKeys < 1 {
		return fmt.Errorf("hot_keys must be >= 1, got %d", r.HotKeys)
	}
	if r.HotKeys > r.Keyspace {
		return fmt.Errorf("hot_keys (%d) must not exceed keyspace (%d)", r.HotKeys, r.Keyspace)
	}
	if r.PersistenceMode != "" && !PersistSupported(r.Impl(), r.PersistenceMode) {
		modes := PersistModes(r.Impl())
		if len(modes) == 0 {
			return fmt.Errorf("unknown implementation %q", r.Impl())
		}
		return fmt.Errorf("implementation %q does not provide persistence mode %q (it provides: %s)",
			r.Impl(), r.PersistenceMode, strings.Join(modes, "|"))
	}
	if r.NetworkDelayMS < 0 || r.NetworkDelayMS > MaxNetworkDelayMS {
		return fmt.Errorf("network_delay_ms must be in [0,%d], got %d", MaxNetworkDelayMS, r.NetworkDelayMS)
	}
	// The legacy in-adapter sleep and the OS-level emulation must never be
	// combined: the run would not be attributable to either mechanism.
	if r.NetworkDelayMS > 0 && (r.CommCostMS > 0 || r.CommJitterPct > 0) {
		return fmt.Errorf("network_delay_ms (%d) must not be combined with the legacy in-adapter comm_cost_ms (%d) / comm_jitter_pct (%d): the two mechanisms would be confounded",
			r.NetworkDelayMS, r.CommCostMS, r.CommJitterPct)
	}
	switch r.NetworkEmulationMethod {
	case "", NetEmuTCNetem:
	default:
		return fmt.Errorf("network_emulation_method must be %q, got %q", NetEmuTCNetem, r.NetworkEmulationMethod)
	}
	switch r.NetworkFilter {
	case "", NetFilterPeers, NetFilterAll:
	default:
		return fmt.Errorf("network_filter must be %q or %q, got %q", NetFilterPeers, NetFilterAll, r.NetworkFilter)
	}
	if r.NetworkInterface != "" && !validIfaceName(r.NetworkInterface) {
		return fmt.Errorf("network_interface %q is not a valid interface name", r.NetworkInterface)
	}
	return nil
}

// validIfaceName bounds the interface name to what a Linux interface name can
// be (and to a conservative ASCII subset, since the name is passed to `tc`).
func validIfaceName(name string) bool {
	if len(name) == 0 || len(name) > 15 { // IFNAMSIZ-1
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '.', c == '-':
		default:
			return false
		}
	}
	return true
}
