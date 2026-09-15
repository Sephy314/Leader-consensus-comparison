// Package labcfg defines the experiment configuration shared by the runner,
// client, and report pipeline. Configurations are explicit JSON files; no
// experimental parameter is hard-coded into protocol or benchmark code.
package labcfg

import (
	"encoding/json"
	"fmt"
	"os"
)

// FailureMode enumerates the supported failure-injection modes.
type FailureMode string

const (
	FailureNone     FailureMode = "none"
	FailureLeader   FailureMode = "leader"   // Raft: kill the current leader
	FailureFollower FailureMode = "follower" // Raft: kill a non-leader replica
	FailureReplica  FailureMode = "replica"  // EPaxos: kill any replica
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
}

// Run is the full configuration of one experiment run.
type Run struct {
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

	// Protocol integration details (recorded, not tuned per protocol).
	GOMAXPROCS       int `json:"gomaxprocs"`
	RaftHeartbeatMS  int `json:"raft_heartbeat_ms"`
	RaftElectionMS   int `json:"raft_election_ms"`
	RaftSnapshotThr  int `json:"raft_snapshot_threshold"`
	RaftTrailingLogs int `json:"raft_trailing_logs"`
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
	case FailureNone, FailureLeader, FailureFollower, FailureReplica:
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
	if r.Failure.Mode == FailureReplica && r.Protocol != "epaxos" {
		return fmt.Errorf("failure mode %q requires protocol epaxos", r.Failure.Mode)
	}
	return nil
}
