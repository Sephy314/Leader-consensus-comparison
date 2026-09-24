package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"conslab/internal/labcfg"
)

// composeModel is the data used to render a run's compose file.
type composeModel struct {
	Project      string
	Image        string
	Protocol     string
	Replicas     int
	ResultsDir   string // absolute host path bind-mounted at /out
	MasterCPUs   float64
	MasterMemMB  int
	ReplicaCPUs  float64
	ReplicaMemMB int
	ClientCPUs   float64
	ClientMemMB  int
	MasterCmd    []string
	ReplicaCmds  [][]string
	ClientCmd    []string
	DataVolumes  bool // Raft replicas persist state to named volumes
	RaftPort     int
	ClientPort   int
}

const composeTemplate = `name: {{.Project}}
services:
  master:
    image: {{.Image}}
    command: {{cmd .MasterCmd}}
    cpus: {{.MasterCPUs}}
    mem_limit: {{.MasterMemMB}}m
    ports:
      - "127.0.0.1:{{.MasterHostPort}}:{{.MasterPort}}"
    networks: [bench]
{{- if .ReplicaServices}}
{{.ReplicaServices}}
{{- end}}
  client:
    image: {{.Image}}
    command: {{cmd .ClientCmd}}
    cpus: {{.ClientCPUs}}
    mem_limit: {{.ClientMemMB}}m
    volumes:
      - {{.ResultsDir}}:/out
    depends_on:
      - master
{{- range .ReplicaNames}}
      - {{.}}
{{- end}}
    networks: [bench]
networks:
  bench:
    driver: bridge
{{- if .VolumeDefs}}
volumes:
{{- range .VolumeNames}}
  {{.}}: {}
{{- end}}
{{- end}}
`

// renderCompose writes the compose file for a run and returns its path.
func renderCompose(dir string, cfg labcfg.Run, runID string) (string, error) {
	model, err := buildComposeModel(dir, cfg, runID)
	if err != nil {
		return "", err
	}
	// A small adapter struct so the TEMPLATE stays simple.
	data := struct {
		composeModel
		MasterHostPort  string
		MasterPort      int
		ReplicaServices string
		ReplicaNames    []string
		VolumeNames     []string
		VolumeDefs      bool
	}{
		composeModel:   model,
		MasterHostPort: masterHostPort,
		MasterPort:     7087,
		ReplicaNames:   replicaServiceNames(cfg.Replicas),
	}
	// Build replica service blocks and volumes.
	var svc strings.Builder
	for i := 0; i < cfg.Replicas; i++ {
		fmt.Fprintf(&svc, "  replica%d:\n", i)
		fmt.Fprintf(&svc, "    image: %s\n", model.Image)
		fmt.Fprintf(&svc, "    command: %s\n", yamlList(model.ReplicaCmds[i]))
		fmt.Fprintf(&svc, "    cpus: %v\n", model.ReplicaCPUs)
		fmt.Fprintf(&svc, "    mem_limit: %dm\n", model.ReplicaMemMB)
		// Expose each replica's admin RPC (clientPort+1000) on a host port so
		// the runner can inject failures (IsolateElections) and query Stats
		// from the host. Runs are sequential, so fixed ports are safe.
		fmt.Fprintf(&svc, "    ports:\n      - \"127.0.0.1:%d:%d\"\n", replicaAdminHostPort(i), 8070)
		if model.DataVolumes {
			fmt.Fprintf(&svc, "    volumes:\n      - raftdata%d:/data\n", i)
		}
		fmt.Fprintf(&svc, "    networks: [bench]\n")
	}
	data.ReplicaServices = strings.TrimRight(svc.String(), "\n")
	if model.DataVolumes {
		data.VolumeDefs = true
		for i := 0; i < cfg.Replicas; i++ {
			data.VolumeNames = append(data.VolumeNames, fmt.Sprintf("raftdata%d", i))
		}
	}
	tmpl, err := template.New("compose").Funcs(template.FuncMap{
		"cmd": func(args []string) string { return yamlList(args) },
	}).Parse(composeTemplate)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "compose.yaml")
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := tmpl.Execute(f, data); err != nil {
		return "", err
	}
	return path, nil
}

// masterHostPort is a fixed host port for the master so the runner can poll
// the master RPC from the host. Runs are executed sequentially.
const masterHostPort = "17087"

// replicaAdminHostPort returns the host port exposing replica i's admin RPC
// (clientPort+1000 = 8070 inside the container).
func replicaAdminHostPort(i int) int {
	return 18070 + i
}

func buildComposeModel(dir string, cfg labcfg.Run, runID string) (composeModel, error) {
	resultsAbs, err := filepath.Abs(dir)
	if err != nil {
		return composeModel{}, err
	}
	m := composeModel{
		Project:      runID,
		Image:        imageTag,
		Protocol:     cfg.Protocol,
		Replicas:     cfg.Replicas,
		ResultsDir:   resultsAbs,
		MasterCPUs:   cfg.MasterCPUs,
		MasterMemMB:  cfg.MasterMemMB,
		ReplicaCPUs:  cfg.ReplicaCPUs,
		ReplicaMemMB: cfg.ReplicaMemMB,
		ClientCPUs:   cfg.ClientCPUs,
		ClientMemMB:  cfg.ClientMemMB,
		ClientPort:   7070,
		RaftPort:     6000,
	}
	// The (protocol, implementation) pair selects the replica and master
	// binaries. This is the only place in the lab where a protocol or
	// implementation name maps to a command line; the benchmark client,
	// workload, metrics, and analysis are shared by all four implementations.
	switch cfg.Impl() {
	case labcfg.ImplHashicorp:
		m.DataVolumes = true
		m.MasterCmd = []string{"raftmaster", "-port", "7087", "-n", itoa(cfg.Replicas)}
		for i := 0; i < cfg.Replicas; i++ {
			m.ReplicaCmds = append(m.ReplicaCmds, []string{
				"raftadapter",
				"-master", "master:7087",
				"-addr", fmt.Sprintf("replica%d", i),
				"-client-port", "7070",
				"-raft-port", "6000",
				"-dir", "/data",
				"-gomaxprocs", itoa(cfg.GOMAXPROCS),
				"-heartbeat-ms", itoa(cfg.RaftHeartbeatMS),
				"-election-ms", itoa(cfg.RaftElectionMS),
				"-snapshot-threshold", itoa(cfg.RaftSnapshotThr),
				"-trailing-logs", itoa(cfg.RaftTrailingLogs),
			})
		}
	case labcfg.ImplEtcd:
		// Raft B: go.etcd.io/raft/v3. Same master (the lab's raft master),
		// same client port, same Raft transport port, same persistent
		// /data volume. The election/heartbeat timeouts are expressed in
		// ticks by the etcd library; the adapter converts the same
		// millisecond timeouts into ticks so both Raft implementations run
		// with the same effective timeouts.
		m.DataVolumes = true
		m.MasterCmd = []string{"raftmaster", "-port", "7087", "-n", itoa(cfg.Replicas)}
		for i := 0; i < cfg.Replicas; i++ {
			m.ReplicaCmds = append(m.ReplicaCmds, []string{
				"etcdraftadapter",
				"-master", "master:7087",
				"-addr", fmt.Sprintf("replica%d", i),
				"-client-port", "7070",
				"-raft-port", "6000",
				"-dir", "/data",
				"-gomaxprocs", itoa(cfg.GOMAXPROCS),
				"-heartbeat-ms", itoa(cfg.RaftHeartbeatMS),
				"-election-ms", itoa(cfg.RaftElectionMS),
				"-trailing-logs", itoa(cfg.RaftTrailingLogs),
			})
		}
	case labcfg.ImplEtcdCore:
		// Raft core-only analysis mode: the same etcd raft core with
		// in-memory storage only (no write-ahead log, no fsync). This
		// isolates the core's throughput from the durability path; it is
		// reported as its own implementation, never as a peer of the
		// durable ones.
		m.DataVolumes = false
		m.MasterCmd = []string{"raftmaster", "-port", "7087", "-n", itoa(cfg.Replicas)}
		for i := 0; i < cfg.Replicas; i++ {
			m.ReplicaCmds = append(m.ReplicaCmds, []string{
				"etcdraftadapter",
				"-master", "master:7087",
				"-addr", fmt.Sprintf("replica%d", i),
				"-client-port", "7070",
				"-raft-port", "6000",
				"-dir", "/data",
				"-no-wal",
				"-gomaxprocs", itoa(cfg.GOMAXPROCS),
				"-heartbeat-ms", itoa(cfg.RaftHeartbeatMS),
				"-election-ms", itoa(cfg.RaftElectionMS),
				"-trailing-logs", itoa(cfg.RaftTrailingLogs),
			})
		}
	case labcfg.ImplOriginal:
		m.MasterCmd = []string{"epaxos-master", "-port", "7087", "-N", itoa(cfg.Replicas)}
		for i := 0; i < cfg.Replicas; i++ {
			m.ReplicaCmds = append(m.ReplicaCmds, []string{
				"epaxos-server",
				"-port", "7070",
				"-maddr", "master",
				"-mport", "7087",
				"-addr", fmt.Sprintf("replica%d", i),
				"-e", "-exec", "-dreply",
				"-p", itoa(cfg.GOMAXPROCS),
			})
		}
	case labcfg.ImplNVB:
		// EPaxos B: github.com/nvanbenschoten/epaxos. Uses the upstream
		// EPaxos master unchanged. The EPaxos library pushes transport and
		// storage onto the integrator, so the adapter runs the benchmark
		// client protocol on the client port and the library's peer
		// transport on a separate port.
		m.MasterCmd = []string{"epaxos-master", "-port", "7087", "-N", itoa(cfg.Replicas)}
		for i := 0; i < cfg.Replicas; i++ {
			m.ReplicaCmds = append(m.ReplicaCmds, []string{
				"nvbepaxos-server",
				"-port", "7070",
				"-peer-port", "6000",
				"-maddr", "master",
				"-mport", "7087",
				"-addr", fmt.Sprintf("replica%d", i),
				"-p", itoa(cfg.GOMAXPROCS),
			})
		}
	}
	// Communication cost and jitter are injected in the adapters' send path
	// (a per-message delay before the write). The flags are identical across
	// implementations, so they are appended here rather than in each case.
	if cfg.CommCostMS > 0 || cfg.CommJitterPct > 0 {
		for i := range m.ReplicaCmds {
			m.ReplicaCmds[i] = append(m.ReplicaCmds[i],
				"-comm-cost-ms", itoa(cfg.CommCostMS),
				"-comm-jitter-pct", itoa(cfg.CommJitterPct))
		}
	}
	m.ClientCmd = []string{
		"client",
		"-maddr", "master",
		"-mport", "7087",
		"-protocol", cfg.Protocol,
		"-impl", cfg.Impl(),
		"-replicas", itoa(cfg.Replicas),
		"-w", itoa(cfg.WritePct),
		"-c", itoa(cfg.Concurrency),
		"-conflict", itoa(cfg.ConflictPct),
		"-hot-keys", itoa(cfg.HotKeys),
		"-duration", itoa(cfg.DurationS),
		"-warmup", itoa(cfg.WarmupS),
		"-out", "/out",
		"-run-id", runID,
		"-seed", i64(cfg.Seed),
		"-keyspace", itoa(cfg.Keyspace),
		"-timeout-ms", itoa(cfg.TimeoutMS),
		"-gomaxprocs", itoa(cfg.GOMAXPROCS),
	}
	// Correctness mode: a fixed request count with deterministic values.
	if cfg.Correctness > 0 {
		m.ClientCmd = append(m.ClientCmd,
			"-correctness", itoa(cfg.Correctness),
			"-value-base", i64(cfg.ValueBase))
	}
	return m, nil
}

// yamlList renders a Go string slice as a YAML flow sequence.
func yamlList(args []string) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = fmt.Sprintf("%q", a)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func itoa(i int) string  { return fmt.Sprintf("%d", i) }
func i64(i int64) string { return fmt.Sprintf("%d", i) }

// replicaServiceNames returns the compose service names of the replicas
// (used by depends_on, which refers to services, not container names).
func replicaServiceNames(n int) []string {
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = fmt.Sprintf("replica%d", i)
	}
	return names
}

// containerName returns the compose-generated container name for a service.
func containerName(runID, service string) string {
	return fmt.Sprintf("%s-%s-1", runID, service)
}
