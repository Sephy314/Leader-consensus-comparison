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
	if cfg.Protocol == "raft" {
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
	} else {
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
	}
	m.ClientCmd = []string{
		"client",
		"-maddr", "master",
		"-mport", "7087",
		"-protocol", cfg.Protocol,
		"-replicas", itoa(cfg.Replicas),
		"-w", itoa(cfg.WritePct),
		"-c", itoa(cfg.Concurrency),
		"-duration", itoa(cfg.DurationS),
		"-warmup", itoa(cfg.WarmupS),
		"-out", "/out",
		"-run-id", runID,
		"-seed", i64(cfg.Seed),
		"-keyspace", itoa(cfg.Keyspace),
		"-timeout-ms", itoa(cfg.TimeoutMS),
		"-gomaxprocs", itoa(cfg.GOMAXPROCS),
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
