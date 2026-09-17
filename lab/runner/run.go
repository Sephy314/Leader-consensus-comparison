package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"conslab/internal/labcfg"
	"conslab/internal/proto"
	"conslab/monitor"
)

// runResult summarizes one executed run.
type runResult struct {
	RunID  string
	Dir    string
	Status string // success | failed
	Reason string
}

// eventLog appends timestamped events to events.csv.
type eventLog struct {
	mu sync.Mutex
	f  *os.File
	w  *csv.Writer
}

func newEventLog(path string) (*eventLog, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	w.Write([]string{"run_id", "ts_ns", "event", "detail"})
	w.Flush()
	return &eventLog{f: f, w: w}, nil
}

func (e *eventLog) log(runID, event, detail string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.w.Write([]string{runID, strconv.FormatInt(time.Now().UnixNano(), 10), event, detail})
	e.w.Flush()
}

func (e *eventLog) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.w.Flush()
	e.f.Close()
}

// executeRun runs one experiment repetition from start to finish.
func executeRun(cfg labcfg.Run, rep int) runResult {
	if err := cfg.Validate(); err != nil {
		return runResult{RunID: runIDFor(cfg, rep), Status: "failed", Reason: "invalid config: " + err.Error()}
	}
	runID, dir := makeRunDir(cfg, rep)
	res := runResult{RunID: runID, Dir: dir, Status: "failed"}
	started := time.Now()

	events, err := newEventLog(filepath.Join(dir, "events.csv"))
	if err != nil {
		res.Reason = err.Error()
		return res
	}
	defer events.close()
	events.log(runID, "run_started", "")

	composePath, err := renderCompose(dir, cfg, runID)
	if err != nil {
		res.Reason = "compose render: " + err.Error()
		writeMetadata(dir, cfg, rep, runID, res, started, time.Now())
		return res
	}

	if err := composeUp(dir, runID, composePath); err != nil {
		res.Reason = "compose up: " + err.Error()
		events.log(runID, "status", "failed: "+res.Reason)
		writeMetadata(dir, cfg, rep, runID, res, started, time.Now())
		composeDown(dir, runID, composePath)
		return res
	}
	defer composeDown(dir, runID, composePath)

	// Start the host-side resource monitor immediately; sampling skips
	// containers that are not yet running.
	stopMonitor := startMonitor(dir, cfg, runID)

	// Wait for the measured phase to begin.
	phaseStart, err := waitPhaseStart(dir, time.Duration(cfg.WarmupS+180)*time.Second)
	if err != nil {
		stopMonitor()
		res.Reason = "waiting for measured phase: " + err.Error()
		events.log(runID, "status", "failed: "+res.Reason)
		writeMetadata(dir, cfg, rep, runID, res, started, time.Now())
		return res
	}
	events.log(runID, "phase_started", "")
	fmt.Printf("[%s] measured phase started\n", runID)

	// Poll the master for leader changes (recorded only on change).
	stopLeader := startLeaderWatch(cfg, runID, events)

	// Inject the failure if configured.
	failDone := make(chan struct{})
	if cfg.Failure.Mode != labcfg.FailureNone {
		go func() {
			defer close(failDone)
			injectFailure(cfg, runID, phaseStart, events)
		}()
	} else {
		close(failDone)
	}

	// Wait for the client to finish.
	clientName := containerName(runID, "client")
	code, err := waitContainerExit(clientName, time.Duration(cfg.DurationS+180)*time.Second)
	stopLeader()
	stopMonitor()
	<-failDone

	if err != nil {
		res.Reason = err.Error()
		killContainer(clientName)
		events.log(runID, "status", "failed: "+res.Reason)
		writeMetadata(dir, cfg, rep, runID, res, started, time.Now())
		return res
	}
	events.log(runID, "client_exit", strconv.Itoa(code))
	events.log(runID, "run_finished", "")

	// Collect per-replica protocol counters (EPaxos fast/slow path) while the
	// containers are still up.
	collectStats(dir, cfg, runID)

	// Validate that the run produced usable raw data.
	if reason := validateRunOutputs(dir); reason != "" {
		res.Reason = reason
	} else {
		res.Status = "success"
	}
	events.log(runID, "status", res.Status+" "+res.Reason)
	writeMetadata(dir, cfg, rep, runID, res, started, time.Now())
	return res
}

// collectStats queries every replica's cumulative protocol counters and
// writes them to stats.json. For EPaxos these are the fast/slow path counts
// (instrumented in the upstream); for Raft the counters are zero.
func collectStats(dir string, cfg labcfg.Run, runID string) {
	if cfg.Protocol != "epaxos" {
		return
	}
	stats := make([]map[string]any, 0, cfg.Replicas)
	for i := 0; i < cfg.Replicas; i++ {
		s, err := replicaStats(i)
		if err != nil {
			stats = append(stats, map[string]any{"replica": i, "error": err.Error()})
			continue
		}
		stats = append(stats, map[string]any{
			"replica":    i,
			"fast_path":  s.FastPath,
			"slow_path":  s.SlowPath,
			"conflicted": s.Conflicted,
		})
	}
	data, _ := json.MarshalIndent(stats, "", "  ")
	os.WriteFile(filepath.Join(dir, "stats.json"), data, 0o644)
}

func makeRunDir(cfg labcfg.Run, rep int) (string, string) {
	base := runIDFor(cfg, rep)
	root := filepath.Join(resultsDir(), "raw")
	id := base
	for i := 2; ; i++ {
		dir := filepath.Join(root, id)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				panic(err)
			}
			return id, dir
		}
		id = fmt.Sprintf("%s-%d", base, i)
	}
}

// validateRunOutputs checks that a run produced usable raw data.
func validateRunOutputs(dir string) string {
	reqPath := filepath.Join(dir, "requests.csv")
	fi, err := os.Stat(reqPath)
	if err != nil {
		return "requests.csv missing"
	}
	if fi.Size() == 0 {
		return "requests.csv empty"
	}
	f, err := os.Open(reqPath)
	if err != nil {
		return "requests.csv unreadable"
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return "requests.csv unparseable"
	}
	if len(rows) < 2 {
		return "requests.csv has no data rows"
	}
	hdr := map[string]int{}
	for i, h := range rows[0] {
		hdr[h] = i
	}
	okIdx, hasOK := hdr["ok"]
	if !hasOK {
		return "requests.csv missing ok column"
	}
	total, ok := 0, 0
	for _, row := range rows[1:] {
		total++
		if row[okIdx] == "1" {
			ok++
		}
	}
	if total == 0 {
		return "requests.csv has no requests"
	}
	if ok == 0 {
		return fmt.Sprintf("all %d requests failed", total)
	}
	resPath := filepath.Join(dir, "resources.csv")
	rfi, err := os.Stat(resPath)
	if err != nil {
		return "resources.csv missing"
	}
	if rfi.Size() == 0 {
		return "resources.csv empty"
	}
	return ""
}

func writeMetadata(dir string, cfg labcfg.Run, rep int, runID string, res runResult, started, finished time.Time) {
	meta := map[string]any{
		"run_id":         runID,
		"experiment":     experimentName(cfg),
		"repetition":     rep,
		"config":         cfg,
		"status":         res.Status,
		"failure_reason": nullable(res.Reason),
		"started_at":     started.UTC().Format(time.RFC3339Nano),
		"finished_at":    finished.UTC().Format(time.RFC3339Nano),
		"duration_s":     finished.Sub(started).Seconds(),
		"versions":       versionInfo(),
		"host":           hostInfo(),
		"monitoring": map[string]any{
			"method":     "host-side cgroup v2 cpu.stat + /proc/<pid>/net/dev + /proc/<pid>/status",
			"interval_s": monitorInterval.Seconds(),
			"resolution": "~200ms; recovery timings derived from this are approximate",
		},
		"read_semantics": readSemantics(cfg.Protocol),
		"notes":          notes(cfg),
	}
	data, _ := json.MarshalIndent(meta, "", "  ")
	os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0o644)
}

// readSemantics documents the consistency semantics of the read path for
// each protocol, so the Read benchmark is interpreted under the correct
// equivalence. Both protocols route reads through consensus; the difference
// is the coordination mechanism.
func readSemantics(protocol string) map[string]any {
	if protocol == "raft" {
		return map[string]any{
			"system":                "raft",
			"read_consistency":      "linearizable",
			"read_path":             "client -> leader (raft.Raft.Leader) -> raft.Apply(GET) -> quorum commit -> state read -> reply",
			"coordination_required": true,
			"target_replica":        "leader",
			"mechanism":             "leader-confirmed, quorum-based (every read is a consensus command; no local-read optimization)",
		}
	}
	return map[string]any{
		"system":                "epaxos",
		"read_consistency":      "linearizable",
		"read_path":             "client -> any replica (round-robin) -> consensus command -> executed at all replicas in dependency order -> reply",
		"coordination_required": true,
		"target_replica":        "any (leaderless)",
		"mechanism":             "quorum-based, dependency-ordered execution (reads and writes share the same consensus path)",
	}
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// notes records protocol integration details that differ between protocols.
func notes(cfg labcfg.Run) []string {
	common := []string{
		fmt.Sprintf("all containers share one image (%s) and the same CPU/memory allocation", imageTag),
		fmt.Sprintf("replica cpus=%v mem=%dMB; client cpus=%v mem=%dMB; master cpus=%v mem=%dMB",
			cfg.ReplicaCPUs, cfg.ReplicaMemMB, cfg.ClientCPUs, cfg.ClientMemMB, cfg.MasterCPUs, cfg.MasterMemMB),
		fmt.Sprintf("GOMAXPROCS=%d for every replica in both protocols", cfg.GOMAXPROCS),
		"reads (GET) are routed through consensus in both protocols",
	}
	if cfg.Protocol == "raft" {
		return append(common,
			"Raft: github.com/hashicorp/raft v1.7.3 performs all consensus; the adapter only converts benchmark requests into raft.Apply calls",
			"Raft: client requests are sent to the leader reported by raft.Raft.Leader() via the raft master",
			fmt.Sprintf("Raft: heartbeat=%dms election=%dms snapshot_threshold=%d trailing_logs=%d",
				cfg.RaftHeartbeatMS, cfg.RaftElectionMS, cfg.RaftSnapshotThr, cfg.RaftTrailingLogs),
			"Raft: state is persisted to a per-replica boltdb volume (survives replica restart)",
		)
	}
	return append(common,
		"EPaxos: upstream efficient/epaxos server is used unchanged (-e -exec -dreply)",
		"EPaxos: client requests are spread round-robin across replicas (leaderless)",
		"EPaxos: upstream master is used unchanged; its reported leader is bookkeeping only, not used by the client",
		"EPaxos: replica state is in memory (upstream default, no -durable); a restarted replica loses local state",
	)
}

// ---- docker helpers ----

const monitorInterval = 200 * time.Millisecond

func composeUp(dir, runID, composePath string) error {
	return runQuiet(dir, "docker", "compose", "-p", runID, "-f", composePath, "up", "-d", "--quiet-pull")
}

func composeDown(dir, runID, composePath string) {
	// Record container logs before teardown for post-mortem debugging.
	logsPath := filepath.Join(dir, "container-logs.txt")
	if f, err := os.Create(logsPath); err == nil {
		cmd := exec.Command("docker", "compose", "-p", runID, "-f", composePath, "logs", "--no-color")
		cmd.Stdout, cmd.Stderr = f, f
		_ = cmd.Run()
		f.Close()
	}
	_ = runQuiet(dir, "docker", "compose", "-p", runID, "-f", composePath, "down", "-v", "--timeout", "5")
}

func killContainer(name string) error {
	return exec.Command("docker", "kill", name).Run()
}

func startContainer(name string) error {
	return exec.Command("docker", "start", name).Run()
}

func waitContainerExit(name string, timeout time.Duration) (int, error) {
	type result struct {
		code int
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		out, err := exec.Command("docker", "wait", name).Output()
		if err != nil {
			ch <- result{-1, err}
			return
		}
		code, err := strconv.Atoi(strings.TrimSpace(string(out)))
		ch <- result{code, err}
	}()
	select {
	case r := <-ch:
		return r.code, r.err
	case <-time.After(timeout):
		return -1, fmt.Errorf("client container %s did not exit within %v", name, timeout)
	}
}

// ---- master RPC from the host ----

func dialMasterRPC(retries int) (*rpc.Client, error) {
	var lastErr error
	for i := 0; i < retries; i++ {
		cli, err := rpc.DialHTTP("tcp", "127.0.0.1:"+masterHostPort)
		if err == nil {
			return cli, nil
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	return nil, lastErr
}

func masterLeader(cli *rpc.Client) int {
	done := make(chan int, 1)
	go func() {
		var reply proto.GetLeaderReply
		if err := cli.Call("Master.GetLeader", new(proto.GetLeaderArgs), &reply); err == nil {
			done <- reply.LeaderId
			return
		}
		done <- -1
	}()
	select {
	case l := <-done:
		return l
	case <-time.After(2 * time.Second):
		return -1
	}
}

// ---- phase / monitor / leader watch ----

func waitPhaseStart(dir string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	path := filepath.Join(dir, "phase.json")
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var m map[string]int64
			if err := json.Unmarshal(data, &m); err == nil {
				if ns, ok := m["phase_started_ns"]; ok {
					return time.Unix(0, ns), nil
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return time.Time{}, fmt.Errorf("phase.json not written within %v", timeout)
}

// startMonitor samples all containers until the returned stop function is
// called.
func startMonitor(dir string, cfg labcfg.Run, runID string) func() {
	targets := []monitor.Target{{Role: "master", ReplicaID: -1, Container: containerName(runID, "master")}}
	for i := 0; i < cfg.Replicas; i++ {
		targets = append(targets, monitor.Target{
			Role: "replica", ReplicaID: i,
			Container: containerName(runID, fmt.Sprintf("replica%d", i)),
		})
	}
	targets = append(targets, monitor.Target{Role: "client", ReplicaID: -1, Container: containerName(runID, "client")})

	f, err := os.Create(filepath.Join(dir, "resources.csv"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "monitor: %v\n", err)
		return func() {}
	}
	w := csv.NewWriter(f)
	w.Write([]string{
		"run_id", "protocol", "role", "replica_id", "container", "ts_ns",
		"cpu_usage_usec", "cpu_user_usec", "cpu_system_usec", "net_rx_bytes", "net_tx_bytes", "rss_bytes",
	})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer f.Close()
		ticker := time.NewTicker(monitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				w.Flush()
				return
			case <-ticker.C:
				for _, s := range monitor.SampleAll(runID, cfg.Protocol, targets) {
					w.Write([]string{
						s.RunID, s.Protocol, s.Role, strconv.Itoa(s.ReplicaID), s.Container,
						strconv.FormatInt(s.TS.UnixNano(), 10),
						strconv.FormatInt(s.CPUUsageUS, 10), strconv.FormatInt(s.CPUUserUS, 10),
						strconv.FormatInt(s.CPUSystemUS, 10), strconv.FormatInt(s.NetRXBytes, 10),
						strconv.FormatInt(s.NetTXBytes, 10), strconv.FormatInt(s.RSSBytes, 10),
					})
				}
				w.Flush()
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// startLeaderWatch polls the master for leader changes and records them.
func startLeaderWatch(cfg labcfg.Run, runID string, events *eventLog) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		cli, err := dialMasterRPC(20)
		if err != nil {
			return
		}
		last := -2
		ticker := time.NewTicker(monitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				l := masterLeader(cli)
				if l != last {
					events.log(runID, "leader_observed", strconv.Itoa(l))
					last = l
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

// masterReplicaHost maps a replica index (as reported by the master) to the
// container hostname, using the master's node list. The node list order is
// registration order, which is NOT the container numbering, so index-based
// container names would kill the wrong replica.
func masterReplicaHost(cli *rpc.Client, index int) string {
	done := make(chan string, 1)
	go func() {
		var reply proto.GetReplicaListReply
		if err := cli.Call("Master.GetReplicaList", new(proto.GetReplicaListArgs), &reply); err == nil && reply.Ready && index >= 0 && index < len(reply.ReplicaList) {
			done <- strings.Split(reply.ReplicaList[index], ":")[0]
			return
		}
		done <- ""
	}()
	select {
	case h := <-done:
		return h
	case <-time.After(2 * time.Second):
		return ""
	}
}

// replicaRPC dials replica i's admin RPC (exposed on a host port).
func replicaRPC(i int) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", fmt.Sprintf("127.0.0.1:%d", replicaAdminHostPort(i)))
}

// isolateReplica asks replica i to isolate its Raft transport for the given
// duration (election-failure injection). DurationMS = 0 clears the isolation.
func isolateReplica(i int, durationMS int64) error {
	cli, err := replicaRPC(i)
	if err != nil {
		return err
	}
	defer cli.Close()
	var reply proto.IsolateReply
	return cli.Call("Replica.IsolateElections", &proto.IsolateArgs{DurationMS: durationMS}, &reply)
}

// replicaStats queries replica i's cumulative protocol counters.
func replicaStats(i int) (proto.StatsReply, error) {
	var reply proto.StatsReply
	cli, err := replicaRPC(i)
	if err != nil {
		return reply, err
	}
	defer cli.Close()
	err = cli.Call("Replica.Stats", new(proto.StatsArgs), &reply)
	return reply, err
}

// injectFailure kills the configured target container at the configured time
// into the measured phase.
func injectFailure(cfg labcfg.Run, runID string, phaseStart time.Time, events *eventLog) {
	target := phaseStart.Add(time.Duration(cfg.Failure.AtS * float64(time.Second)))
	if wait := time.Until(target); wait > 0 {
		time.Sleep(wait)
	}

	var container string
	switch cfg.Failure.Mode {
	case labcfg.FailureLeader:
		cli, err := dialMasterRPC(20)
		if err != nil {
			events.log(runID, "failure_error", "master unreachable: "+err.Error())
			return
		}
		leader := masterLeader(cli)
		if leader < 0 {
			events.log(runID, "failure_error", "no leader reported at injection time")
			return
		}
		host := masterReplicaHost(cli, leader)
		if host == "" {
			events.log(runID, "failure_error", "could not map leader index to host")
			return
		}
		container = containerName(runID, host)
		events.log(runID, "failure_target", fmt.Sprintf("leader replica%d (%s)", leader, host))
	case labcfg.FailureFollower:
		cli, err := dialMasterRPC(20)
		if err != nil {
			events.log(runID, "failure_error", "master unreachable: "+err.Error())
			return
		}
		leader := masterLeader(cli)
		follower := pickFollower(cfg.Replicas, leader)
		host := masterReplicaHost(cli, follower)
		if host == "" {
			events.log(runID, "failure_error", "could not map follower index to host")
			return
		}
		container = containerName(runID, host)
		events.log(runID, "failure_target", fmt.Sprintf("follower replica%d (%s) leader=%d", follower, host, leader))
	case labcfg.FailureElection:
		injectElectionFailure(cfg, runID, events)
		return
	default: // FailureReplica (EPaxos)
		container = containerName(runID, "replica0")
		events.log(runID, "failure_target", "replica0")
	}

	events.log(runID, "failure_injected", container)
	if err := killContainer(container); err != nil {
		events.log(runID, "failure_error", err.Error())
		return
	}
	events.log(runID, "failure_confirmed", container)

	if cfg.Failure.RestartAfterS > 0 {
		time.Sleep(time.Duration(cfg.Failure.RestartAfterS * float64(time.Second)))
		events.log(runID, "restart_issued", container)
		if err := startContainer(container); err != nil {
			events.log(runID, "restart_error", err.Error())
			return
		}
		events.log(runID, "restart_confirmed", container)
	}
}

// injectElectionFailure implements the election-failure experiment: kill the
// current leader, then isolate the remaining replicas' Raft transport for
// FailedElections*election_timeout so the next election attempt(s) fail.
// The isolation expires automatically, after which a successful election
// restores service. The ACTUAL number of failed elections is measured from
// the leader-election log transitions (events.csv), not assumed.
func injectElectionFailure(cfg labcfg.Run, runID string, events *eventLog) {
	cli, err := dialMasterRPC(20)
	if err != nil {
		events.log(runID, "failure_error", "master unreachable: "+err.Error())
		return
	}
	leader := masterLeader(cli)
	if leader < 0 {
		events.log(runID, "failure_error", "no leader reported at injection time")
		return
	}
	host := masterReplicaHost(cli, leader)
	if host == "" {
		events.log(runID, "failure_error", "could not map leader index to host")
		return
	}
	events.log(runID, "failure_target", fmt.Sprintf("leader replica%d (%s)", leader, host))

	// Kill the leader so an election is triggered.
	container := containerName(runID, host)
	events.log(runID, "failure_injected", container)
	if err := killContainer(container); err != nil {
		events.log(runID, "failure_error", err.Error())
		return
	}
	events.log(runID, "failure_confirmed", container)

	// Isolate the surviving replicas' transports for the target number of
	// failed elections. Each failed election attempt takes roughly one
	// election timeout.
	isoMS := int64(cfg.Failure.FailedElections) * int64(cfg.RaftElectionMS)
	if isoMS <= 0 {
		events.log(runID, "election_isolation", "0ms (no failed elections targeted)")
		return
	}
	events.log(runID, "election_isolation_start", fmt.Sprintf("%dms for %d failed elections", isoMS, cfg.Failure.FailedElections))
	for i := 0; i < cfg.Replicas; i++ {
		if i == leader {
			continue
		}
		if err := isolateReplica(i, isoMS); err != nil {
			events.log(runID, "election_isolation_error", fmt.Sprintf("replica%d: %v", i, err))
		}
	}
	events.log(runID, "election_isolation_confirmed", fmt.Sprintf("%d replicas isolated for %dms", cfg.Replicas-1, isoMS))
}

func pickFollower(n, leader int) int {
	for i := 0; i < n; i++ {
		if i != leader {
			return i
		}
	}
	return 0
}
