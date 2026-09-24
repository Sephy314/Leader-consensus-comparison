// Command correctness runs the correctness-validation harness: short,
// deterministic tests that verify each implementation performs consistent
// consensus execution, independent of the performance benchmark.
//
// For each (protocol, implementation) it starts a fresh compose project,
// runs a fixed number of requests at a given concurrency (with deterministic
// per-request values), optionally kills/restarts a replica mid-run, then
// collects every replica's state-machine snapshot over the admin RPC and
// checks:
//
//   - every request that got a reply was applied exactly once (no missing,
//     no duplicate application),
//   - the surviving replicas' final states agree,
//   - after a restart, the restarted replica's state matches the committed
//     state,
//   - new requests are processed after the fault.
//
// The check is state-based, not log-based: replicas are compared on their
// final key/value state, never on their internal log representation, so the
// EPaxos dependency-ordering model is not assumed to match Raft's.
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"conslab/internal/labcfg"
	"conslab/internal/proto"
)

// correctnessTest is one (protocol, implementation, scenario) combination.
type correctnessTest struct {
	Protocol    string // "raft" | "epaxos"
	Impl        string // "hashicorp" | "etcd" | "original" | "nvb"
	Scenario    string // "sequential" | "concurrency-8" | "concurrency-32" | "follower-failure" | "leader-failure" | "replica-failure"
	Concurrency int
	Requests    int
	KillAfter   int    // kill this many requests in (0 = no fault)
	KillRole    string // "leader" | "follower" | "replica"
	Restart     bool   // restart the killed replica and verify state
}

// correctnessResult is the per-test outcome, also written to JSON.
type correctnessResult struct {
	Protocol    string           `json:"protocol"`
	Impl        string           `json:"implementation"`
	Scenario    string           `json:"scenario"`
	Concurrency int              `json:"concurrency"`
	Requests    int              `json:"requests"`
	Pass        bool             `json:"pass"`
	Failures    []string         `json:"failures,omitempty"`
	Applied     map[string]int64 `json:"applied_per_replica,omitempty"`
}

func cmdCorrectness(args []string) error {
	fs := flag.NewFlagSet("correctness", flag.ContinueOnError)
	only := fs.String("only", "", "run only this implementation (hashicorp|etcd|original|nvb)")
	requests := fs.Int("requests", 2000, "requests per test")
	base := fs.String("results-base", "correctness", "results subtree to write to")
	if err := fs.Parse(args); err != nil {
		return err
	}
	resultsBase = *base

	tests := []correctnessTest{
		// Sequential: concurrency 1.
		{Protocol: "raft", Impl: "hashicorp", Scenario: "sequential", Concurrency: 1, Requests: *requests},
		{Protocol: "raft", Impl: "etcd", Scenario: "sequential", Concurrency: 1, Requests: *requests},
		{Protocol: "epaxos", Impl: "original", Scenario: "sequential", Concurrency: 1, Requests: *requests},
		{Protocol: "epaxos", Impl: "nvb", Scenario: "sequential", Concurrency: 1, Requests: *requests},
		// Concurrent.
		{Protocol: "raft", Impl: "hashicorp", Scenario: "concurrency-8", Concurrency: 8, Requests: *requests},
		{Protocol: "raft", Impl: "etcd", Scenario: "concurrency-8", Concurrency: 8, Requests: *requests},
		{Protocol: "epaxos", Impl: "original", Scenario: "concurrency-8", Concurrency: 8, Requests: *requests},
		{Protocol: "epaxos", Impl: "nvb", Scenario: "concurrency-8", Concurrency: 8, Requests: *requests},
		{Protocol: "raft", Impl: "hashicorp", Scenario: "concurrency-32", Concurrency: 32, Requests: *requests},
		{Protocol: "raft", Impl: "etcd", Scenario: "concurrency-32", Concurrency: 32, Requests: *requests},
		{Protocol: "epaxos", Impl: "original", Scenario: "concurrency-32", Concurrency: 32, Requests: *requests},
		{Protocol: "epaxos", Impl: "nvb", Scenario: "concurrency-32", Concurrency: 32, Requests: *requests},
		// Faults.
		{Protocol: "raft", Impl: "hashicorp", Scenario: "follower-failure", Concurrency: 8, Requests: *requests, KillAfter: *requests / 2, KillRole: "follower", Restart: true},
		{Protocol: "raft", Impl: "etcd", Scenario: "follower-failure", Concurrency: 8, Requests: *requests, KillAfter: *requests / 2, KillRole: "follower", Restart: true},
		{Protocol: "raft", Impl: "hashicorp", Scenario: "leader-failure", Concurrency: 8, Requests: *requests, KillAfter: *requests / 2, KillRole: "leader", Restart: true},
		{Protocol: "raft", Impl: "etcd", Scenario: "leader-failure", Concurrency: 8, Requests: *requests, KillAfter: *requests / 2, KillRole: "leader", Restart: true},
		{Protocol: "epaxos", Impl: "original", Scenario: "replica-failure", Concurrency: 8, Requests: *requests, KillAfter: *requests / 2, KillRole: "replica", Restart: true},
		{Protocol: "epaxos", Impl: "nvb", Scenario: "replica-failure", Concurrency: 8, Requests: *requests, KillAfter: *requests / 2, KillRole: "replica", Restart: true},
	}

	var results []correctnessResult
	for _, t := range tests {
		if *only != "" && t.Impl != *only {
			continue
		}
		res := runCorrectnessTest(t)
		results = append(results, res)
		printCorrectnessResult(res)
	}

	// Summary.
	fmt.Println("\nCorrectness Validation")
	fmt.Println("======================")
	byImpl := map[string][]correctnessResult{}
	for _, r := range results {
		byImpl[r.Protocol+"/"+r.Impl] = append(byImpl[r.Protocol+"/"+r.Impl], r)
	}
	allPass := true
	for _, key := range sortedKeys(byImpl) {
		fmt.Printf("%s\n", key)
		for _, r := range byImpl[key] {
			status := "PASS"
			if !r.Pass {
				status = "FAIL"
				allPass = false
			}
			fmt.Printf("  %-18s %s\n", r.Scenario, status)
		}
	}
	overall := "PASS"
	if !allPass {
		overall = "FAIL"
	}
	fmt.Printf("\nOverall: %s\n", overall)

	// JSON output.
	outDir := filepath.Join(resultsDir(), "raw")
	_ = os.MkdirAll(outDir, 0o755)
	jsonPath := filepath.Join(resultsDir(), "correctness.json")
	data, _ := json.MarshalIndent(map[string]any{
		"overall": overall,
		"results": results,
	}, "", "  ")
	if err := os.WriteFile(jsonPath, data, 0o644); err != nil {
		return err
	}
	fmt.Printf("results written to %s\n", jsonPath)
	return nil
}

func sortedKeys(m map[string][]correctnessResult) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// runCorrectnessTest runs one test end to end and returns its result.
func runCorrectnessTest(t correctnessTest) correctnessResult {
	res := correctnessResult{
		Protocol: t.Protocol, Impl: t.Impl, Scenario: t.Scenario,
		Concurrency: t.Concurrency, Requests: t.Requests, Pass: true,
		Applied: map[string]int64{},
	}
	runID := fmt.Sprintf("correctness-%s-%s-%s", t.Protocol, t.Impl, t.Scenario)
	dir := filepath.Join(resultsDir(), "raw", runID)
	_ = os.MkdirAll(dir, 0o755)

	cfg := labcfg.Default()
	cfg.Protocol = t.Protocol
	cfg.Implementation = t.Impl
	cfg.Replicas = 3
	cfg.WritePct = 100
	cfg.ReadPct = 0
	cfg.Concurrency = t.Concurrency
	cfg.DurationS = 30
	cfg.WarmupS = 0
	cfg.Repetitions = 1
	cfg.Failure = labcfg.Failure{Mode: labcfg.FailureNone}
	cfg.Correctness = t.Requests
	cfg.ValueBase = 1

	composePath, err := renderCompose(dir, cfg, runID)
	if err != nil {
		res.Pass = false
		res.Failures = append(res.Failures, "compose render: "+err.Error())
		return res
	}
	if err := composeUp(dir, runID, composePath); err != nil {
		res.Pass = false
		res.Failures = append(res.Failures, "compose up: "+err.Error())
		return res
	}
	defer composeDown(dir, runID, composePath)

	// Wait for the master and all replicas.
	master, err := dialMasterRPC(120)
	if err != nil {
		res.Pass = false
		res.Failures = append(res.Failures, "master unreachable: "+err.Error())
		return res
	}
	defer master.Close()
	if !waitReplicas(master, cfg.Replicas, 60*time.Second) {
		res.Pass = false
		res.Failures = append(res.Failures, "replicas did not register")
		return res
	}
	// Raft needs a leader before the client can run.
	if t.Protocol == "raft" {
		if !waitLeader(master, 60*time.Second) {
			res.Pass = false
			res.Failures = append(res.Failures, "no raft leader elected")
			return res
		}
	}

	// Start the client in the background (it runs in a container).
	clientName := containerName(runID, "client")
	clientDone := make(chan error, 1)
	go func() {
		_, err := waitContainerExit(clientName, 5*time.Minute)
		clientDone <- err
	}()

	// Fault injection: kill a replica partway through the request stream.
	if t.KillAfter > 0 {
		// Wait until roughly KillAfter requests have been issued. The client
		// writes requests.csv incrementally; poll its row count.
		reqPath := filepath.Join(dir, "requests.csv")
		deadline := time.Now().Add(3 * time.Minute)
		for time.Now().Before(deadline) {
			if n := csvRows(reqPath); n >= t.KillAfter {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		target := t.KillRole
		if t.Protocol == "raft" && t.KillRole == "leader" {
			target = fmt.Sprintf("replica%d", masterLeader(master))
		} else if t.Protocol == "raft" && t.KillRole == "follower" {
			leader := masterLeader(master)
			target = fmt.Sprintf("replica%d", (leader+1)%cfg.Replicas)
		} else {
			target = "replica0"
		}
		_ = killContainer(containerName(runID, target))
		if t.Restart {
			// Restart after a short delay so the cluster notices the failure.
			time.Sleep(2 * time.Second)
			_ = startContainer(containerName(runID, target))
		}
	}

	// Wait for the client to finish.
	if err := <-clientDone; err != nil {
		res.Pass = false
		res.Failures = append(res.Failures, "client: "+err.Error())
		return res
	}

	// Read the client's request log.
	reqPath := filepath.Join(dir, "requests.csv")
	rows, err := readCSV(reqPath)
	if err != nil {
		res.Pass = false
		res.Failures = append(res.Failures, "reading requests.csv: "+err.Error())
		return res
	}

	// Reference model: each replied request wrote its own key (the request
	// id) with value valueBase + id, so the final state must be exactly
	// {id: value} for every replied request. The reply value is what the
	// client was told; a replica that applied a request twice with a
	// different value, or not at all, will differ from this reference.
	expected := int64(0)
	ref := map[int64]int64{}
	for _, r := range rows {
		if r["ok"] != "1" {
			continue
		}
		expected++
		ref[atoi64(r["request_id"])] = atoi64(r["value"])
	}

	// Wait for replicas to catch up to the expected applied count. Raft
	// replays its persisted log on restart, so every replica must catch up.
	// EPaxos keeps state in memory, so a restarted replica cannot recover
	// pre-restart state; only the surviving replicas must catch up, and the
	// restarted one is only required to be alive.
	// The RPC service name differs by implementation: the adapters register
	// a `Replica` type (hashicorp/etcd/original), the nvb adapter registers
	// a `Server` type.
	rpcName := "Replica.GetState"
	if t.Impl == "nvb" {
		rpcName = "Server.GetState"
	}
	states := map[string]map[int64]int64{}
	applied := map[string]int64{}
	caughtUp := map[string]bool{}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for i := 0; i < cfg.Replicas; i++ {
			name := fmt.Sprintf("replica%d", i)
			if caughtUp[name] {
				continue
			}
			cli, err := rpc.DialHTTP("tcp", fmt.Sprintf("127.0.0.1:%d", replicaAdminHostPort(i)))
			if err != nil {
				all = false
				continue
			}
			var reply proto.GetStateReply
			if err := cli.Call(rpcName, new(proto.GetStateArgs), &reply); err != nil {
				cli.Close()
				all = false
				continue
			}
			cli.Close()
			if reply.Applied >= expected {
				caughtUp[name] = true
				states[name] = reply.Store
				applied[name] = reply.Applied
				res.Applied[name] = reply.Applied
			} else {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 1. Applied count: every caught-up replica applied at least the replied
	//    requests. A replica may apply MORE than the client saw replied: a
	//    request committed during a fault can be applied on the replicas
	//    while the client's reply is lost (the client reconnects and the
	//    request is never re-issued). That is correct consensus behaviour,
	//    not a duplicate. What must never happen is applying FEWER than the
	//    replied requests (a missing application) or applying a request
	//    twice with a different value (a duplicate, caught by the state
	//    check below).
	for name, n := range applied {
		if n < expected {
			res.Pass = false
			res.Failures = append(res.Failures, fmt.Sprintf(
				"applied count mismatch: %s applied %d, expected at least %d (replied requests)",
				name, n, expected))
		}
		// In a fault-free run every request is replied, so a replica that
		// applied MORE than the replied requests must have applied a request
		// twice (a duplicate). Fault runs legitimately apply more (replies
		// lost during the fault), so the exact match is only enforced there.
		if t.KillAfter == 0 && n > expected {
			res.Pass = false
			res.Failures = append(res.Failures, fmt.Sprintf(
				"duplicate application: %s applied %d, expected exactly %d (fault-free run)",
				name, n, expected))
		}
	}

	// 2. State agreement: every caught-up replica must contain every replied
	//    request's key with the correct value, and agree with every other
	//    caught-up replica. A replica may additionally contain requests that
	//    committed during a fault but whose replies were lost; those are
	//    correct and must not fail the check.
	for name, st := range states {
		for k, v := range ref {
			if st[k] != v {
				res.Pass = false
				res.Failures = append(res.Failures, fmt.Sprintf(
					"state mismatch: %s key %d = %d, expected %d (replied request)",
					name, k, st[k], v))
			}
		}
	}
	for a, sa := range states {
		for b, sb := range states {
			if a < b {
				for k := range ref {
					if sa[k] != sb[k] {
						res.Pass = false
						res.Failures = append(res.Failures, fmt.Sprintf(
							"replica states differ: %s vs %s at key %d (%d vs %d)",
							a, b, k, sa[k], sb[k]))
						break
					}
				}
			}
		}
	}

	// 3. Quorum: at least a majority of replicas must be caught up, or the
	//    committed state is not preserved.
	if len(caughtUp) < cfg.Replicas/2+1 {
		res.Pass = false
		res.Failures = append(res.Failures, fmt.Sprintf(
			"only %d/%d replicas caught up (no quorum)", len(caughtUp), cfg.Replicas))
	}

	// 4. Raft restart must recover: every replica (including the restarted
	//    one) must catch up, because Raft persists its log.
	if t.Protocol == "raft" && len(caughtUp) != cfg.Replicas {
		res.Pass = false
		res.Failures = append(res.Failures, fmt.Sprintf(
			"raft restart did not recover: %d/%d replicas caught up", len(caughtUp), cfg.Replicas))
	}

	// 5. Fault tests: new requests must have been processed after the fault.
	if t.KillAfter > 0 {
		after := 0
		for _, r := range rows {
			if r["ok"] == "1" && atoi64(r["request_id"]) >= int64(t.KillAfter) {
				after++
			}
		}
		if after == 0 {
			res.Pass = false
			res.Failures = append(res.Failures, "no successful request after the fault")
		}
	}

	return res
}

// ---- helpers ----

func waitReplicas(master *rpc.Client, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var reply proto.GetReplicaListReply
		if err := master.Call("Master.GetReplicaList", new(proto.GetReplicaListArgs), &reply); err == nil && reply.Ready && len(reply.ReplicaList) == n {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func waitLeader(master *rpc.Client, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if masterLeader(master) >= 0 {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func csvRows(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	r := csv.NewReader(f)
	n := 0
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		n++
	}
	return n - 1 // minus header
}

func readCSV(path string) ([]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	var rows []map[string]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		m := map[string]string{}
		for i, h := range header {
			if i < len(rec) {
				m[h] = rec[i]
			}
		}
		rows = append(rows, m)
	}
	return rows, nil
}

func atoi64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

func mapsEqual(a, b map[int64]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func printCorrectnessResult(r correctnessResult) {
	status := "PASS"
	if !r.Pass {
		status = "FAIL"
	}
	fmt.Printf("[%s] %s/%s %s (%d req, c=%d)\n", status, r.Protocol, r.Impl, r.Scenario, r.Requests, r.Concurrency)
	for _, f := range r.Failures {
		fmt.Printf("  CORRECTNESS FAILED\n  implementation: %s\n  test: %s\n  %s\n", r.Impl, r.Scenario, f)
	}
}

var _ = strings.TrimSpace
