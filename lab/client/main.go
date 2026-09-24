// Command client is the lab's common benchmark client for both Raft and
// EPaxos. It speaks the upstream EPaxos/genericsmr wire protocol and drives
// the same logical workload for both protocols.
//
// Concurrency is implemented as independent worker goroutines, each with its
// own TCP connection, issuing requests in a closed loop. The workload
// (op mix, key distribution, timing) is identical for both protocols; only
// the endpoint selection differs, and that is protocol-intrinsic:
//
//   - Raft:   every request is sent to the leader reported by HashiCorp Raft
//     (via the raft master).
//   - EPaxos: every request is sent to a replica chosen round-robin across
//     all replicas (leaderless).
//
// Every request is recorded with enough metadata to compute latency,
// throughput, success/failure rate, and p50/p95/p99 latency distributions.
package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"conslab/internal/proto"
	"state"
)

var (
	masterAddr  = flag.String("maddr", "master", "master host")
	masterPort  = flag.Int("mport", 7087, "master port")
	protocol    = flag.String("protocol", "raft", "raft | epaxos")
	impl        = flag.String("impl", "", "implementation name (recorded; empty = protocol's primary implementation)")
	replicas    = flag.Int("replicas", 3, "number of replicas (metadata)")
	writePct    = flag.Int("w", 50, "percentage of writes (PUT)")
	concurrency = flag.Int("c", 32, "number of client workers")
	durationS   = flag.Int("duration", 60, "measured phase duration (seconds)")
	warmupS     = flag.Int("warmup", 10, "warmup duration (seconds)")
	outDir      = flag.String("out", "/out", "output directory (bind-mounted)")
	runID       = flag.String("run-id", "run", "run identifier")
	seed        = flag.Int64("seed", 42, "random seed")
	keyspace    = flag.Int("keyspace", 1000, "number of distinct keys")
	conflictPct = flag.Int("conflict", 0, "percentage of requests that target the shared hot keys (conflict rate)")
	hotKeys     = flag.Int("hot-keys", 1, "number of distinct hot keys contended on")
	timeoutMS   = flag.Int("timeout-ms", 2000, "per-request timeout (ms)")
	gomaxprocs  = flag.Int("gomaxprocs", 4, "GOMAXPROCS")
	correctness = flag.Int("correctness", 0, "correctness mode: run exactly this many requests (0 = timed benchmark)")
	valueBase   = flag.Int64("value-base", 0, "correctness mode: PUT value = value-base + request id (deterministic)")
)

type reqRecord struct {
	runID       string
	protocol    string
	replicas    int
	readPct     int
	writePct    int
	concurrency int
	conflictPct int
	worker      int
	seq         int64
	requestID   int64
	op          string
	key         int64
	hot         bool
	target      int
	startNS     int64
	endNS       int64
	latencyNS   int64
	ok          bool
	errMsg      string
	value       int64 // returned value (correctness mode)
}

type conn struct {
	nc     net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

type worker struct {
	id     int
	c      *client
	conn   *conn
	target int
	seq    int64
	rand   *rand.Rand
}

type client struct {
	cfg      *config
	master   *rpc.Client
	replicas []string // "host:port" client addresses
	leader   int
	records  chan reqRecord
	wg       sync.WaitGroup
}

type config struct {
	masterAddr  string
	masterPort  int
	protocol    string
	impl        string
	replicas    int
	writePct    int
	concurrency int
	duration    time.Duration
	warmup      time.Duration
	outDir      string
	runID       string
	seed        int64
	keyspace    int
	conflictPct int
	hotKeys     int
	timeout     time.Duration
	correctness int   // 0 = timed benchmark; >0 = run exactly this many requests
	valueBase   int64 // correctness mode: PUT value = valueBase + request id
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*gomaxprocs)

	cfg := &config{
		masterAddr:  *masterAddr,
		masterPort:  *masterPort,
		protocol:    *protocol,
		impl:        *impl,
		replicas:    *replicas,
		writePct:    *writePct,
		concurrency: *concurrency,
		duration:    time.Duration(*durationS) * time.Second,
		warmup:      time.Duration(*warmupS) * time.Second,
		outDir:      *outDir,
		runID:       *runID,
		seed:        *seed,
		keyspace:    *keyspace,
		conflictPct: *conflictPct,
		hotKeys:     *hotKeys,
		timeout:     time.Duration(*timeoutMS) * time.Millisecond,
		correctness: *correctness,
		valueBase:   *valueBase,
	}
	if cfg.conflictPct < 0 || cfg.conflictPct > 100 {
		log.Fatalf("conflict must be in [0,100], got %d", cfg.conflictPct)
	}
	if cfg.hotKeys < 1 || cfg.hotKeys > cfg.keyspace {
		log.Fatalf("hot-keys must be in [1,%d], got %d", cfg.keyspace, cfg.hotKeys)
	}

	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		log.Fatal(err)
	}

	c := &client{cfg: cfg}
	c.master = dialMaster(cfg.masterAddr, cfg.masterPort)
	c.replicas = c.getReplicaList()
	c.leader = -1
	if cfg.protocol == "raft" {
		c.leader = c.waitForLeader(30 * time.Second)
		log.Printf("raft leader is replica %d", c.leader)
		// Raft: the adapter serves client connections on a dedicated port
		// (7070), separate from the Raft transport (6000), so probing it is
		// safe.
		c.waitReplicasReady(60 * time.Second)
	} else {
		// EPaxos: the upstream server serves peer AND client connections on
		// the same port (7070). Probing it would be accepted as a peer
		// connection and corrupt the peer protocol, so we wait for the
		// master to report all replicas registered, then allow a settle
		// period for ConnectToPeers to finish.
		c.waitMasterReady(60 * time.Second)
		time.Sleep(3 * time.Second)
	}
	log.Printf("all %d replicas ready", len(c.replicas))

	c.records = make(chan reqRecord, 65536)

	// CSV writer goroutine.
	csvDone := make(chan struct{})
	go c.writeCSV(csvDone)

	// Warmup phase (no recording). Skipped in correctness mode: the harness
	// needs a deterministic request set, and the fixed request count makes a
	// warmup unnecessary.
	if cfg.correctness == 0 {
		log.Printf("warmup %v", cfg.warmup)
		c.runPhase(cfg.warmup, false)
	}

	// Measured phase.
	phaseStart := time.Now()
	writePhaseMarker(cfg.outDir, phaseStart)
	if cfg.correctness > 0 {
		log.Printf("correctness phase: %d requests, concurrency %d", cfg.correctness, cfg.concurrency)
		c.runCorrectness(cfg.correctness)
	} else {
		log.Printf("measured phase %v starting", cfg.duration)
		c.runPhase(cfg.duration, true)
	}
	c.wg.Wait()
	close(c.records)
	<-csvDone

	writeSummary(cfg, phaseStart)
	log.Printf("client done")
}

// runCorrectness issues exactly n requests across the workers, each with a
// deterministic value (valueBase + request id) and a unique key (the request
// id), so the harness can detect missing and duplicate applications exactly.
// The request id is a global atomic counter, so all workers run concurrently.
func (c *client) runCorrectness(n int) {
	c.wg.Add(c.cfg.concurrency)
	var nextID int64
	for i := 0; i < c.cfg.concurrency; i++ {
		w := &worker{id: i, c: c, rand: rand.New(rand.NewSource(c.cfg.seed + int64(i)))}
		if c.cfg.protocol == "raft" {
			w.target = c.leader
		} else {
			w.target = i % len(c.replicas)
		}
		w.conn = c.dial(w.target)
		go w.runCorrectness(n, &nextID)
	}
	c.wg.Wait()
}

// runCorrectness issues the worker's share of the n requests. Each request
// writes its own key (the request id) with value valueBase + id, so the
// final state is exactly {id: valueBase+id} for every replied request.
func (w *worker) runCorrectness(n int, nextID *int64) {
	defer w.c.wg.Done()
	for {
		id := atomic.AddInt64(nextID, 1) - 1
		if id >= int64(n) {
			return
		}
		key := id
		val := w.c.cfg.valueBase + id
		start := time.Now()
		ok, errMsg, rval := w.doRequestVal(state.PUT, key, val)
		end := time.Now()
		w.c.records <- reqRecord{
			runID: w.c.cfg.runID, protocol: w.c.cfg.protocol, replicas: w.c.cfg.replicas,
			readPct: 0, writePct: 100,
			concurrency: w.c.cfg.concurrency, conflictPct: 0,
			worker: w.id, seq: id,
			requestID: id,
			op:        "PUT", key: key, hot: false, target: w.target,
			startNS: start.UnixNano(), endNS: end.UnixNano(),
			latencyNS: end.Sub(start).Nanoseconds(), ok: ok, errMsg: errMsg, value: rval,
		}
	}
}

// doRequestVal is doRequest plus the returned value (correctness mode).
func (w *worker) doRequestVal(op state.Operation, key int64, val int64) (bool, string, int64) {
	if w.conn == nil {
		w.reconnect()
		if w.conn == nil {
			return false, "no connection", 0
		}
	}
	cmd := state.Command{Op: op, K: state.Key(key), V: state.Value(val)}
	prop := &proto.Propose{CommandId: int32(w.seq), Command: cmd, Timestamp: time.Now().UnixNano()}
	if err := w.conn.nc.SetWriteDeadline(time.Now().Add(w.c.cfg.timeout)); err != nil {
		return false, err.Error(), 0
	}
	if err := w.conn.writer.WriteByte(proto.PROPOSE); err != nil {
		w.reconnect()
		return false, err.Error(), 0
	}
	prop.Marshal(w.conn.writer)
	if err := w.conn.writer.Flush(); err != nil {
		w.reconnect()
		return false, err.Error(), 0
	}
	if err := w.conn.nc.SetReadDeadline(time.Now().Add(w.c.cfg.timeout)); err != nil {
		return false, err.Error(), 0
	}
	reply := new(proto.ProposeReplyTS)
	if err := reply.Unmarshal(w.conn.reader); err != nil {
		w.reconnect()
		return false, err.Error(), 0
	}
	w.conn.nc.SetReadDeadline(time.Time{})
	return reply.OK != 0, "", int64(reply.Value)
}

func (c *client) runPhase(d time.Duration, record bool) {
	deadline := time.Now().Add(d)
	c.wg.Add(c.cfg.concurrency)
	for i := 0; i < c.cfg.concurrency; i++ {
		// Each worker gets its own RNG, seeded deterministically from the
		// run seed and the worker id. A shared RNG would be a data race
		// (math/rand.Rand is not safe for concurrent use) and would make
		// runs non-reproducible.
		w := &worker{id: i, c: c, rand: rand.New(rand.NewSource(c.cfg.seed + int64(i)))}
		if c.cfg.protocol == "raft" {
			w.target = c.leader
		} else {
			w.target = i % len(c.replicas)
		}
		w.conn = c.dial(w.target)
		go w.run(deadline, record)
	}
	c.wg.Wait()
}

func (w *worker) run(deadline time.Time, record bool) {
	defer w.c.wg.Done()
	for time.Now().Before(deadline) {
		op := state.GET
		if w.rand.Intn(100) < w.c.cfg.writePct {
			op = state.PUT
		}
		key, hot := w.c.pickKey(w.rand)
		start := time.Now()
		ok, errMsg := w.doRequest(op, key)
		end := time.Now()
		if record {
			w.c.records <- reqRecord{
				runID: w.c.cfg.runID, protocol: w.c.cfg.protocol, replicas: w.c.cfg.replicas,
				readPct: 100 - w.c.cfg.writePct, writePct: w.c.cfg.writePct,
				concurrency: w.c.cfg.concurrency, conflictPct: w.c.cfg.conflictPct,
				worker: w.id, seq: w.seq,
				requestID: int64(w.id)*1_000_000_000 + w.seq,
				op:        opName(op), key: key, hot: hot, target: w.target,
				startNS: start.UnixNano(), endNS: end.UnixNano(),
				latencyNS: end.Sub(start).Nanoseconds(), ok: ok, errMsg: errMsg,
			}
		}
		w.seq++
	}
}

// pickKey implements the conflict-rate workload. With probability
// conflictPct/100 the request targets one of the hotKeys shared keys; with
// the remaining probability it targets a uniformly random key from the
// disjoint cold range. Hot and cold ranges do not overlap, so the realized
// hot-key fraction is an exact, measurable quantity rather than an
// assumption about the RNG.
func (c *client) pickKey(r *rand.Rand) (int64, bool) {
	if c.cfg.conflictPct > 0 && r.Intn(100) < c.cfg.conflictPct {
		return int64(r.Intn(c.cfg.hotKeys)), true
	}
	cold := c.cfg.keyspace - c.cfg.hotKeys
	if cold <= 0 {
		return int64(r.Intn(c.cfg.hotKeys)), true
	}
	return int64(c.cfg.hotKeys + r.Intn(cold)), false
}

func opName(op state.Operation) string {
	if op == state.PUT {
		return "PUT"
	}
	return "GET"
}

// doRequest sends one PROPOSE and waits for the reply. On failure the worker
// reconnects: for Raft it re-queries the master for the new leader; for
// EPaxos it moves to the next replica.
func (w *worker) doRequest(op state.Operation, key int64) (bool, string) {
	if w.conn == nil {
		w.reconnect()
		if w.conn == nil {
			return false, "no connection"
		}
	}
	cmd := state.Command{Op: op, K: state.Key(key), V: state.Value(w.seq)}
	prop := &proto.Propose{CommandId: int32(w.seq), Command: cmd, Timestamp: time.Now().UnixNano()}
	if err := w.conn.nc.SetWriteDeadline(time.Now().Add(w.c.cfg.timeout)); err != nil {
		return false, err.Error()
	}
	if err := w.conn.writer.WriteByte(proto.PROPOSE); err != nil {
		w.reconnect()
		return false, err.Error()
	}
	prop.Marshal(w.conn.writer)
	if err := w.conn.writer.Flush(); err != nil {
		w.reconnect()
		return false, err.Error()
	}
	if err := w.conn.nc.SetReadDeadline(time.Now().Add(w.c.cfg.timeout)); err != nil {
		return false, err.Error()
	}
	reply := new(proto.ProposeReplyTS)
	if err := reply.Unmarshal(w.conn.reader); err != nil {
		w.reconnect()
		return false, err.Error()
	}
	w.conn.nc.SetReadDeadline(time.Time{})
	return reply.OK != 0, ""
}

// reconnect re-establishes the worker's connection after a failure.
func (w *worker) reconnect() {
	if w.c.cfg.protocol == "raft" {
		// The master's GetLeader can take a couple of seconds after a leader
		// dies (it must time out the dead replica's RPC), so retry until a
		// valid leader is reported instead of giving up after one attempt.
		for i := 0; i < 20; i++ {
			l := w.c.queryLeader()
			if l >= 0 && l < len(w.c.replicas) {
				w.c.leader = l
				w.target = l
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	} else {
		w.target = (w.target + 1) % len(w.c.replicas)
	}
	w.conn = w.c.dial(w.target)
}

func (c *client) dial(i int) *conn {
	if i < 0 || i >= len(c.replicas) {
		return nil
	}
	// Short dial timeout: after a leader failure the worker must fail fast
	// and reconnect to the new leader. A long dial would block the worker
	// for the rest of the measured phase.
	nc, err := net.DialTimeout("tcp", c.replicas[i], 1*time.Second)
	if err != nil {
		log.Printf("dial %s failed: %v", c.replicas[i], err)
		return nil
	}
	return &conn{nc: nc, reader: bufio.NewReader(nc), writer: bufio.NewWriter(nc)}
}

func dialMaster(host string, port int) *rpc.Client {
	for {
		cli, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", host, port))
		if err == nil {
			return cli
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *client) getReplicaList() []string {
	for {
		var reply proto.GetReplicaListReply
		if err := c.master.Call("Master.GetReplicaList", new(proto.GetReplicaListArgs), &reply); err == nil && reply.Ready {
			return reply.ReplicaList
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *client) queryLeader() int {
	done := make(chan int, 1)
	go func() {
		var reply proto.GetLeaderReply
		if err := c.master.Call("Master.GetLeader", new(proto.GetLeaderArgs), &reply); err == nil {
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

func (c *client) waitForLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.queryLeader(); l >= 0 {
			return l
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatalf("no raft leader within %v", timeout)
	return -1
}

// waitReplicasReady dials every replica until it accepts a connection.
// Only safe for Raft, where the client port is dedicated.
func (c *client) waitReplicasReady(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	ready := make([]bool, len(c.replicas))
	for {
		all := true
		for i := range c.replicas {
			if ready[i] {
				continue
			}
			nc, err := net.DialTimeout("tcp", c.replicas[i], 2*time.Second)
			if err == nil {
				nc.Close()
				ready[i] = true
			} else {
				all = false
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			log.Fatalf("replicas not ready within %v", timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitMasterReady polls the master until it reports all replicas registered.
func (c *client) waitMasterReady(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var reply proto.GetReplicaListReply
		if err := c.master.Call("Master.GetReplicaList", new(proto.GetReplicaListArgs), &reply); err == nil && reply.Ready {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatalf("master did not report all replicas ready within %v", timeout)
}

func (c *client) writeCSV(done chan struct{}) {
	f, err := os.Create(filepath.Join(c.cfg.outDir, "requests.csv"))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{
		"run_id", "protocol", "impl", "replicas", "read_pct", "write_pct", "concurrency",
		"conflict_pct", "worker", "seq", "request_id", "op", "key", "hot", "target",
		"start_ns", "end_ns", "latency_ns", "ok", "error", "value",
	})
	for r := range c.records {
		w.Write([]string{
			r.runID, r.protocol, c.cfg.impl, itoa(r.replicas), itoa(r.readPct), itoa(r.writePct), itoa(r.concurrency),
			itoa(r.conflictPct), itoa(r.worker), itoa64(r.seq), itoa64(r.requestID), r.op, itoa64(r.key),
			boolStr(r.hot), itoa(r.target),
			itoa64(r.startNS), itoa64(r.endNS), itoa64(r.latencyNS), boolStr(r.ok), r.errMsg, itoa64(r.value),
		})
	}
	w.Flush()
	close(done)
}

func writePhaseMarker(outDir string, start time.Time) {
	data, _ := json.Marshal(map[string]int64{"phase_started_ns": start.UnixNano()})
	os.WriteFile(filepath.Join(outDir, "phase.json"), data, 0o644)
}

func writeSummary(cfg *config, phaseStart time.Time) {
	summary := map[string]any{
		"run_id": cfg.runID, "protocol": cfg.protocol, "implementation": cfg.impl,
		"replicas": cfg.replicas,
		"read_pct": 100 - cfg.writePct, "write_pct": cfg.writePct,
		"concurrency": cfg.concurrency, "duration_s": cfg.duration.Seconds(),
		"warmup_s": cfg.warmup.Seconds(), "phase_started_ns": phaseStart.UnixNano(),
		"phase_ended_ns": time.Now().UnixNano(),
		"conflict_pct":   cfg.conflictPct, "hot_keys": cfg.hotKeys,
	}
	data, _ := json.MarshalIndent(summary, "", "  ")
	os.WriteFile(filepath.Join(cfg.outDir, "client-summary.json"), data, 0o644)
}

func itoa(i int) string     { return fmt.Sprintf("%d", i) }
func itoa64(i int64) string { return fmt.Sprintf("%d", i) }
func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
