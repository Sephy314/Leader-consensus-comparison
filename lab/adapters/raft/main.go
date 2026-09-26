// Command raftadapter is the lab's thin adapter around HashiCorp Raft.
//
// It does NOT implement consensus. HashiCorp Raft performs leader election,
// log replication, quorum calculation, commit logic, term management, and
// state transitions. The adapter only:
//
//  1. accepts benchmark client requests on the upstream EPaxos/genericsmr
//     wire protocol (PROPOSE -> ProposeReplyTS),
//  2. converts them into state-machine commands,
//  3. calls raft.Raft.Apply,
//  4. waits for the resulting future,
//  5. returns the benchmark response.
//
// It also reports the leader chosen by HashiCorp Raft (raft.Raft.Leader) to
// the lab's raft master over RPC. It never elects or selects a leader.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"conslab/internal/proto"
	"state"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

var (
	masterAddr  = flag.String("master", "master:7087", "raft master address (host:port)")
	myAddr      = flag.String("addr", "", "advertised address of this replica (container hostname)")
	clientPort  = flag.Int("client-port", 7070, "benchmark client port (genericsmr wire protocol)")
	raftPort    = flag.Int("raft-port", 6000, "HashiCorp Raft transport port")
	dir         = flag.String("dir", "/data", "directory for Raft log/snapshot state")
	gomaxprocs  = flag.Int("gomaxprocs", 4, "GOMAXPROCS")
	heartbeatMS = flag.Int("heartbeat-ms", 1000, "Raft heartbeat timeout (ms)")
	electionMS  = flag.Int("election-ms", 2000, "Raft election timeout (ms)")
	snapThr     = flag.Int("snapshot-threshold", 8192, "Raft snapshot threshold")
	trailingLog = flag.Int("trailing-logs", 1024, "Raft trailing logs")
	storeMode   = flag.String("store", "bolt", "persistent store backend: bolt (raft-boltdb, durable) or inmem (raft.NewInmemStore, no durability)")
	applyTO     = flag.Duration("apply-timeout", 5*time.Second, "timeout for raft.Apply")
	commCostMS  = flag.Int("comm-cost-ms", 0, "fixed one-way latency (ms) added to every inter-replica message; 0 = local-network baseline")
	commJitter  = flag.Int("comm-jitter-pct", 0, "jitter as % of comm-cost-ms: each message waits an extra uniform delay in [0, cost*jitter/100]")
)

// fsm is the benchmark application state machine. It is separate from Raft
// and implements the same logical semantics as the EPaxos state machine
// (state.Command.Execute). Reads (GET) are routed through consensus exactly
// like writes; there is no Raft-only local-read optimization.
type fsm struct {
	mu      sync.Mutex
	st      *state.State
	applied int64 // client commands applied (non-empty log entries)
}

func newFSM() *fsm { return &fsm{st: state.InitState()} }

func (f *fsm) Apply(l *raft.Log) interface{} {
	var cmd state.Command
	if err := cmd.Unmarshal(bytes.NewReader(l.Data)); err != nil {
		return state.NIL
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied++
	return cmd.Execute(f.st)
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(f.st.Store))); err != nil {
		return nil, err
	}
	for k, v := range f.st.Store {
		if err := binary.Write(&buf, binary.LittleEndian, uint64(k)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, uint64(v)); err != nil {
			return nil, err
		}
	}
	return &fsmSnapshot{data: buf.Bytes()}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var n uint32
	if err := binary.Read(rc, binary.LittleEndian, &n); err != nil {
		return err
	}
	m := make(map[state.Key]state.Value, n)
	for i := uint32(0); i < n; i++ {
		var k, v uint64
		if err := binary.Read(rc, binary.LittleEndian, &k); err != nil {
			return err
		}
		if err := binary.Read(rc, binary.LittleEndian, &v); err != nil {
			return err
		}
		m[state.Key(k)] = state.Value(v)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st.Store = m
	return nil
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// Replica is the RPC surface exposed to the lab's raft master (mirrors the
// upstream EPaxos server's RPC surface on port clientPort+1000).
type Replica struct {
	id    int
	raft  *raft.Raft
	peers []raft.Server // ordered like the master's NodeList
	iso   *isolatingTransport
	fsm   *fsm
}

func (r *Replica) Ping(args *proto.PingArgs, reply *proto.PingReply) error { return nil }

// GetState returns the state-machine snapshot and the applied-command count
// for the correctness harness. Read-only; never used by the benchmark.
func (r *Replica) GetState(args *proto.GetStateArgs, reply *proto.GetStateReply) error {
	r.fsm.mu.Lock()
	defer r.fsm.mu.Unlock()
	reply.Store = make(map[int64]int64, len(r.fsm.st.Store))
	for k, v := range r.fsm.st.Store {
		reply.Store[int64(k)] = int64(v)
	}
	reply.Applied = r.fsm.applied
	return nil
}

// BeTheLeader is a no-op: HashiCorp Raft decides leadership, never this lab.
func (r *Replica) BeTheLeader(args *proto.BeTheLeaderArgs, reply *proto.BeTheLeaderReply) error {
	return nil
}

// LeaderId reports the leader chosen by HashiCorp Raft, as an index into the
// master's NodeList (or -1 while no leader is elected).
func (r *Replica) LeaderId(args *proto.LeaderIdArgs, reply *proto.LeaderIdReply) error {
	addr := r.raft.Leader()
	if addr == "" {
		reply.LeaderId = -1
		return nil
	}
	for i, p := range r.peers {
		if p.Address == addr {
			reply.LeaderId = i
			return nil
		}
	}
	reply.LeaderId = -1
	return nil
}

// Stats reports cumulative protocol counters. For Raft there is no fast/slow
// path distinction, so the counters are zero; the RPC exists so the runner
// can query both protocols uniformly.
func (r *Replica) Stats(args *proto.StatsArgs, reply *proto.StatsReply) error {
	reply.FastPath = 0
	reply.SlowPath = 0
	reply.Conflicted = 0
	return nil
}

// ElectionStats reports the measured election-failure counters (see
// proto.ElectionStatsReply). The runner queries every surviving replica
// after an election-failure run; the report derives the actual number of
// failed elections from these counters.
func (r *Replica) ElectionStats(args *proto.ElectionStatsArgs, reply *proto.ElectionStatsReply) error {
	if r.iso == nil {
		return nil
	}
	reply.DroppedPreVotes = r.iso.droppedPreVotes()
	reply.DroppedVotes = r.iso.droppedVotes()
	return nil
}

// IsolateElections is the election-failure injection hook. It makes this
// replica's Raft transport drop RequestVote/RequestPreVote for the requested
// duration, so the next election attempt(s) fail to obtain a quorum. The
// injection is a fault hook around the existing transport; HashiCorp Raft's
// election algorithm is not modified. DurationMS = 0 clears the isolation.
func (r *Replica) IsolateElections(args *proto.IsolateArgs, reply *proto.IsolateReply) error {
	if r.iso == nil {
		reply.OK = false
		return nil
	}
	if args.DurationMS <= 0 {
		r.iso.clear()
	} else {
		r.iso.isolateFor(time.Duration(args.DurationMS) * time.Millisecond)
	}
	reply.OK = true
	return nil
}

// isolatingTransport wraps a raft.Transport and drops vote requests while
// isolated. This is the mechanism behind the election-failure experiment:
// with votes unreachable, a candidate's election attempt fails; when the
// isolation is cleared, the next attempt succeeds.
//
// The wrapper also counts the vote requests it drops. HashiCorp Raft v1.7.3
// implements pre-vote as a SEPARATE RPC (RequestPreVote) and enables it by
// default. A failed election attempt therefore manifests as a pre-vote round
// that receives no response: the candidate sends RequestPreVote to every
// peer, all are dropped while isolated, and the node retries after the next
// randomized election timeout. droppedPreVotes is thus the primary measured
// counter (each failed attempt = one pre-vote round = replicas-1 dropped
// pre-votes); droppedVotes is a cross-check (expected ~0 while pre-vote is
// enabled, because the pre-vote fails before a real vote is sent).
type isolatingTransport struct {
	raft.Transport
	mu           sync.Mutex
	isolateUntil time.Time
	droppedPreV  int64 // atomic; RequestPreVote dropped while isolated (primary)
	droppedV     int64 // atomic; RequestVote dropped while isolated (cross-check)

	// commCostMS / commJitterPct inject a simulated network: every outbound
	// RPC waits commCostMS plus a uniform jitter before being sent.
	commCostMS    int
	commJitterPct int
}

// commDelay sleeps for the configured one-way latency of one inter-replica
// message: the fixed cost plus a uniform jitter in [0, cost*jitter/100].
// costMS=0 means no added latency (the local-network baseline).
func (t *isolatingTransport) commDelay() {
	if t.commCostMS <= 0 {
		return
	}
	base := time.Duration(t.commCostMS) * time.Millisecond
	if t.commJitterPct > 0 {
		maxJitter := base * time.Duration(t.commJitterPct) / 100
		base += time.Duration(rand.Int63n(int64(maxJitter) + 1))
	}
	time.Sleep(base)
}

// AppendEntries waits the communication delay before forwarding. This is the
// leader->follower replication RPC, the hot path of the protocol.
func (t *isolatingTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	t.commDelay()
	return t.Transport.AppendEntries(id, target, args, resp)
}

// AppendEntriesPipeline wraps the pipelined append path with the same delay.
// HashiCorp Raft pipelines appends when MaxRPCsInFlight >= 2 (the default),
// so without this wrapper the delay would only apply to the non-pipelined
// fallback.
func (t *isolatingTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	p, err := t.Transport.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	return &latencyPipeline{inner: p, delay: t.commDelay}, nil
}

// latencyPipeline wraps a raft.AppendPipeline so each pipelined AppendEntries
// waits the configured communication delay before being sent.
type latencyPipeline struct {
	inner raft.AppendPipeline
	delay func()
}

func (p *latencyPipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	p.delay()
	return p.inner.AppendEntries(args, resp)
}
func (p *latencyPipeline) Consumer() <-chan raft.AppendFuture { return p.inner.Consumer() }
func (p *latencyPipeline) Close() error                       { return p.inner.Close() }

func (t *isolatingTransport) isolateFor(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolateUntil = time.Now().Add(d)
}

func (t *isolatingTransport) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolateUntil = time.Time{}
}

func (t *isolatingTransport) isolated() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return time.Now().Before(t.isolateUntil)
}

func (t *isolatingTransport) droppedPreVotes() int64 { return atomic.LoadInt64(&t.droppedPreV) }
func (t *isolatingTransport) droppedVotes() int64    { return atomic.LoadInt64(&t.droppedV) }

// RequestVote drops real vote requests while isolated and counts the drops.
// Otherwise it waits the communication delay before forwarding.
func (t *isolatingTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	if t.isolated() {
		atomic.AddInt64(&t.droppedV, 1)
		return fmt.Errorf("transport isolated: vote request dropped (election-failure injection)")
	}
	t.commDelay()
	return t.Transport.RequestVote(id, target, args, resp)
}

// RequestPreVote drops pre-vote requests while isolated and counts the drops.
// Each dropped pre-vote is part of one failed election attempt (the
// candidate's pre-vote round). Pre-vote is a separate interface
// (raft.WithPreVote) in HashiCorp Raft v1.7.3. Otherwise it waits the
// communication delay before forwarding.
func (t *isolatingTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	if t.isolated() {
		atomic.AddInt64(&t.droppedPreV, 1)
		return fmt.Errorf("transport isolated: pre-vote request dropped (election-failure injection)")
	}
	t.commDelay()
	if pv, ok := t.Transport.(raft.WithPreVote); ok {
		return pv.RequestPreVote(id, target, args, resp)
	}
	return fmt.Errorf("underlying transport does not support pre-vote")
}

func (r *Replica) handlePropose(prop *proto.Propose, w *bufio.Writer) {
	var buf bytes.Buffer
	prop.Command.Marshal(&buf)
	f := r.raft.Apply(buf.Bytes(), *applyTO)
	reply := &proto.ProposeReplyTS{CommandId: prop.CommandId, Timestamp: time.Now().UnixNano()}
	if err := f.Error(); err != nil {
		reply.OK = 0
	} else {
		reply.OK = 1
		reply.Value = f.Response().(state.Value)
	}
	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) serveClients(port int) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("client listener: %v", err)
	}
	log.Printf("benchmark client listener on :%d", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go r.handleClient(conn)
	}
}

func (r *Replica) handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			return
		}
		switch msgType {
		case proto.PROPOSE:
			prop := new(proto.Propose)
			if err := prop.Unmarshal(reader); err != nil {
				return
			}
			r.handlePropose(prop, writer)
		default:
			log.Printf("unknown client message type %d", msgType)
			return
		}
	}
}

func registerWithMaster(masterAddr, myAddr string, port int) (int, []string) {
	args := &proto.RegisterArgs{Addr: myAddr, Port: port}
	var reply proto.RegisterReply
	for {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready {
				mcli.Close()
				return reply.ReplicaId, reply.NodeList
			}
			if err == nil {
				mcli.Close()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// resolvePeerAddr resolves a container hostname to the IP that HashiCorp
// Raft will advertise. NewTCPTransport resolves the bind address to an IP,
// so the peer/leader lists must use resolved IPs for raft.Raft.Leader() to
// match a peer entry. Retries while Docker DNS converges.
func resolvePeerAddr(host string, port int) (string, error) {
	var lastErr error
	for i := 0; i < 60; i++ {
		ips, err := net.LookupHost(host)
		if err == nil && len(ips) > 0 {
			return net.JoinHostPort(ips[0], strconv.Itoa(port)), nil
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	return "", fmt.Errorf("resolving %s: %w", host, lastErr)
}

func setupRaft(dir, bindAddr string, advertise *net.TCPAddr, peers []raft.Server, fsm raft.FSM) (*raft.Raft, *isolatingTransport, error) {
	// The storage backend is a configuration switch on github.com/hashicorp/raft's
	// own storage interface. The lab adds no persistence mechanism of its own:
	// both modes are provided by the library.
	var (
		store     raft.LogStore
		stable    raft.StableStore
		snapshots raft.SnapshotStore
	)
	switch *storeMode {
	case "bolt":
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, err
		}
		boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
		if err != nil {
			return nil, nil, err
		}
		fileSnaps, err := raft.NewFileSnapshotStore(dir, 2, os.Stderr)
		if err != nil {
			return nil, nil, err
		}
		store, stable, snapshots = boltStore, boltStore, fileSnaps
	case "inmem":
		inmem := raft.NewInmemStore()
		store, stable, snapshots = inmem, inmem, raft.NewInmemSnapshotStore()
	default:
		return nil, nil, fmt.Errorf("unknown -store %q (want bolt or inmem)", *storeMode)
	}
	transport, err := raft.NewTCPTransport(bindAddr, advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, nil, err
	}
	// Wrap the transport so the lab can inject election failures by
	// dropping vote requests. The wrapper delegates everything else.
	iso := &isolatingTransport{Transport: transport}
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(advertise.String())
	cfg.HeartbeatTimeout = time.Duration(*heartbeatMS) * time.Millisecond
	cfg.ElectionTimeout = time.Duration(*electionMS) * time.Millisecond
	cfg.LeaderLeaseTimeout = cfg.HeartbeatTimeout / 2
	cfg.CommitTimeout = 50 * time.Millisecond
	cfg.SnapshotThreshold = uint64(*snapThr)
	cfg.SnapshotInterval = 5 * time.Second
	cfg.TrailingLogs = uint64(*trailingLog)
	cfg.LogOutput = os.Stderr

	hasState, err := raft.HasExistingState(store, stable, snapshots)
	if err != nil {
		return nil, nil, err
	}
	if !hasState {
		conf := raft.Configuration{Servers: peers}
		if err := raft.BootstrapCluster(cfg, store, stable, snapshots, iso, conf); err != nil {
			return nil, nil, err
		}
	}
	r, err := raft.NewRaft(cfg, fsm, store, stable, snapshots, iso)
	if err != nil {
		return nil, nil, err
	}
	return r, iso, nil
}

// captureProfile writes a CPU profile covering the first secs seconds of the
// process and a heap profile at the end of that window, into dir. It exists
// for the diagnostic in docs/notes/2026-09-26-cpu-profile.md and is enabled
// only when PPROF_DIR is set, so recorded runs are unaffected.
func captureProfile(dir, addr string, secs int) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("pprof: mkdir %s: %v", dir, err)
		return
	}
	name := strings.ReplaceAll(addr, ":", "_")
	cpuPath := filepath.Join(dir, fmt.Sprintf("cpu-%s-%d.pprof", name, time.Now().Unix()))
	f, err := os.Create(cpuPath)
	if err != nil {
		log.Printf("pprof: create %s: %v", cpuPath, err)
		return
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Printf("pprof: start: %v", err)
		f.Close()
		return
	}
	log.Printf("pprof: capturing %ds of CPU into %s", secs, cpuPath)
	go func() {
		time.Sleep(time.Duration(secs) * time.Second)
		pprof.StopCPUProfile()
		f.Close()
		hp := filepath.Join(dir, "heap-"+name+".pprof")
		if hf, err := os.Create(hp); err == nil {
			runtime.GC()
			pprof.WriteHeapProfile(hf)
			hf.Close()
		}
		log.Printf("pprof: wrote %s", cpuPath)
	}()
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*gomaxprocs)
	if *myAddr == "" {
		host, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
		*myAddr = host
	}

	// Diagnostic only: PPROF_DIR is set by the profiling recipe in
	// docs/notes/2026-09-26-cpu-profile.md and never by a recorded run.
	if pdir := os.Getenv("PPROF_DIR"); pdir != "" {
		secs := 30
		if v := os.Getenv("PPROF_SECONDS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				secs = n
			}
		}
		captureProfile(pdir, *myAddr, secs)
	}

	replicaID, nodeList := registerWithMaster(*masterAddr, *myAddr, *clientPort)
	log.Printf("registered with master: id=%d nodeList=%v", replicaID, nodeList)

	// Resolve every peer (including this replica) to the IP HashiCorp Raft
	// will actually advertise, and build the peer set from those.
	peers := make([]raft.Server, len(nodeList))
	localAddr := ""
	for i, ap := range nodeList {
		host := strings.Split(ap, ":")[0]
		addr, err := resolvePeerAddr(host, *raftPort)
		if err != nil {
			log.Fatalf("resolving peer %s: %v", host, err)
		}
		peers[i] = raft.Server{ID: raft.ServerID(addr), Address: raft.ServerAddress(addr)}
		if host == *myAddr {
			localAddr = addr
		}
	}
	if localAddr == "" {
		log.Fatalf("own address %q not present in node list %v", *myAddr, nodeList)
	}
	advertise, err := net.ResolveTCPAddr("tcp", localAddr)
	if err != nil {
		log.Fatalf("resolving own advertise address: %v", err)
	}

	fsm := newFSM()
	r, iso, err := setupRaft(*dir, localAddr, advertise, peers, fsm)
	if err != nil {
		log.Fatalf("raft setup: %v", err)
	}
	iso.commCostMS = *commCostMS
	iso.commJitterPct = *commJitter

	rep := &Replica{id: replicaID, raft: r, peers: peers, iso: iso, fsm: fsm}
	rpc.Register(rep)
	rpc.HandleHTTP()
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *clientPort+1000))
		if err != nil {
			log.Fatalf("rpc listener: %v", err)
		}
		log.Printf("rpc listener on :%d", *clientPort+1000)
		http.Serve(ln, nil)
	}()

	rep.serveClients(*clientPort)
}
