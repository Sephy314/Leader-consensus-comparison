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
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
	applyTO     = flag.Duration("apply-timeout", 5*time.Second, "timeout for raft.Apply")
)

// fsm is the benchmark application state machine. It is separate from Raft
// and implements the same logical semantics as the EPaxos state machine
// (state.Command.Execute). Reads (GET) are routed through consensus exactly
// like writes; there is no Raft-only local-read optimization.
type fsm struct {
	mu sync.Mutex
	st *state.State
}

func newFSM() *fsm { return &fsm{st: state.InitState()} }

func (f *fsm) Apply(l *raft.Log) interface{} {
	var cmd state.Command
	if err := cmd.Unmarshal(bytes.NewReader(l.Data)); err != nil {
		return state.NIL
	}
	f.mu.Lock()
	defer f.mu.Unlock()
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
}

func (r *Replica) Ping(args *proto.PingArgs, reply *proto.PingReply) error { return nil }

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
type isolatingTransport struct {
	raft.Transport
	mu           sync.Mutex
	isolateUntil time.Time
}

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

// RequestVote drops both RequestVote and RequestPreVote (the pre-vote flag is
// carried in the request) while isolated.
func (t *isolatingTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	if t.isolated() {
		return fmt.Errorf("transport isolated: vote request dropped (election-failure injection)")
	}
	return t.Transport.RequestVote(id, target, args, resp)
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(dir, 2, os.Stderr)
	if err != nil {
		return nil, nil, err
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

	hasState, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		return nil, nil, err
	}
	if !hasState {
		conf := raft.Configuration{Servers: peers}
		if err := raft.BootstrapCluster(cfg, store, store, snapshots, iso, conf); err != nil {
			return nil, nil, err
		}
	}
	r, err := raft.NewRaft(cfg, fsm, store, store, snapshots, iso)
	if err != nil {
		return nil, nil, err
	}
	return r, iso, nil
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

	r, iso, err := setupRaft(*dir, localAddr, advertise, peers, newFSM())
	if err != nil {
		log.Fatalf("raft setup: %v", err)
	}

	rep := &Replica{id: replicaID, raft: r, peers: peers, iso: iso}
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
