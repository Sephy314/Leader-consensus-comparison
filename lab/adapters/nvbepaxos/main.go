// Command nvbepaxos-Server is the lab's adapter around
// github.com/nvanbenschoten/epaxos (EPaxos B).
//
// It does NOT implement consensus and it does NOT re-implement the benchmark.
// The workload, request timing, concurrency, measurement duration, metrics,
// result storage, and analysis are all provided by the shared lab components.
//
// Like the etcd Raft library, this EPaxos library is core-only: it implements
// the protocol state machine and explicitly leaves network transport, storage,
// and the Ready processing loop to the integrator. Following the library's
// documented usage, this adapter provides:
//
//  1. peer transport      — the library's own transport package (gRPC-based)
//  2. storage             — the library's in-memory default (Storage left nil),
//     which matches the original EPaxos implementation
//     used for EPaxos A: both keep replica state in memory
//  3. Ready loop          — step messages, execute committed commands, tick
//  4. proposal submission and committed-command execution
//  5. the benchmark client wire protocol (PROPOSE -> ProposeReplyTS)
//  6. the admin RPC surface the runner and the EPaxos master use
//
// The send path is deliberately decoupled from the Ready loop. The library's
// transport delivers a batch by opening a stream and waiting for the peer's
// handler to finish (CloseAndRecv), while the peer's handler can only finish
// once its own state-machine loop has drained the message. If that loop were
// also the one sending, two replicas would each wait for the other to drain
// and both would stop draining their own inbound channel: a hard deadlock,
// observed under concurrent write load. The Ready loop therefore only
// appends outbound messages to an ordered queue, and a dedicated sender
// goroutine performs the blocking network I/O.
//
// Integration choices the library does not provide a default for, and which
// are therefore recorded rather than tuned:
//
//   - Tick interval: the library counts its timers in ticks. Its own demo uses
//     10 ms; this adapter uses 5 ms to match the clock rate of the original
//     EPaxos implementation used for EPaxos A (whose fast clock runs at 5 ms).
//     The library's only timer is the slow-path timeout, 2 ticks.
//   - Command encoding: the benchmark's int64 key is encoded big-endian into
//     the library's Span.Key, with EndKey unset (a single-key span) and
//     Writing set from the operation, so the library's Span-overlap conflict
//     detection coincides with the benchmark's own conflict definition.
//   - Fast/slow-path counters are NOT exposed. The library has no such
//     counters, and the counters reported for EPaxos A are lab instrumentation
//     added to that codebase. This adapter reports zeros, and the sensitivity
//     analysis therefore does not use fast/slow-path ratios. See the paper's
//     threats to validity.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"conslab/internal/proto"
	"state"

	"github.com/nvanbenschoten/epaxos/epaxos"
	epaxospb "github.com/nvanbenschoten/epaxos/epaxos/epaxospb"
	"github.com/nvanbenschoten/epaxos/transport"
)

var (
	port       = flag.Int("port", 7070, "benchmark client port (genericsmr wire protocol)")
	peerPort   = flag.Int("peer-port", 6000, "EPaxos peer transport port")
	masterAddr = flag.String("maddr", "master", "master host")
	masterPort = flag.Int("mport", 7087, "master port")
	myAddr     = flag.String("addr", "", "advertised address of this replica (container hostname)")
	gomaxprocs = flag.Int("p", 4, "GOMAXPROCS")
	applyTO    = flag.Duration("apply-timeout", 5*time.Second, "timeout for a proposed command to be executed")
	commCostMS = flag.Int("comm-cost-ms", 0, "fixed one-way latency (ms) added to every inter-replica message; 0 = local-network baseline")
	commJitter = flag.Int("comm-jitter-pct", 0, "jitter as % of comm-cost-ms: each message waits an extra uniform delay in [0, cost*jitter/100]")
)

// tickInterval is the wall-clock duration of one library tick. The library's
// only timer is the slow-path timeout (2 ticks); 5 ms matches the clock rate
// of the original EPaxos implementation used for EPaxos A.
const tickInterval = 5 * time.Millisecond

// ---- request tracking ----

// pending correlates an in-flight client request with the result of the
// command once it has been executed. The library returns executed commands,
// not per-request futures, so the correlation is explicit.
type pending struct {
	mu   sync.Mutex
	m    map[uint64]chan state.Value
	base uint64
	seq  atomic.Uint64
}

func newPending(replicaID int) *pending {
	return &pending{
		m:    make(map[uint64]chan state.Value),
		base: uint64(replicaID)<<48 | 1,
	}
}

func (p *pending) next() uint64 { return p.base + p.seq.Add(1) }

func (p *pending) add(id uint64) chan state.Value {
	ch := make(chan state.Value, 1)
	p.mu.Lock()
	p.m[id] = ch
	p.mu.Unlock()
	return ch
}

func (p *pending) drop(id uint64) {
	p.mu.Lock()
	delete(p.m, id)
	p.mu.Unlock()
}

func (p *pending) deliver(id uint64, v state.Value) {
	p.mu.Lock()
	ch, ok := p.m[id]
	if ok {
		delete(p.m, id)
	}
	p.mu.Unlock()
	if ok {
		ch <- v
	}
}

func keyBytes(k state.Key) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(k))
	return b[:]
}

// ---- replica ----

type Server struct {
	id   int
	node epaxos.Node
	tr   *transport.EPaxosServer
	st   *state.State
	pend *pending

	appliedN atomic.Int64 // client commands applied, for the correctness harness

	peerMu    sync.Mutex
	peers     map[epaxospb.ReplicaID]*transport.EPaxosClient
	peerAddrs map[epaxospb.ReplicaID]string

	// outbox is the ordered queue of outbound messages. It is drained by a
	// single sender goroutine, which keeps per-peer ordering while ensuring
	// the Ready loop never blocks on network I/O.
	outMu   sync.Mutex
	outbox  []epaxospb.Message
	outWake chan struct{}

	// commCostMS / commJitterPct inject a simulated network: every outbound
	// message waits commCostMS plus a uniform jitter before being sent.
	commCostMS    int
	commJitterPct int
}

// Ping, BeTheLeader, LeaderId and Stats form the admin RPC surface the runner
// and the EPaxos master use. EPaxos is leaderless, so LeaderId is always -1.
func (s *Server) Ping(args *proto.PingArgs, reply *proto.PingReply) error { return nil }

// GetState returns the state-machine snapshot and the applied-command count
// for the correctness harness. Read-only; never used by the benchmark.
func (s *Server) GetState(args *proto.GetStateArgs, reply *proto.GetStateReply) error {
	snap := s.st.Snapshot()
	reply.Store = make(map[int64]int64, len(snap))
	for k, v := range snap {
		reply.Store[int64(k)] = int64(v)
	}
	reply.Applied = s.appliedN.Load()
	return nil
}

func (s *Server) BeTheLeader(args *proto.BeTheLeaderArgs, reply *proto.BeTheLeaderReply) error {
	return nil
}

func (s *Server) LeaderId(args *proto.LeaderIdArgs, reply *proto.LeaderIdReply) error {
	reply.LeaderId = -1
	return nil
}

// Stats reports protocol counters. This library exposes no fast/slow-path
// counters (those reported for EPaxos A are instrumentation added to that
// codebase), so the counters are zero and the sensitivity analysis does not
// use them.
func (s *Server) Stats(args *proto.StatsArgs, reply *proto.StatsReply) error {
	reply.FastPath = 0
	reply.SlowPath = 0
	reply.Conflicted = 0
	return nil
}

func (s *Server) run(ctx context.Context) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.node.Tick()
		case m := <-s.tr.Msgs():
			if err := s.node.Step(ctx, *m); err != nil {
				log.Printf("step of message to %d: %v", m.GetTo(), err)
			}
		case rd := <-s.node.Ready():
			s.enqueue(rd.Messages)
			s.execute(rd.ExecutedCommands)
		case <-ctx.Done():
			return
		}
	}
}

// execute applies executed commands to the benchmark state machine and
// delivers each result to the client request that produced it.
func (s *Server) execute(cmds []epaxospb.Command) {
	for _, c := range cmds {
		if len(c.Data) == 0 {
			continue
		}
		var cmd state.Command
		if err := cmd.Unmarshal(bytes.NewReader(c.Data)); err != nil {
			continue
		}
		s.pend.deliver(c.GetID(), cmd.Execute(s.st))
		s.appliedN.Add(1)
	}
}

// enqueue appends outbound messages to the sender's queue. It never blocks on
// the network, so the Ready loop always returns to draining inbound messages.
func (s *Server) enqueue(msgs []epaxospb.Message) {
	if len(msgs) == 0 {
		return
	}
	s.outMu.Lock()
	s.outbox = append(s.outbox, msgs...)
	queued := len(s.outbox)
	s.outMu.Unlock()
	if queued > outboxWarn {
		// Protocol messages are never dropped (that would break liveness),
		// so a growing backlog is reported rather than hidden.
		log.Printf("outbox backlog: %d messages queued", queued)
	}
	select {
	case s.outWake <- struct{}{}:
	default:
	}
}

// outboxWarn is the queue depth at which the sender reports a backlog.
const outboxWarn = 100000

// sender drains the outbox in order. It is the only goroutine performing
// outbound network I/O, which preserves per-peer message ordering.
func (s *Server) sender(ctx context.Context) {
	for {
		select {
		case <-s.outWake:
		case <-ctx.Done():
			return
		}
		for {
			s.outMu.Lock()
			if len(s.outbox) == 0 {
				s.outMu.Unlock()
				break
			}
			batch := s.outbox
			s.outbox = nil
			s.outMu.Unlock()
			if d := commDelay(s.commCostMS, s.commJitterPct); d > 0 {
				time.Sleep(d)
			}
			s.sendBatch(ctx, batch)
		}
	}
}

// commDelay returns the extra one-way latency for one inter-replica message:
// the fixed cost plus a uniform jitter in [0, cost*jitter/100]. costMS=0
// means no added latency (the local-network baseline).
func commDelay(costMS, jitterPct int) time.Duration {
	if costMS <= 0 {
		return 0
	}
	base := time.Duration(costMS) * time.Millisecond
	if jitterPct <= 0 {
		return base
	}
	maxJitter := base * time.Duration(jitterPct) / 100
	return base + time.Duration(rand.Int63n(int64(maxJitter)+1))
}

func (s *Server) sendBatch(ctx context.Context, msgs []epaxospb.Message) {
	byPeer := make(map[epaxospb.ReplicaID][]epaxospb.Message)
	for _, m := range msgs {
		if int(m.GetTo()) == s.id {
			continue // local delivery is handled by the library
		}
		byPeer[m.GetTo()] = append(byPeer[m.GetTo()], m)
	}
	for to, list := range byPeer {
		if err := s.sendTo(ctx, to, list); err != nil {
			log.Printf("send to %d: %v", to, err)
		}
	}
}

// sendTo delivers a batch of messages to one peer over one stream, matching
// the library's own transport usage.
func (s *Server) sendTo(ctx context.Context, to epaxospb.ReplicaID, msgs []epaxospb.Message) error {
	c, err := s.peer(ctx, to)
	if err != nil {
		return err
	}
	stream, err := c.DeliverMessage(ctx)
	if err != nil {
		s.dropPeer(to)
		return err
	}
	for i := range msgs {
		if err := stream.Send(&msgs[i]); err != nil {
			s.dropPeer(to)
			return err
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		s.dropPeer(to)
		return err
	}
	return nil
}

func (s *Server) peer(ctx context.Context, to epaxospb.ReplicaID) (*transport.EPaxosClient, error) {
	s.peerMu.Lock()
	if c := s.peers[to]; c != nil {
		s.peerMu.Unlock()
		return c, nil
	}
	addr, ok := s.peerAddrs[to]
	s.peerMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no address for peer %d", to)
	}
	// The library's client blocks until the peer's transport is up.
	c, err := transport.NewEPaxosClient(addr)
	if err != nil {
		return nil, fmt.Errorf("dial peer %d at %s: %w", to, addr, err)
	}
	s.peerMu.Lock()
	s.peers[to] = c
	s.peerMu.Unlock()
	return c, nil
}

func (s *Server) dropPeer(to epaxospb.ReplicaID) {
	s.peerMu.Lock()
	if c := s.peers[to]; c != nil {
		c.Close()
		delete(s.peers, to)
	}
	s.peerMu.Unlock()
}

// ---- benchmark client protocol ----

func (s *Server) handlePropose(prop *proto.Propose, w *bufio.Writer) {
	reply := &proto.ProposeReplyTS{CommandId: prop.CommandId, Timestamp: time.Now().UnixNano()}

	cmdID := s.pend.next()
	ch := s.pend.add(cmdID)

	var cmdBuf bytes.Buffer
	prop.Command.Marshal(&cmdBuf)

	ctx, cancel := context.WithTimeout(context.Background(), *applyTO)
	defer cancel()
	err := s.node.Propose(ctx, epaxospb.Command{
		ID:      cmdID,
		Span:    epaxospb.Span{Key: keyBytes(prop.Command.K)},
		Writing: prop.Command.Op == state.PUT,
		Data:    cmdBuf.Bytes(),
	})
	if err != nil {
		s.pend.drop(cmdID)
		reply.OK = 0
		reply.Marshal(w)
		w.Flush()
		return
	}

	select {
	case v := <-ch:
		reply.OK = 1
		reply.Value = v
	case <-time.After(*applyTO):
		s.pend.drop(cmdID)
		reply.OK = 0
	}
	reply.Marshal(w)
	w.Flush()
}

func (s *Server) serveClients() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("client listener: %v", err)
	}
	log.Printf("benchmark client listener on :%d", *port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn net.Conn) {
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
			s.handlePropose(prop, writer)
		default:
			log.Printf("unknown client message type %d", msgType)
			return
		}
	}
}

// ---- startup ----

func registerWithMaster(masterAddr string, myAddr string, port int) (int, []string) {
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
			mcli.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
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

	replicaID, nodeList := registerWithMaster(
		fmt.Sprintf("%s:%d", *masterAddr, *masterPort), *myAddr, *port)
	log.Printf("registered with master: id=%d nodeList=%v", replicaID, nodeList)

	// The library's peer transport is a separate port from the benchmark
	// client protocol, so peer addresses are derived from the master's node
	// list (which carries the client port) by substituting the peer port.
	s := &Server{
		id:        replicaID,
		st:        state.InitState(),
		pend:      newPending(replicaID),
		peers:     make(map[epaxospb.ReplicaID]*transport.EPaxosClient),
		peerAddrs: make(map[epaxospb.ReplicaID]string, len(nodeList)),
		outWake:   make(chan struct{}, 1),
	}
	s.commCostMS = *commCostMS
	s.commJitterPct = *commJitter
	nodes := make([]epaxospb.ReplicaID, len(nodeList))
	for i, ap := range nodeList {
		host := strings.Split(ap, ":")[0]
		s.peerAddrs[epaxospb.ReplicaID(i)] = net.JoinHostPort(host, itoa(*peerPort))
		nodes[i] = epaxospb.ReplicaID(i)
	}

	ps, err := transport.NewEPaxosServer(*peerPort)
	if err != nil {
		log.Fatalf("peer transport listener on :%d: %v", *peerPort, err)
	}
	s.tr = ps
	log.Printf("peer transport on :%d, %d peers", *peerPort, len(nodes))

	// Storage is left nil, so the library uses its in-memory default. That
	// matches the original EPaxos implementation used for EPaxos A, which
	// keeps replica state in memory by upstream default.
	cfg := &epaxos.Config{
		ID:     epaxospb.ReplicaID(replicaID),
		Nodes:  nodes,
		Logger: epaxos.NewDefaultLogger(),
	}
	s.node = epaxos.StartNode(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.run(ctx)
	go s.sender(ctx)

	go func() {
		if err := ps.Serve(); err != nil {
			log.Fatalf("peer transport serve: %v", err)
		}
	}()

	rpc.Register(s)
	rpc.HandleHTTP()
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port+1000))
		if err != nil {
			log.Fatalf("rpc listener: %v", err)
		}
		log.Printf("rpc listener on :%d", *port+1000)
		http.Serve(ln, nil)
	}()

	s.serveClients()
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }
