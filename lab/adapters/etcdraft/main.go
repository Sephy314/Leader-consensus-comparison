// Command etcdraftadapter is the lab's adapter around go.etcd.io/raft (Raft B).
//
// It does NOT implement consensus and it does NOT re-implement the benchmark.
// The workload, request timing, concurrency, measurement duration, metrics,
// result storage, and analysis are all provided by the shared lab components;
// this binary only integrates the etcd Raft library.
//
// The etcd library differs in kind from HashiCorp Raft (Raft A): it implements
// only the core Raft state machine and explicitly leaves transport, storage,
// and the processing loop to the integrator. Following the library's
// documented usage, this adapter therefore provides:
//
//  1. peer transport      — TCP, one persistent connection per peer
//  2. storage             — MemoryStorage plus a fsynced write-ahead log, so
//     HardState and Entries are durable before the
//     corresponding Ready messages are sent
//  3. Ready loop          — route Ready messages: local storage messages to
//     dedicated append/apply threads, everything else to the transport
//  4. proposal submission and committed-entry application
//  5. the benchmark client wire protocol (PROPOSE -> ProposeReplyTS)
//  6. the admin RPC surface the runner and the raft master use
//
// Integration choices that the library does not provide a default for, and
// which are therefore recorded rather than tuned:
//
//   - Election/heartbeat timeouts: the library is configured in ticks, so the
//     same millisecond timeouts the HashiCorp adapter uses are converted at a
//     fixed tick interval (100 ms), giving ElectionTick=20 / HeartbeatTick=10
//     for the default 2000 ms / 1000 ms.
//   - MaxSizePerMsg / MaxInflightMsgs: required by the library; both are set
//     to the values in the library's own README example (4096 / 256).
//   - PreVote: enabled, matching HashiCorp Raft v1.7.3, which enables
//     pre-vote by default.
//   - AsyncStorageWrites: enabled. The default Ready/Advance contract
//     serializes the processing loop (the next Ready waits for Advance), so
//     fsync and the network send sit on the proposal critical path. The async
//     interface turns storage writes into messages handled by dedicated
//     threads, with batched WAL fsyncs — the same structure HashiCorp Raft
//     has internally (group commit plus a separate FSM goroutine).
//   - Log compaction: the library has no snapshot threshold/trailing-logs
//     policy, so entries below (applied - trailing-logs) are compacted, which
//     mirrors HashiCorp Raft's TrailingLogs setting.
package main

import (
	"bufio"
	"bytes"
	"context"
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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"conslab/internal/proto"
	"state"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	gproto "google.golang.org/protobuf/proto"
)

var (
	masterAddr  = flag.String("master", "master:7087", "raft master address (host:port)")
	myAddr      = flag.String("addr", "", "advertised address of this replica (container hostname)")
	clientPort  = flag.Int("client-port", 7070, "benchmark client port (genericsmr wire protocol)")
	raftPort    = flag.Int("raft-port", 6000, "raft peer transport port")
	dir         = flag.String("dir", "/data", "directory for the write-ahead log")
	noWAL       = flag.Bool("no-wal", false, "run the raft core with in-memory storage only (no write-ahead log, no fsync)")
	gomaxprocs  = flag.Int("gomaxprocs", 4, "GOMAXPROCS")
	heartbeatMS = flag.Int("heartbeat-ms", 1000, "raft heartbeat timeout (ms)")
	electionMS  = flag.Int("election-ms", 2000, "raft election timeout (ms)")
	trailingLog = flag.Int("trailing-logs", 1024, "log entries kept after compaction")
	applyTO     = flag.Duration("apply-timeout", 5*time.Second, "timeout for a proposal to be applied")
)

// tickInterval is the wall-clock duration of one raft tick. The library counts
// election and heartbeat timeouts in ticks, so this is what converts the
// configured millisecond timeouts into tick counts.
const tickInterval = 100 * time.Millisecond

// maxFrame bounds a single received transport frame.
const maxFrame = 8 << 20

func ticksFor(ms int) int {
	t := int(time.Duration(ms) * time.Millisecond / tickInterval)
	if t < 1 {
		t = 1
	}
	return t
}

func walPath() string { return filepath.Join(*dir, "raft.wal") }

// ---- request tracking ----

// pending correlates an in-flight client request with the state-machine result
// of the log entry it produced. HashiCorp Raft returns a future from
// raft.Apply; the etcd library does not, so the correlation is explicit: the
// request id is encoded into the proposed payload and recovered when the entry
// is applied.
type pending struct {
	mu   sync.Mutex
	m    map[uint64]chan state.Value
	base uint64
	seq  atomic.Uint64
}

func newPending(replicaID int) *pending {
	return &pending{
		m:    make(map[uint64]chan state.Value),
		base: uint64(replicaID) << 56, // per-replica prefix: ids never collide
	}
}

func (p *pending) next() uint64 {
	return p.base | (p.seq.Add(1) & 0x00FFFFFFFFFFFFFF)
}

func (p *pending) add(reqID uint64) chan state.Value {
	ch := make(chan state.Value, 1)
	p.mu.Lock()
	p.m[reqID] = ch
	p.mu.Unlock()
	return ch
}

func (p *pending) drop(reqID uint64) {
	p.mu.Lock()
	delete(p.m, reqID)
	p.mu.Unlock()
}

func (p *pending) deliver(reqID uint64, v state.Value) {
	p.mu.Lock()
	ch, ok := p.m[reqID]
	if ok {
		delete(p.m, reqID)
	}
	p.mu.Unlock()
	if ok {
		ch <- v
	}
}

const reqIDLen = 8

func encodeCommand(reqID uint64, cmd []byte) []byte {
	buf := make([]byte, reqIDLen+len(cmd))
	binary.BigEndian.PutUint64(buf[:reqIDLen], reqID)
	copy(buf[reqIDLen:], cmd)
	return buf
}

func decodeCommand(b []byte) (uint64, state.Command, bool) {
	if len(b) <= reqIDLen {
		return 0, state.Command{}, false
	}
	var cmd state.Command
	if err := cmd.Unmarshal(bytes.NewReader(b[reqIDLen:])); err != nil {
		return 0, state.Command{}, false
	}
	return binary.BigEndian.Uint64(b[:reqIDLen]), cmd, true
}

// ---- peer transport ----

// transport carries raftpb messages between replicas over TCP. The etcd
// library ships no transport, so this is the adapter's. Each peer gets one
// persistent connection; writes to different peers are independent, so the
// transport introduces no global lock on the send path.
//
// The send path is deliberately decoupled from the Ready loop: send()
// marshals immediately and enqueues the bytes, and a per-peer sender
// goroutine performs the blocking network I/O. The library's Ready channel
// is unbuffered, so a synchronous send inside the Ready loop would stall
// proposal and inbound processing whenever a peer's TCP buffer is full.
// The queue is bounded so the loop cannot race ahead of the network.
type transport struct {
	selfID uint64
	port   int
	addrs  map[uint64]string
	step   func(context.Context, *pb.Message) error

	mu    sync.Mutex
	conns map[uint64]*peerConn

	// outbox is the per-peer bounded queue of marshalled outbound
	// messages, drained by that peer's sender goroutine. Per-peer ordering
	// is preserved because each peer has exactly one sender.
	outbox map[uint64]chan []byte

	isoUntil   atomic.Int64
	droppedPre atomic.Int64
	droppedV   atomic.Int64
}

type peerConn struct {
	addr string
	mu   sync.Mutex
	nc   net.Conn
	w    *bufio.Writer
}

func newTransport(selfID uint64, port int, addrs map[uint64]string) *transport {
	outbox := make(map[uint64]chan []byte, len(addrs))
	for id := range addrs {
		outbox[id] = make(chan []byte, outboxCap)
	}
	return &transport{
		selfID: selfID,
		port:   port,
		addrs:  addrs,
		conns:  make(map[uint64]*peerConn),
		outbox: outbox,
	}
}

func (t *transport) listen() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", t.port))
	if err != nil {
		return err
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go t.serve(c)
		}
	}()
	return nil
}

func (t *transport) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReaderSize(c, 64<<10)
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n == 0 || n > maxFrame {
			return
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return
		}
		var m pb.Message
		if err := gproto.Unmarshal(buf, &m); err != nil {
			return
		}
		if err := t.step(context.Background(), &m); err != nil {
			return
		}
	}
}

func (t *transport) peer(id uint64) *peerConn {
	t.mu.Lock()
	defer t.mu.Unlock()
	pc := t.conns[id]
	if pc == nil {
		pc = &peerConn{addr: t.addrs[id]}
		t.conns[id] = pc
	}
	return pc
}

func (pc *peerConn) ensure() error {
	if pc.w != nil {
		return nil
	}
	nc, err := net.DialTimeout("tcp", pc.addr, 2*time.Second)
	if err != nil {
		return err
	}
	pc.nc = nc
	pc.w = bufio.NewWriterSize(nc, 64<<10)
	return nil
}

func (pc *peerConn) write(frame []byte) error {
	if err := pc.ensure(); err != nil {
		return err
	}
	if err := pc.nc.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	if _, err := pc.w.Write(frame); err != nil {
		return err
	}
	return pc.w.Flush()
}

func (pc *peerConn) close() {
	if pc.nc != nil {
		pc.nc.Close()
	}
	pc.nc, pc.w = nil, nil
}

// outboxCap bounds each peer's outbound queue. A bounded queue provides
// backpressure: when a peer's queue is full, the Ready loop waits instead
// of racing ahead of the network.
const outboxCap = 1024

// send marshals each message immediately and enqueues the bytes for the
// peer's sender goroutine. Marshalling happens here, not in the sender,
// because the raftpb.Message objects are reused by the library. The queue
// is bounded, so send blocks only when the peer is genuinely slow, never
// on the network itself. While election isolation is active, vote requests
// are dropped and counted instead of being delivered, which is the same
// injection the HashiCorp adapter performs.
func (t *transport) send(msgs []*pb.Message) {
	for _, m := range msgs {
		if t.isolated() {
			switch m.GetType() {
			case pb.MessageType_MsgPreVote:
				t.droppedPre.Add(1)
				continue
			case pb.MessageType_MsgVote:
				t.droppedV.Add(1)
				continue
			}
		}
		if m.GetTo() == t.selfID {
			if err := t.step(context.Background(), m); err != nil {
				log.Printf("local step: %v", err)
			}
			continue
		}
		body, err := gproto.Marshal(m)
		if err != nil {
			log.Printf("marshal: %v", err)
			continue
		}
		frame := make([]byte, 4+len(body))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
		copy(frame[4:], body)
		t.outbox[m.GetTo()] <- frame
	}
}

// sender is the per-peer goroutine performing the blocking writes. It is
// the only goroutine writing to its peer's connection, so per-peer message
// ordering is preserved and the Ready loop never blocks on the network.
func (t *transport) sender(id uint64) {
	for frame := range t.outbox[id] {
		if err := t.sendOne(id, frame); err != nil {
			log.Printf("send to %d: %v", id, err)
		}
	}
}

func (t *transport) sendOne(id uint64, frame []byte) error {
	pc := t.peer(id)
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if err := pc.write(frame); err != nil {
		// A stale persistent connection is expected after a restart or a
		// network hiccup: reconnect once before giving up.
		pc.close()
		if err := pc.write(frame); err != nil {
			pc.close()
			return err
		}
	}
	return nil
}

func (t *transport) isolateFor(d time.Duration) { t.isoUntil.Store(time.Now().Add(d).UnixNano()) }
func (t *transport) clear()                     { t.isoUntil.Store(0) }
func (t *transport) isolated() bool {
	u := t.isoUntil.Load()
	return u != 0 && time.Now().UnixNano() < u
}
func (t *transport) droppedPreVotes() int64 { return t.droppedPre.Load() }
func (t *transport) droppedVotes() int64    { return t.droppedV.Load() }

// ---- replica ----

// Replica is the RPC surface exposed to the lab's raft master and to the
// runner (clientPort+1000), mirroring the HashiCorp adapter's surface.
type Replica struct {
	myIdx   int
	myID    uint64 // raft id (nodeList index + 1)
	node    raft.Node
	ms      *raft.MemoryStorage
	tr      *transport
	pend    *pending
	st      *state.State
	lead    atomic.Int64
	applied atomic.Uint64 // last applied index, published for the append thread's compaction
}

func (r *Replica) Ping(args *proto.PingArgs, reply *proto.PingReply) error { return nil }

// BeTheLeader is a no-op: etcd/raft decides leadership, never this lab.
func (r *Replica) BeTheLeader(args *proto.BeTheLeaderArgs, reply *proto.BeTheLeaderReply) error {
	return nil
}

// LeaderId reports the leader chosen by etcd/raft, as an index into the
// master's NodeList (-1 while no leader is known).
func (r *Replica) LeaderId(args *proto.LeaderIdArgs, reply *proto.LeaderIdReply) error {
	reply.LeaderId = int(r.lead.Load())
	return nil
}

// Stats reports protocol counters. etcd/raft has no fast/slow path, so the
// counters are zero; the RPC exists so the runner can query both protocols
// uniformly.
func (r *Replica) Stats(args *proto.StatsArgs, reply *proto.StatsReply) error {
	reply.FastPath = 0
	reply.SlowPath = 0
	reply.Conflicted = 0
	return nil
}

// ElectionStats reports the measured election-failure counters.
func (r *Replica) ElectionStats(args *proto.ElectionStatsArgs, reply *proto.ElectionStatsReply) error {
	reply.DroppedPreVotes = r.tr.droppedPreVotes()
	reply.DroppedVotes = r.tr.droppedVotes()
	return nil
}

// IsolateElections is the election-failure injection hook: the transport drops
// vote requests for the requested duration, so the next election attempt(s)
// fail to obtain a quorum. DurationMS = 0 clears the isolation.
func (r *Replica) IsolateElections(args *proto.IsolateArgs, reply *proto.IsolateReply) error {
	if args.DurationMS <= 0 {
		r.tr.clear()
	} else {
		r.tr.isolateFor(time.Duration(args.DurationMS) * time.Millisecond)
	}
	reply.OK = true
	return nil
}

func (r *Replica) handlePropose(prop *proto.Propose, w *bufio.Writer) {
	reply := &proto.ProposeReplyTS{CommandId: prop.CommandId, Timestamp: time.Now().UnixNano()}

	reqID := r.pend.next()
	ch := r.pend.add(reqID)

	var cmdBuf bytes.Buffer
	prop.Command.Marshal(&cmdBuf)

	ctx, cancel := context.WithTimeout(context.Background(), *applyTO)
	defer cancel()
	if err := r.node.Propose(ctx, encodeCommand(reqID, cmdBuf.Bytes())); err != nil {
		// The library may drop proposals (for example when the uncommitted
		// size limit is exceeded); the request is reported as failed, exactly
		// as HashiCorp Raft's Apply future reports an error.
		r.pend.drop(reqID)
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
		r.pend.drop(reqID)
		reply.OK = 0
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

// ---- ready loop ----

// The library's default Ready/Advance contract serializes the whole
// processing loop: the next Ready is not produced until Advance() is called,
// so a slow fsync or a slow network write stalls proposals. The library
// offers an alternative interface, AsyncStorageWrites, which turns local
// storage operations into messages (MsgStorageAppend / MsgStorageApply)
// carried in Ready.Message. The main loop then only routes messages; the
// durable write and the state-machine apply happen on dedicated threads, and
// the responses (which include the messages that must wait for durability,
// e.g. MsgApp to followers) are delivered by those threads. This is the
// library's documented usage (doc.go, "Usage with Asynchronous Storage
// Writes") and mirrors HashiCorp Raft's structure: a leader loop that only
// dispatches, a separate FSM goroutine, and batched log stores.

// appendBatchMax bounds how many MsgStorageAppend messages one WAL write
// covers. HashiCorp Raft groups up to MaxAppendEntries (64) proposals into
// one StoreLogs transaction; this is the same group-commit bound for the
// etcd adapter.
const appendBatchMax = 64

// appendThread owns the durable log. It receives MsgStorageAppend messages
// in order, writes their entries and hard state to the WAL (one write, one
// fsync per batch), mirrors them into MemoryStorage, compacts the log, and
// only then delivers the message responses — the messages that must not be
// sent before the write is durable (MsgApp to followers) and the
// MsgStorageAppendResp that tells raft the entries are stable.
func (r *Replica) appendThread(w *wal, node raft.Node, ch <-chan *pb.Message) {
	var lastCompact uint64
	for {
		m, ok := <-ch
		if !ok {
			return
		}
		batch := []*pb.Message{m}
	drain:
		for len(batch) < appendBatchMax {
			select {
			case m2 := <-ch:
				batch = append(batch, m2)
			default:
				break drain
			}
		}

		items := make([]walItem, 0, len(batch))
		sync := false
		var lastAppended uint64
		for _, bm := range batch {
			hs := &pb.HardState{Term: bm.Term, Vote: bm.Vote, Commit: bm.Commit}
			if !raft.IsEmptyHardState(hs) || len(bm.GetEntries()) > 0 {
				items = append(items, walItem{hs: hs, entries: bm.GetEntries()})
			}
			// Durability is required exactly when someone is waiting on it:
			// the message's responses are delivered only after the write.
			if len(bm.GetResponses()) > 0 {
				sync = true
			}
			if n := len(bm.GetEntries()); n > 0 {
				lastAppended = bm.GetEntries()[n-1].GetIndex()
			}
			if !raft.IsEmptySnap(bm.GetSnapshot()) {
				if err := r.ms.ApplySnapshot(bm.GetSnapshot()); err != nil {
					log.Fatalf("apply snapshot: %v", err)
				}
			}
		}
		if w != nil {
			if err := w.appendBatch(items, sync); err != nil {
				log.Fatalf("wal append: %v", err)
			}
		}
		for _, bm := range batch {
			if len(bm.GetEntries()) > 0 {
				if err := r.ms.Append(bm.GetEntries()); err != nil {
					log.Fatalf("storage append: %v", err)
				}
			}
			hs := &pb.HardState{Term: bm.Term, Vote: bm.Vote, Commit: bm.Commit}
			if !raft.IsEmptyHardState(hs) {
				if err := r.ms.SetHardState(hs); err != nil {
					log.Fatalf("set hard state: %v", err)
				}
			}
		}

		// Compact below (applied - trailing-logs), mirroring HashiCorp
		// Raft's TrailingLogs policy. The apply thread publishes the applied
		// index; compaction only ever covers entries this thread has already
		// appended, so MemoryStorage.Compact cannot run past the stable log.
		if applied := r.applied.Load(); applied > uint64(*trailingLog)+1 {
			if to := applied - uint64(*trailingLog); to > lastCompact && to <= lastAppended {
				if err := r.ms.Compact(to); err == nil {
					lastCompact = to
				}
			}
		}

		for _, bm := range batch {
			for _, resp := range bm.GetResponses() {
				if resp.GetTo() == r.myID {
					if err := node.Step(context.Background(), resp); err != nil {
						log.Printf("local step: %v", err)
					}
				} else {
					r.tr.send([]*pb.Message{resp})
				}
			}
		}
	}
}

// applyThread owns the state machine. It receives MsgStorageApply messages
// in order, applies the committed entries, and delivers the responses (the
// MsgStorageApplyResp that tells raft the entries are applied).
func (r *Replica) applyThread(node raft.Node, ch <-chan *pb.Message) {
	for m := range ch {
		for _, e := range m.GetEntries() {
			switch e.GetType() {
			case pb.EntryType_EntryNormal:
				if len(e.Data) == 0 {
					continue // empty entry appended by a new leader
				}
				reqID, cmd, ok := decodeCommand(e.Data)
				if !ok {
					continue
				}
				r.pend.deliver(reqID, cmd.Execute(r.st))
			case pb.EntryType_EntryConfChange:
				var cc pb.ConfChange
				if err := gproto.Unmarshal(e.Data, &cc); err == nil {
					node.ApplyConfChange(&cc)
				}
			case pb.EntryType_EntryConfChangeV2:
				var cc pb.ConfChangeV2
				if err := gproto.Unmarshal(e.Data, &cc); err == nil {
					node.ApplyConfChange(&cc)
				}
			}
			r.applied.Store(e.GetIndex())
		}
		for _, resp := range m.GetResponses() {
			if resp.GetTo() == r.myID {
				if err := node.Step(context.Background(), resp); err != nil {
					log.Printf("local step: %v", err)
				}
			} else {
				r.tr.send([]*pb.Message{resp})
			}
		}
	}
}

// run is the library's documented async-storage-writes loop: tick, read
// Ready, and route every message — local storage messages to their threads,
// everything else to the transport. There is no Advance: the storage
// threads' responses take its place.
func (r *Replica) run(w *wal, node raft.Node) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	toAppend := make(chan *pb.Message, 1024)
	toApply := make(chan *pb.Message, 1024)
	go r.appendThread(w, node, toAppend)
	go r.applyThread(node, toApply)
	for {
		select {
		case <-ticker.C:
			node.Tick()
		case rd := <-node.Ready():
			if rd.SoftState != nil {
				if rd.SoftState.Lead == 0 {
					r.lead.Store(-1)
				} else {
					r.lead.Store(int64(rd.SoftState.Lead) - 1) // raft ids are index+1
				}
			}
			for _, m := range rd.Messages {
				switch m.GetTo() {
				case raft.LocalAppendThread:
					toAppend <- m
				case raft.LocalApplyThread:
					toApply <- m
				default:
					r.tr.send([]*pb.Message{m})
				}
			}
		}
	}
}

// ---- startup ----

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
	if !*noWAL {
		if err := os.MkdirAll(*dir, 0o755); err != nil {
			log.Fatal(err)
		}
	}

	replicaID, nodeList := registerWithMaster(*masterAddr, *myAddr, *clientPort)
	log.Printf("registered with master: id=%d nodeList=%v", replicaID, nodeList)

	// Raft ids must be non-zero, so the replica index is shifted by one.
	myID := uint64(replicaID) + 1
	peerAddrs := make(map[uint64]string, len(nodeList))
	peers := make([]raft.Peer, 0, len(nodeList))
	for i, ap := range nodeList {
		id := uint64(i) + 1
		host := strings.Split(ap, ":")[0]
		peerAddrs[id] = net.JoinHostPort(host, itoa(*raftPort))
		peers = append(peers, raft.Peer{ID: id})
	}

	ms := raft.NewMemoryStorage()
	var w *wal
	if !*noWAL {
		if err := loadWAL(walPath(), ms); err != nil {
			log.Fatalf("replaying wal: %v", err)
		}
		var err error
		w, err = openWAL(walPath())
		if err != nil {
			log.Fatalf("opening wal: %v", err)
		}
		defer w.Close()
	}

	tr := newTransport(myID, *raftPort, peerAddrs)

	electionTick, heartbeatTick := ticksFor(*electionMS), ticksFor(*heartbeatMS)
	if electionTick <= heartbeatTick {
		log.Fatalf("election timeout (%d ticks) must exceed heartbeat timeout (%d ticks)", electionTick, heartbeatTick)
	}
	cfg := &raft.Config{
		ID:                 myID,
		ElectionTick:       electionTick,
		HeartbeatTick:      heartbeatTick,
		Storage:            ms,
		MaxSizePerMsg:      4096, // library README example values; the library
		MaxInflightMsgs:    256,  // provides no defaults
		PreVote:            true, // HashiCorp Raft enables pre-vote by default
		AsyncStorageWrites: true, // decouple storage writes from the Ready loop
	}
	node := raft.StartNode(cfg, peers)

	rep := &Replica{
		myIdx: replicaID,
		myID:  myID,
		node:  node,
		ms:    ms,
		tr:    tr,
		pend:  newPending(replicaID),
	}
	rep.lead.Store(-1)
	rep.st = state.InitState()

	tr.step = node.Step
	if err := tr.listen(); err != nil {
		log.Fatalf("peer transport listener: %v", err)
	}
	log.Printf("peer transport on :%d, %d peers", *raftPort, len(peers))

	go tr.sender(myID)
	for id := range peerAddrs {
		if id != myID {
			go tr.sender(id)
		}
	}
	go rep.run(w, node)

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

func itoa(i int) string { return fmt.Sprintf("%d", i) }
