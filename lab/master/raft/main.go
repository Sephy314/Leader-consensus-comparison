// Command raftmaster is the lab's coordination service for Raft runs.
//
// It mirrors the upstream EPaxos master's RPC surface (Master.Register,
// Master.GetReplicaList, Master.GetLeader) so the common benchmark client is
// identical for both protocols. The raft master does NOT elect or select the
// Raft leader: it queries each Raft adapter for the leader chosen by
// HashiCorp Raft (raft.Raft.Leader) and reports the majority answer.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"conslab/internal/proto"
)

var (
	portnum  = flag.Int("port", 7087, "port to listen on")
	numNodes = flag.Int("n", 3, "number of replicas")
)

type Master struct {
	N        int
	nodeList []string
	addrList []string
	portList []int
	lock     *sync.Mutex
	nodes    []*rpc.Client
	alive    []bool

	leaderCache   int
	leaderCacheAt time.Time
}

func main() {
	flag.Parse()
	log.Printf("raft master starting on port %d, waiting for %d replicas", *portnum, *numNodes)

	master := &Master{
		N:             *numNodes,
		nodeList:      make([]string, 0, *numNodes),
		addrList:      make([]string, 0, *numNodes),
		portList:      make([]int, 0, *numNodes),
		lock:          new(sync.Mutex),
		nodes:         make([]*rpc.Client, *numNodes),
		alive:         make([]bool, *numNodes),
		leaderCache:   -1,
		leaderCacheAt: time.Time{},
	}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("master listen error:", err)
	}
	go master.run()
	http.Serve(l, nil)
}

// callTimeout runs an RPC call with a deadline. net/rpc.Call blocks forever
// on a dead connection, which would wedge leader reporting after a replica
// failure.
func callTimeout(cli *rpc.Client, method string, args, reply any, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- cli.Call(method, args, reply) }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("rpc %s timed out after %v", method, timeout)
	}
}

// run waits for all replicas to register, then maintains RPC connections to
// their admin ports (clientPort+1000) and pings them for liveness.
func (master *Master) run() {
	for {
		master.lock.Lock()
		ready := len(master.nodeList) == master.N
		master.lock.Unlock()
		if ready {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < master.N; i++ {
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		cli, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Printf("error connecting to replica %d at %s: %v", i, addr, err)
			continue
		}
		master.lock.Lock()
		master.nodes[i] = cli
		master.lock.Unlock()
	}

	for {
		time.Sleep(time.Second)
		master.lock.Lock()
		nodes := append([]*rpc.Client(nil), master.nodes...)
		master.lock.Unlock()
		for i, node := range nodes {
			if node == nil {
				continue
			}
			err := callTimeout(node, "Replica.Ping", new(proto.PingArgs), new(proto.PingReply), 2*time.Second)
			master.lock.Lock()
			master.alive[i] = err == nil
			master.lock.Unlock()
		}
	}
}

// Register is idempotent by address:port (same semantics as the upstream
// EPaxos master).
func (master *Master) Register(args *proto.RegisterArgs, reply *proto.RegisterReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	nlen := len(master.nodeList)
	index := nlen
	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)
	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}
	if index == nlen {
		master.nodeList = append(master.nodeList, addrPort)
		master.addrList = append(master.addrList, args.Addr)
		master.portList = append(master.portList, args.Port)
	}
	if len(master.nodeList) == master.N {
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
	} else {
		reply.Ready = false
	}
	return nil
}

// GetLeader reports the leader chosen by HashiCorp Raft. The answer is the
// replica index (into NodeList) reported by a majority of reachable adapters,
// or -1 while no leader is elected. Results are cached for 500ms, but a
// cached leader that is no longer alive is never returned.
func (master *Master) GetLeader(args *proto.GetLeaderArgs, reply *proto.GetLeaderReply) error {
	master.lock.Lock()
	if time.Since(master.leaderCacheAt) < 500*time.Millisecond && master.leaderCache >= 0 {
		if master.leaderCache < len(master.alive) && master.alive[master.leaderCache] {
			reply.LeaderId = master.leaderCache
			master.lock.Unlock()
			return nil
		}
	}
	nodes := append([]*rpc.Client(nil), master.nodes...)
	master.lock.Unlock()

	counts := map[int]int{}
	for i, node := range nodes {
		if node == nil {
			continue
		}
		master.lock.Lock()
		alive := master.alive[i]
		master.lock.Unlock()
		if !alive {
			// Skip replicas known to be dead: their RPC would block for the
			// full timeout, making GetLeader too slow for the client to
			// discover the new leader promptly.
			continue
		}
		var lr proto.LeaderIdReply
		if err := callTimeout(node, "Replica.LeaderId", new(proto.LeaderIdArgs), &lr, 2*time.Second); err == nil && lr.LeaderId >= 0 {
			counts[lr.LeaderId]++
		}
	}
	best := -1
	for id, c := range counts {
		if c > master.N/2 {
			best = id
			break
		}
	}
	master.lock.Lock()
	master.leaderCache = best
	master.leaderCacheAt = time.Now()
	master.lock.Unlock()
	reply.LeaderId = best
	return nil
}

func (master *Master) GetReplicaList(args *proto.GetReplicaListArgs, reply *proto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()
	if len(master.nodeList) == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
	} else {
		reply.Ready = false
	}
	return nil
}
