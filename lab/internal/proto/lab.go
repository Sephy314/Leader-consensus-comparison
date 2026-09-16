package proto

// This file contains lab-specific additions to the wire protocol that are
// NOT part of the upstream EPaxos/genericsmr protocol. They are kept in a
// separate file so that proto.go, marsh.go, and master.go remain verbatim
// copies of the pinned upstream sources (verified by `make check-upstream`).

// LeaderIdArgs/LeaderIdReply let the lab's raft master query a Raft adapter
// for the leader chosen by HashiCorp Raft itself (raft.Raft.Leader). The
// raft master never elects or selects a leader; it only reports what
// HashiCorp Raft decided.
type LeaderIdArgs struct {
}

type LeaderIdReply struct {
	LeaderId int
}

// StatsArgs/StatsReply let the lab's runner query a replica's cumulative
// protocol counters. For EPaxos these are the fast-path (no-conflict) and
// slow-path (conflict-resolved) execution counts, which the upstream
// implementation already tracks; the lab only makes them cumulative and
// reachable over RPC. For Raft the counters are zero (Raft has no fast/slow
// path distinction).
type StatsArgs struct {
}

type StatsReply struct {
	// FastPath is the cumulative number of commands that completed on the
	// protocol's fast path (EPaxos: committed in one round trip).
	FastPath int64
	// SlowPath is the cumulative number of commands that required the slow
	// path (EPaxos: an extra Accept round).
	SlowPath int64
	// Conflicted is the cumulative number of conflicts observed
	// (EPaxos: non-equal replies plus dependency-set mismatches).
	Conflicted int64
}

// IsolateArgs/IsolateReply control the lab's election-failure injection for
// Raft. The runner asks every non-leader replica to isolate its Raft
// transport (drop RequestVote/RequestPreVote) for a duration, causing the
// next election attempt(s) to fail. HashiCorp Raft's election algorithm is
// not modified; the injection only makes the transport unreachable for
// votes, which is a fault-injection hook around the existing mechanism.
type IsolateArgs struct {
	// DurationMS is how long the transport stays isolated (0 = clear).
	DurationMS int64
}

type IsolateReply struct {
	OK bool
}
