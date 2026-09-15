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
