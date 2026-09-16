package main

import (
	"bytes"
	"testing"
	"time"

	"conslab/internal/proto"
	"state"

	"github.com/hashicorp/raft"
)

// applyCmd marshals a command and applies it to the FSM, mirroring what the
// adapter does with a client request.
func applyCmd(t *testing.T, f *fsm, cmd state.Command) state.Value {
	t.Helper()
	var buf bytes.Buffer
	cmd.Marshal(&buf)
	v := f.Apply(&raft.Log{Data: buf.Bytes()})
	val, ok := v.(state.Value)
	if !ok {
		t.Fatalf("Apply returned %T, want state.Value", v)
	}
	return val
}

// TestFSMPutGet verifies the application state machine implements the
// benchmark's PUT/GET semantics.
func TestFSMPutGet(t *testing.T) {
	f := newFSM()

	if got := applyCmd(t, f, state.Command{Op: state.PUT, K: 42, V: 7}); got != state.Value(7) {
		t.Fatalf("PUT returned %d, want 7", got)
	}
	if got := applyCmd(t, f, state.Command{Op: state.GET, K: 42}); got != state.Value(7) {
		t.Fatalf("GET returned %d, want 7", got)
	}
	if got := applyCmd(t, f, state.Command{Op: state.GET, K: 99}); got != state.NIL {
		t.Fatalf("GET of missing key returned %d, want NIL", got)
	}
}

// TestFSMOverwrite verifies that a PUT replaces a previous value.
func TestFSMOverwrite(t *testing.T) {
	f := newFSM()
	applyCmd(t, f, state.Command{Op: state.PUT, K: 1, V: 10})
	applyCmd(t, f, state.Command{Op: state.PUT, K: 1, V: 20})
	if got := applyCmd(t, f, state.Command{Op: state.GET, K: 1}); got != state.Value(20) {
		t.Fatalf("GET after overwrite returned %d, want 20", got)
	}
}

// TestFSMApplyTolerantOfBadInput verifies a malformed command does not panic
// and returns NIL, so a corrupt log entry cannot crash the adapter.
func TestFSMApplyTolerantOfBadInput(t *testing.T) {
	f := newFSM()
	v := f.Apply(&raft.Log{Data: []byte{}})
	if v != state.NIL {
		t.Fatalf("malformed log returned %v, want NIL", v)
	}
}

// TestFSMSnapshotRestoreRoundTrip verifies that a snapshot is complete: a
// fresh FSM restored from it must hold the same state.
func TestFSMSnapshotRestoreRoundTrip(t *testing.T) {
	src := newFSM()
	for k := int64(0); k < 50; k++ {
		applyCmd(t, src, state.Command{Op: state.PUT, K: state.Key(k), V: state.Value(k * 3)})
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&nopSink{&buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()

	dst := newFSM()
	if err := dst.Restore(&nopReadCloser{&buf}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for k := int64(0); k < 50; k++ {
		if got := applyCmd(t, dst, state.Command{Op: state.GET, K: state.Key(k)}); got != state.Value(k*3) {
			t.Fatalf("after restore, key %d = %d, want %d", k, got, k*3)
		}
	}
}

func TestFSMSnapshotEmptyState(t *testing.T) {
	f := newFSM()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot of empty FSM: %v", err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&nopSink{&buf}); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	dst := newFSM()
	if err := dst.Restore(&nopReadCloser{&buf}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := applyCmd(t, dst, state.Command{Op: state.GET, K: 1}); got != state.NIL {
		t.Fatalf("restored empty FSM returned %d for missing key, want NIL", got)
	}
}

// nopSink is a raft.SnapshotSink writing to a buffer; Cancel is a no-op.
type nopSink struct{ *bytes.Buffer }

func (s *nopSink) ID() string    { return "test" }
func (s *nopSink) Cancel() error { return nil }
func (s *nopSink) Close() error  { return nil }

type nopReadCloser struct{ *bytes.Buffer }

func (r *nopReadCloser) Close() error { return nil }

// ---- election-failure injection ----

// fakeTransport counts RequestVote calls so the isolation wrapper can be
// verified without a real network.
type fakeTransport struct {
	raft.Transport // embedded; unused methods are nil
	voteCalls      int
}

func (f *fakeTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	f.voteCalls++
	return nil
}

// TestIsolatingTransportPassesWhenClear verifies votes flow through when no
// isolation is active.
func TestIsolatingTransportPassesWhenClear(t *testing.T) {
	base := &fakeTransport{}
	iso := &isolatingTransport{Transport: base}
	args := &raft.RequestVoteRequest{}
	resp := &raft.RequestVoteResponse{}
	if err := iso.RequestVote("a", "b", args, resp); err != nil {
		t.Fatalf("clear transport returned error: %v", err)
	}
	if base.voteCalls != 1 {
		t.Fatalf("base transport called %d times, want 1", base.voteCalls)
	}
}

// TestIsolatingTransportDropsWhileIsolated verifies the injection actually
// blocks vote requests, which is what causes an election attempt to fail.
func TestIsolatingTransportDropsWhileIsolated(t *testing.T) {
	base := &fakeTransport{}
	iso := &isolatingTransport{Transport: base}
	iso.isolateFor(50 * time.Millisecond)

	args := &raft.RequestVoteRequest{}
	resp := &raft.RequestVoteResponse{}
	if err := iso.RequestVote("a", "b", args, resp); err == nil {
		t.Fatal("isolated transport should drop the vote request")
	}
	if base.voteCalls != 0 {
		t.Fatalf("base transport should not be called while isolated, got %d", base.voteCalls)
	}
}

// TestIsolatingTransportExpires verifies the isolation clears itself, which
// is how the experiment lets a later election succeed.
func TestIsolatingTransportExpires(t *testing.T) {
	base := &fakeTransport{}
	iso := &isolatingTransport{Transport: base}
	iso.isolateFor(20 * time.Millisecond)
	if !iso.isolated() {
		t.Fatal("transport should be isolated immediately after isolateFor")
	}
	time.Sleep(40 * time.Millisecond)
	if iso.isolated() {
		t.Fatal("isolation should have expired")
	}
	args := &raft.RequestVoteRequest{}
	resp := &raft.RequestVoteResponse{}
	if err := iso.RequestVote("a", "b", args, resp); err != nil {
		t.Fatalf("transport after expiry returned error: %v", err)
	}
	if base.voteCalls != 1 {
		t.Fatalf("base transport called %d times after expiry, want 1", base.voteCalls)
	}
}

// TestIsolatingTransportClear verifies the explicit clear path (DurationMS=0).
func TestIsolatingTransportClear(t *testing.T) {
	base := &fakeTransport{}
	iso := &isolatingTransport{Transport: base}
	iso.isolateFor(time.Hour)
	iso.clear()
	if iso.isolated() {
		t.Fatal("clear should remove the isolation")
	}
}

// TestReplicaIsolateElectionsRPC verifies the RPC handler routes a positive
// duration to isolation and zero to clearing.
func TestReplicaIsolateElectionsRPC(t *testing.T) {
	base := &fakeTransport{}
	rep := &Replica{iso: &isolatingTransport{Transport: base}}

	var reply proto.IsolateReply
	if err := rep.IsolateElections(&proto.IsolateArgs{DurationMS: 1000}, &reply); err != nil {
		t.Fatalf("IsolateElections: %v", err)
	}
	if !reply.OK {
		t.Fatal("IsolateElections should report OK")
	}
	if !rep.iso.isolated() {
		t.Fatal("replica should be isolated after positive duration")
	}

	if err := rep.IsolateElections(&proto.IsolateArgs{DurationMS: 0}, &reply); err != nil {
		t.Fatalf("IsolateElections(clear): %v", err)
	}
	if rep.iso.isolated() {
		t.Fatal("duration 0 should clear the isolation")
	}
}

// TestReplicaIsolateElectionsWithoutTransport verifies a nil transport is
// reported as not-OK rather than panicking.
func TestReplicaIsolateElectionsWithoutTransport(t *testing.T) {
	rep := &Replica{}
	var reply proto.IsolateReply
	if err := rep.IsolateElections(&proto.IsolateArgs{DurationMS: 1000}, &reply); err != nil {
		t.Fatalf("IsolateElections: %v", err)
	}
	if reply.OK {
		t.Fatal("nil transport should report OK=false")
	}
}

// TestReplicaStatsIsZero verifies Raft reports zero fast/slow path counters,
// so the runner can query both protocols uniformly.
func TestReplicaStatsIsZero(t *testing.T) {
	rep := &Replica{}
	var reply proto.StatsReply
	if err := rep.Stats(&proto.StatsArgs{}, &reply); err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if reply.FastPath != 0 || reply.SlowPath != 0 || reply.Conflicted != 0 {
		t.Fatalf("raft stats should be zero, got %+v", reply)
	}
}
