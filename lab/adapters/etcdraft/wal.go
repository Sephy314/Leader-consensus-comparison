// Write-ahead log for the etcd/raft adapter.
//
// go.etcd.io/raft implements only the core Raft algorithm: it explicitly
// leaves storage to the integrator ("users must implement their own storage
// layer to persist the Raft log and state"). The lab therefore provides this
// minimal append-only WAL so that Raft B persists HardState and Entries, and
// fsyncs them before the corresponding Ready messages are sent --- the
// durability rule the library's Ready documentation states.
//
// This exists for comparability, not as an experiment variable: HashiCorp Raft
// (Raft A) persists through raft-boltdb and fsyncs on commit, so a Raft B that
// kept its log only in memory would be measured against a different I/O cost.
// The file is written under the same /data volume Raft A uses.
//
// ponytail: no checksums or torn-write recovery, because the benchmark never
// restarts a replica (failure injection kills a container and does not restart
// it) and every repetition starts from a fresh volume. Upgrade path if a
// restart-from-persistence experiment is ever added: add a per-record checksum
// and truncate at the first bad record during replay.
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	gproto "google.golang.org/protobuf/proto"
)

const (
	walKindHardState byte = 1
	walKindEntry     byte = 2
)

// wal is an append-only log of HardState snapshots and log entries.
type wal struct {
	f   *os.File
	off int64
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &wal{f: f, off: off}, nil
}

func (w *wal) records(hs *pb.HardState, entries []*pb.Entry) ([][]byte, error) {
	var recs [][]byte
	if hs != nil && !raft.IsEmptyHardState(hs) {
		body, err := gproto.Marshal(hs)
		if err != nil {
			return nil, err
		}
		recs = append(recs, frame(walKindHardState, body))
	}
	for _, e := range entries {
		body, err := gproto.Marshal(e)
		if err != nil {
			return nil, err
		}
		recs = append(recs, frame(walKindEntry, body))
	}
	return recs, nil
}

func frame(kind byte, body []byte) []byte {
	buf := make([]byte, 4+1+len(body))
	binary.BigEndian.PutUint32(buf[:4], uint32(1+len(body)))
	buf[4] = kind
	copy(buf[5:], body)
	return buf
}

// append durably writes HardState and Entries. sync mirrors the library's
// Ready.MustSync: when false the write is buffered by the OS but not forced to
// stable storage.
func (w *wal) append(hs *pb.HardState, entries []*pb.Entry, sync bool) error {
	recs, err := w.records(hs, entries)
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		return nil
	}
	var buf []byte
	for _, r := range recs {
		buf = append(buf, r...)
	}
	if _, err := w.f.WriteAt(buf, w.off); err != nil {
		return err
	}
	w.off += int64(len(buf))
	if sync {
		return w.f.Sync()
	}
	return nil
}

func (w *wal) Close() error { return w.f.Close() }

// loadWAL replays a WAL file into a fresh MemoryStorage. It is called once at
// startup, before the Raft node is created, so a replica that is restarted
// from its volume resumes with the state it had persisted. A missing or empty
// file is a fresh replica.
func loadWAL(path string, ms *raft.MemoryStorage) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	var pending []*pb.Entry
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		err := ms.Append(pending)
		pending = nil
		return err
	}

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return flush()
			}
			return err
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		if n == 0 {
			return fmt.Errorf("wal: zero-length record")
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(f, body); err != nil {
			return err
		}
		switch body[0] {
		case walKindHardState:
			var hs pb.HardState
			if err := gproto.Unmarshal(body[1:], &hs); err != nil {
				return err
			}
			if err := flush(); err != nil {
				return err
			}
			if err := ms.SetHardState(&hs); err != nil {
				return err
			}
		case walKindEntry:
			var e pb.Entry
			if err := gproto.Unmarshal(body[1:], &e); err != nil {
				return err
			}
			pending = append(pending, &e)
		default:
			return fmt.Errorf("wal: unknown record kind %d", body[0])
		}
	}
}
