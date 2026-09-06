package kv

import (
	"testing"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

func apply(k *KV, index uint64, cmd Command) Result {
	r, err := k.Apply(raft.LogEntry{Index: index, Term: 1, Command: encode(cmd)})
	if err != nil {
		panic(err)
	}
	return r.(Result)
}

func TestSessionDedup(t *testing.T) {
	k := New()
	cmd := Command{Op: OpAppend, Key: "k", Value: "x", ClientID: 1, SeqNum: 1}
	r1 := apply(k, 1, cmd)
	r2 := apply(k, 2, cmd)
	if r1.Value != "x" || r2.Value != "x" {
		t.Fatalf("dedup failed: %+v %+v", r1, r2)
	}
	if v, _ := k.GetLocal("k"); v != "x" {
		t.Fatalf("value = %q want x", v)
	}
}

func TestCAS(t *testing.T) {
	k := New()
	apply(k, 1, Command{Op: OpPut, Key: "k", Value: "a"})
	if !apply(k, 2, Command{Op: OpCAS, Key: "k", Value: "b", Expected: "a"}).OK {
		t.Fatal("cas should succeed")
	}
	if apply(k, 3, Command{Op: OpCAS, Key: "k", Value: "c", Expected: "a"}).OK {
		t.Fatal("cas should fail")
	}
	if v, _ := k.GetLocal("k"); v != "b" {
		t.Fatalf("got %q", v)
	}
}

func TestSnapshotRestore(t *testing.T) {
	k := New()
	apply(k, 1, Command{Op: OpPut, Key: "a", Value: "1", ClientID: 9, SeqNum: 1})
	snap, err := k.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	k2 := New()
	if err := k2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if v, ok := k2.GetLocal("a"); !ok || v != "1" {
		t.Fatalf("restore data %q %v", v, ok)
	}
	dup := apply(k2, 2, Command{Op: OpPut, Key: "a", Value: "2", ClientID: 9, SeqNum: 1})
	if dup.Value != "1" {
		t.Fatalf("session not restored: %+v", dup)
	}
}
