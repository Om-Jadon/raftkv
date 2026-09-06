package transport_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
	grpctransport "github.com/Om-Jadon/raftkv/internal/transport/grpc"
	"github.com/Om-Jadon/raftkv/internal/transport/memory"
)

type stubHandler struct {
	mu   sync.Mutex
	rv   int
	ae   int
	snap int
	last *raft.RequestVoteArgs
}

func (s *stubHandler) RequestVote(_ context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rv++
	cp := *args
	s.last = &cp
	return &raft.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (s *stubHandler) AppendEntries(_ context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	s.mu.Lock()
	s.ae++
	n := len(args.Entries)
	s.mu.Unlock()
	return &raft.AppendEntriesReply{Term: args.Term, Success: true, ConflictIndex: uint64(n)}, nil
}

func (s *stubHandler) InstallSnapshot(_ context.Context, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	s.mu.Lock()
	s.snap++
	s.mu.Unlock()
	return &raft.InstallSnapshotReply{Term: args.Term}, nil
}

func (s *stubHandler) counts() (rv, ae, snap int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rv, s.ae, s.snap
}

func TestMemoryTransportRoundTrip(t *testing.T) {
	h1, h2 := &stubHandler{}, &stubHandler{}
	net := memory.New(1)
	net.Register(1, h1)
	net.Register(2, h2)
	t.Cleanup(net.Close)
	runRoundTrip(t, net.Transport(1), net.Transport(2), h2)
}

func TestGRPCTransportRoundTrip(t *testing.T) {
	h1, h2 := &stubHandler{}, &stubHandler{}
	tr1 := grpctransport.New(1, nil)
	tr2 := grpctransport.New(2, nil)
	a1, err := tr1.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a2, err := tr2.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tr1.SetPeer(2, a2)
	tr2.SetPeer(1, a1)
	tr1.SetHandler(h1)
	tr2.SetHandler(h2)
	t.Cleanup(func() { _ = tr1.Close(); _ = tr2.Close() })
	runRoundTrip(t, tr1, tr2, h2)
}

func runRoundTrip(t *testing.T, from, _ raft.Transport, dest *stubHandler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	vr, err := from.SendRequestVote(ctx, 2, &raft.RequestVoteArgs{
		Term: 3, CandidateID: 1, LastLogIndex: 7, LastLogTerm: 2,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !vr.VoteGranted || vr.Term != 3 {
		t.Fatalf("vote reply = %+v", vr)
	}

	ar, err := from.SendAppendEntries(ctx, 2, &raft.AppendEntriesArgs{
		Term: 3, LeaderID: 1, Entries: []raft.LogEntry{{Term: 3, Index: 1, Command: []byte("x")}},
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !ar.Success {
		t.Fatalf("append reply = %+v", ar)
	}

	sr, err := from.SendInstallSnapshot(ctx, 2, &raft.InstallSnapshotArgs{
		Term: 3, LeaderID: 1, LastIncludedIndex: 1, LastIncludedTerm: 3, Data: []byte("snap"), Done: true,
	})
	if err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if sr.Term != 3 {
		t.Fatalf("snap reply = %+v", sr)
	}

	rv, ae, snap := dest.counts()
	if rv != 1 || ae != 1 || snap != 1 {
		t.Fatalf("counts rv=%d ae=%d snap=%d", rv, ae, snap)
	}
}

func TestGRPCLazyDial(t *testing.T) {
	h := &stubHandler{}
	client := grpctransport.New(1, nil)
	server := grpctransport.New(2, nil)
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	client.SetPeer(2, "127.0.0.1:1") // nothing listening; must error, not panic
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := client.SendRequestVote(ctx, 2, &raft.RequestVoteArgs{Term: 1, CandidateID: 1})
	if err == nil {
		t.Fatal("expected dial/rpc error to down peer")
	}

	addr, err := server.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server.SetHandler(h)
	client.SetPeer(2, addr)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	reply, err := client.SendRequestVote(ctx2, 2, &raft.RequestVoteArgs{Term: 4, CandidateID: 1})
	if err != nil {
		t.Fatalf("after peer came up: %v", err)
	}
	if reply.Term != 4 || !reply.VoteGranted {
		t.Fatalf("reply = %+v", reply)
	}
}
