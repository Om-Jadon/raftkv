package grpctransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/internal/transport/grpc/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const maxMsgSize = 16 << 20

// ErrClosed is returned after Transport.Close.
var ErrClosed = errors.New("grpc transport: closed")

// Transport is a raft.Transport over gRPC. Dials are lazy: a missing peer
// returns an RPC error instead of crashing the process.
type Transport struct {
	id      raft.NodeID
	timeout time.Duration

	mu      sync.Mutex
	peers   map[raft.NodeID]string
	handler raft.RPCHandler
	server  *grpc.Server
	lis     net.Listener
	conns   map[raft.NodeID]*grpc.ClientConn
	closed  bool
}

// New returns a transport for local id. Peer addresses may be filled later via SetPeer.
func New(id raft.NodeID, peers map[raft.NodeID]string) *Transport {
	cp := make(map[raft.NodeID]string, len(peers))
	for k, v := range peers {
		cp[k] = v
	}
	return &Transport{
		id:      id,
		timeout: 200 * time.Millisecond,
		peers:   cp,
		conns:   make(map[raft.NodeID]*grpc.ClientConn),
	}
}

// SetHandler installs the Raft RPC receiver (typically *raft.Node).
func (t *Transport) SetHandler(h raft.RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = h
}

// SetPeer sets or replaces the dial address for a peer.
func (t *Transport) SetPeer(id raft.NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addr
	if conn, ok := t.conns[id]; ok {
		delete(t.conns, id)
		go conn.Close()
	}
}

// Listen starts the gRPC server on addr (host:port, port may be 0) and returns the bound address.
func (t *Transport) Listen(addr string) (string, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)
	raftpb.RegisterRaftServer(srv, &rpcServer{t: t})

	t.mu.Lock()
	t.lis = lis
	t.server = srv
	t.mu.Unlock()

	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), nil
}

// Addr returns the bound listen address, or empty if not listening.
func (t *Transport) Addr() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lis == nil {
		return ""
	}
	return t.lis.Addr().String()
}

// Close stops the server and all client connections. It is idempotent.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	srv := t.server
	conns := t.conns
	t.conns = make(map[raft.NodeID]*grpc.ClientConn)
	t.mu.Unlock()

	if srv != nil {
		srv.GracefulStop()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	return nil
}

func (t *Transport) SendRequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	cli, err := t.client(to)
	if err != nil {
		return nil, err
	}
	ctx, cancel := t.rpcCtx(ctx)
	defer cancel()
	resp, err := cli.RequestVote(ctx, raftpb.FromVoteArgs(args))
	if err != nil {
		return nil, err
	}
	return raftpb.ToVoteReply(resp), nil
}

func (t *Transport) SendAppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	cli, err := t.client(to)
	if err != nil {
		return nil, err
	}
	ctx, cancel := t.rpcCtx(ctx)
	defer cancel()
	resp, err := cli.AppendEntries(ctx, raftpb.FromAppendArgs(args))
	if err != nil {
		return nil, err
	}
	return raftpb.ToAppendReply(resp), nil
}

func (t *Transport) SendInstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	cli, err := t.client(to)
	if err != nil {
		return nil, err
	}
	ctx, cancel := t.rpcCtx(ctx)
	defer cancel()
	resp, err := cli.InstallSnapshot(ctx, raftpb.FromSnapArgs(args))
	if err != nil {
		return nil, err
	}
	return raftpb.ToSnapReply(resp), nil
}

func (t *Transport) rpcCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, t.timeout)
}

func (t *Transport) client(to raft.NodeID) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	if conn, ok := t.conns[to]; ok {
		return raftpb.NewRaftClient(conn), nil
	}
	addr, ok := t.peers[to]
	if !ok || addr == "" {
		return nil, fmt.Errorf("grpc transport: unknown peer %d", to)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
	)
	if err != nil {
		return nil, err
	}
	t.conns[to] = conn
	return raftpb.NewRaftClient(conn), nil
}

type rpcServer struct {
	raftpb.UnimplementedRaftServer
	t *Transport
}

func (s *rpcServer) handler() (raft.RPCHandler, error) {
	s.t.mu.Lock()
	defer s.t.mu.Unlock()
	if s.t.closed {
		return nil, status.Error(codes.Unavailable, "closed")
	}
	if s.t.handler == nil {
		return nil, status.Error(codes.Unavailable, "handler not ready")
	}
	return s.t.handler, nil
}

func (s *rpcServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	h, err := s.handler()
	if err != nil {
		return nil, err
	}
	reply, err := h.RequestVote(ctx, raftpb.ToVoteArgs(req))
	if err != nil {
		return nil, err
	}
	return raftpb.FromVoteReply(reply), nil
}

func (s *rpcServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	h, err := s.handler()
	if err != nil {
		return nil, err
	}
	reply, err := h.AppendEntries(ctx, raftpb.ToAppendArgs(req))
	if err != nil {
		return nil, err
	}
	return raftpb.FromAppendReply(reply), nil
}

func (s *rpcServer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	h, err := s.handler()
	if err != nil {
		return nil, err
	}
	reply, err := h.InstallSnapshot(ctx, raftpb.ToSnapArgs(req))
	if err != nil {
		return nil, err
	}
	return raftpb.FromSnapReply(reply), nil
}
