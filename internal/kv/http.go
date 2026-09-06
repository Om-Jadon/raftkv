package kv

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

const (
	headerLeader   = "X-Raft-Leader"
	headerClientID = "X-Raft-Client-ID"
	headerSeq      = "X-Raft-Seq"
	headerNode     = "X-Raft-Node"
)

// Server serves the KV HTTP API on top of a Raft node.
type Server struct {
	node    atomic.Pointer[raft.Node]
	timeout time.Duration

	addrs atomic.Value // map[raft.NodeID]string

	// DropNextReply, if true, discards the next successful reply after apply.
	// Tests use this to simulate a crash between apply and client response.
	DropNextReply atomic.Bool
}

// NewServer wraps node. Call SetAddrs after HTTP listen addresses are known.
func NewServer(node *raft.Node) *Server {
	s := &Server{timeout: 2 * time.Second}
	s.node.Store(node)
	s.addrs.Store(map[raft.NodeID]string{})
	return s
}

// SetNode replaces the Raft node (used after a crash/restart on the same HTTP port).
func (s *Server) SetNode(node *raft.Node) {
	s.node.Store(node)
}

func (s *Server) raft() *raft.Node {
	return s.node.Load()
}

// SetAddrs installs node-id → base URL mapping used for X-Raft-Leader.
func (s *Server) SetAddrs(addrs map[raft.NodeID]string) {
	cp := make(map[raft.NodeID]string, len(addrs))
	for k, v := range addrs {
		cp[k] = v
	}
	s.addrs.Store(cp)
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("POST /kv/{key}", s.handleAppend)
	mux.HandleFunc("GET /status", s.handleStatus)
	return mux
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	cmd := Command{Op: OpGet, Key: r.PathValue("key")}
	res, err := s.submit(w, r, cmd)
	if err != nil {
		return
	}
	if !res.Found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.WriteString(w, res.Value)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cmd := Command{Op: OpPut, Key: r.PathValue("key"), Value: string(body)}
	if _, err := s.submit(w, r, cmd); err != nil {
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	cmd := Command{Op: OpDelete, Key: r.PathValue("key")}
	if _, err := s.submit(w, r, cmd); err != nil {
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cmd := Command{Op: OpAppend, Key: r.PathValue("key"), Value: string(body)}
	res, err := s.submit(w, r, cmd)
	if err != nil {
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.WriteString(w, res.Value)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.raft().Status()
	addrs := s.addrs.Load().(map[raft.NodeID]string)
	out := map[string]any{
		"id":          st.ID,
		"role":        st.Role.String(),
		"term":        st.Term,
		"commitIndex": st.CommitIndex,
		"lastApplied": st.LastApplied,
		"logSize":     st.LogSize,
		"leader":      st.Leader,
		"leaderAddr":  addrs[st.Leader],
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request, cmd Command) (Result, error) {
	cmd.ClientID = parseUint(r.Header.Get(headerClientID))
	cmd.SeqNum = parseUint(r.Header.Get(headerSeq))

	index, term, wait, ok := s.raft().Propose(encode(cmd))
	if !ok {
		s.writeNotLeader(w)
		return Result{}, raft.ErrNotLeader
	}
	res, err := raft.WaitApply(r.Context(), wait, s.timeout)
	if err != nil {
		s.raft().CancelWait(index, term)
		if errors.Is(err, raft.ErrLostLeadership) || errors.Is(err, raft.ErrNotLeader) {
			s.writeNotLeader(w)
			return Result{}, err
		}
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return Result{}, err
	}
	if s.DropNextReply.CompareAndSwap(true, false) {
		panic(http.ErrAbortHandler)
	}
	out, _ := res.Result.(Result)
	w.Header().Set(headerNode, strconv.FormatUint(uint64(s.raft().ID()), 10))
	return out, nil
}

func (s *Server) writeNotLeader(w http.ResponseWriter) {
	addrs := s.addrs.Load().(map[raft.NodeID]string)
	if addr, ok := addrs[s.raft().LeaderID()]; ok && addr != "" {
		w.Header().Set(headerLeader, addr)
	}
	w.Header().Set(headerNode, strconv.FormatUint(uint64(s.raft().ID()), 10))
	http.Error(w, "not leader", http.StatusServiceUnavailable)
}

func parseUint(s string) uint64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}
