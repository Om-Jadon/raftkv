package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Om-Jadon/raftkv/internal/kv"
	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/internal/storage"
	grpctransport "github.com/Om-Jadon/raftkv/internal/transport/grpc"
)

func main() {
	id := flag.Uint64("id", 1, "this node's Raft ID")
	peers := flag.String("peers", "", "comma-separated id@host:raftPort (include self)")
	raftAddr := flag.String("raft-addr", "", "gRPC listen address (default: this id's peer addr)")
	httpAddr := flag.String("http-addr", "", "HTTP listen address")
	httpPeers := flag.String("http-peers", "", "comma-separated id@host:httpPort (include self)")
	dataDir := flag.String("data-dir", "", "durable storage directory")
	snapBytes := flag.Uint64("snapshot-bytes", 64<<10, "compact the Raft log when it exceeds this many bytes (0 disables)")
	flag.Parse()

	if *dataDir == "" {
		log.Fatal("--data-dir is required")
	}
	spec, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("--peers: %v", err)
	}
	self := raft.NodeID(*id)
	raftMap, peerIDs, selfRaft, err := splitPeers(spec, self)
	if err != nil {
		log.Fatal(err)
	}
	if *raftAddr == "" {
		*raftAddr = selfRaft
	}
	httpURLs, selfHTTP, err := parseHTTPPeers(*httpPeers, self)
	if err != nil {
		log.Fatalf("--http-peers: %v", err)
	}
	if *httpAddr == "" {
		*httpAddr = selfHTTP
	}
	if *httpAddr == "" {
		log.Fatal("--http-addr is required (or include self in --http-peers)")
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}
	st, err := storage.Open(*dataDir)
	if err != nil {
		log.Fatal(err)
	}

	tr := grpctransport.New(self, raftMap)
	bound, err := tr.Listen(*raftAddr)
	if err != nil {
		log.Fatalf("raft listen %s: %v", *raftAddr, err)
	}

	store := kv.New()
	cfg := raft.WANConfig(self, peerIDs)
	cfg.DataDir = *dataDir
	cfg.SnapshotThresholdBytes = *snapBytes
	node := raft.New(cfg, tr, raft.RealClock{}, st, store)
	tr.SetHandler(node)
	node.Run()

	api := kv.NewServer(node)
	if len(httpURLs) > 0 {
		api.SetAddrs(httpURLs)
	}

	hs := &http.Server{Addr: *httpAddr, Handler: api.Handler()}
	go func() {
		log.Printf("node %d raft=%s http=%s", self, bound, *httpAddr)
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdownCtx)
	node.Stop()
	_ = tr.Close()
	_ = st.Close()
}

func parsePeers(s string) (map[raft.NodeID]string, error) {
	out := make(map[raft.NodeID]string)
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("empty")
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		id, addr, err := splitIDAddr(part)
		if err != nil {
			return nil, err
		}
		out[id] = addr
	}
	return out, nil
}

func splitPeers(all map[raft.NodeID]string, self raft.NodeID) (others map[raft.NodeID]string, ids []raft.NodeID, selfAddr string, err error) {
	selfAddr, ok := all[self]
	if !ok {
		return nil, nil, "", fmt.Errorf("peers do not include id %d", self)
	}
	others = make(map[raft.NodeID]string)
	for id, addr := range all {
		if id == self {
			continue
		}
		others[id] = addr
		ids = append(ids, id)
	}
	return others, ids, selfAddr, nil
}

func parseHTTPPeers(s string, self raft.NodeID) (map[raft.NodeID]string, string, error) {
	out := make(map[raft.NodeID]string)
	if strings.TrimSpace(s) == "" {
		return out, "", nil
	}
	var selfAddr string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		id, addr, err := splitIDAddr(part)
		if err != nil {
			return nil, "", err
		}
		if !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
		out[id] = addr
		if id == self {
			u := strings.TrimPrefix(addr, "http://")
			selfAddr = u
		}
	}
	return out, selfAddr, nil
}

func splitIDAddr(part string) (raft.NodeID, string, error) {
	at := strings.LastIndex(part, "@")
	if at <= 0 {
		return 0, "", fmt.Errorf("want id@host:port, got %q", part)
	}
	n, err := strconv.ParseUint(part[:at], 10, 64)
	if err != nil {
		return 0, "", err
	}
	addr := part[at+1:]
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return 0, "", fmt.Errorf("%q: %w", addr, err)
	}
	return raft.NodeID(n), addr, nil
}
