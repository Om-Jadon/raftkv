package chaos

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Om-Jadon/raftkv/internal/raft"
	"github.com/Om-Jadon/raftkv/internal/transport/memory"
	"github.com/Om-Jadon/raftkv/test/harness"
)

var chaosSeed = flag.Int64("chaos.seed", -1, "run a single chaos seed")

func seedCount(t *testing.T) (start int64, n int) {
	t.Helper()
	if *chaosSeed >= 0 {
		return *chaosSeed, 1
	}
	if s := os.Getenv("CHAOS_SEEDS"); s != "" {
		parsed, err := strconv.Atoi(s)
		if err != nil || parsed <= 0 {
			t.Fatalf("CHAOS_SEEDS=%q", s)
		}
		return 1, parsed
	}
	if testing.Short() {
		return 1, 2
	}
	return 1, 5
}

func TestChaosSeeds(t *testing.T) {
	cfg := defaultConfig()
	if os.Getenv("CHAOS_PERSIST") == "1" {
		cfg.persist = true
	}
	start, n := seedCount(t)
	t.Logf("running %d chaos seed(s) starting at %d", n, start)
	for i := 0; i < n; i++ {
		seed := start + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			if err := runSeed(t, seed, cfg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestChaosSoak(t *testing.T) {
	if os.Getenv("CHAOS_SOAK") != "1" {
		t.Skip("set CHAOS_SOAK=1 to run the 2-minute soak")
	}
	cfg := defaultConfig()
	cfg.nemesis = 2 * time.Minute
	cfg.stabilize = 5 * time.Second
	seed := time.Now().UnixNano()
	t.Logf("soak seed=%d", seed)
	if err := runSeed(t, seed, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestAsymmetricPartition(t *testing.T) {
	c := harness.NewCluster(t, 3)
	c.Network().Block(1, 2)
	_, err := c.SendRequestVote(1, 2, &raft.RequestVoteArgs{Term: 1, CandidateID: 1})
	if !errors.Is(err, memory.ErrPartitioned) {
		t.Fatalf("1->2: %v, want ErrPartitioned", err)
	}
	_, err = c.SendRequestVote(2, 1, &raft.RequestVoteArgs{Term: 1, CandidateID: 2})
	if err != nil {
		t.Fatalf("2->1 should succeed: %v", err)
	}
}
