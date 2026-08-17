package raft

import "testing"

func TestSanity(t *testing.T) {
	if 1+1 != 2 {
		t.Fatal("sanity check failed")
	}
}
