package raft

import "testing"

func TestStaleCandidateLosesVote(t *testing.T) {
	// Voter has term-2 at index 5. Candidate only replicated term-1.
	if LogUpToDate(1, 99, 2, 5) {
		t.Fatal("longer stale term must not count as up-to-date")
	}
}

func TestSameTermLongerLogWins(t *testing.T) {
	if !LogUpToDate(3, 10, 3, 7) {
		t.Fatal("same term, longer log must win")
	}
	if LogUpToDate(3, 6, 3, 7) {
		t.Fatal("same term, shorter log must lose")
	}
}

func TestEqualLogGrants(t *testing.T) {
	if !LogUpToDate(4, 2, 4, 2) {
		t.Fatal("equal last term/index is up-to-date")
	}
}

func TestEmptyVoterLogAlwaysGrants(t *testing.T) {
	if !LogUpToDate(0, 0, 0, 0) {
		t.Fatal("empty-empty is up-to-date")
	}
	if !LogUpToDate(1, 1, 0, 0) {
		t.Fatal("any candidate log beats empty voter")
	}
}
