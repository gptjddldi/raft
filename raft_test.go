package main

import "testing"

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	h.TestSingleLeader()
}

func TestElectionLeaderDisconnection(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	originId, originTerm := h.TestSingleLeader()

	h.DisconnectPeer(originId)

	sleepMs(350)

	newId, newTerm := h.TestSingleLeader()
	if newId == originId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= originTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, originTerm)
	}
}
