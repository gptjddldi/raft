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

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(100)

	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}
	sleepMs(400)
	h.CheckNoLeader()

	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	h.TestSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	originId, _ := h.TestSingleLeader()
	h.DisconnectPeer(originId)

	sleepMs(400)
	newId, newTerm := h.TestSingleLeader()
	h.ReconnectPeer(originId)
	sleepMs(150)
	newId2, newTerm2 := h.TestSingleLeader()

	if newId != newId2 {
		t.Errorf("again leader id got %d; want %d", newId2, newId)
	}

	if newTerm != newTerm2 {
		t.Errorf("again leader Term got %d; want %d", newTerm2, newTerm)
	}
}
