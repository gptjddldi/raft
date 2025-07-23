package main

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

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

func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 200*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	tlog("submitting 42 to %d", origLeaderId)

	isLeader := h.SubmitToServer(origLeaderId, 42)
	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", origLeaderId)
	}
	sleepMs(150)
	h.CheckCommittedN(42, 3)
}

func TestSubmitNonLeaderFails(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	originLeaderId, _ := h.CheckSingleLeader()
	sid := (originLeaderId + 1) % 3
	tlog("submitting 42 to %d", sid)
	isLeader := h.SubmitToServer(sid, 42)
	if isLeader {
		t.Errorf("want id=%d !leader, but it is", sid)
	}
	sleepMs(10)
}

func TestCommitMultipleCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)
	h := NewHarness(t, 3)
	defer h.Shutdown()

	originLeaderId, _ := h.CheckSingleLeader()

	values := []int{42, 55, 81}
	for _, v := range values {
		tlog("submitting %d to %d", v, originLeaderId)
		isLeader := h.SubmitToServer(originLeaderId, v)
		if !isLeader {
			t.Errorf("want id=%d leader, but is's not", originLeaderId)
		}
		sleepMs(100)
	}
	sleepMs(150)
	nc, i1 := h.CheckCommitted(42)
	_, i2 := h.CheckCommitted(55)
	if nc != 3 {
		t.Errorf("want nc=3 but got %d", nc)
	}
	if i1 >= i2 {
		t.Errorf("want i1<i2, git i1=%d, i2=%d", i1, i2)
	}
	_, i3 := h.CheckCommitted(81)
	if i2 >= i3 {
		t.Errorf("want i2<i3, git i2=%d, i3=%d", i2, i3)
	}
}

func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Submit a couple of values to a fully connected cluster.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	dPeerId := (origLeaderId + 1) % 3
	h.DisconnectPeer(dPeerId)
	sleepMs(250)

	// Submit a new command; it will be committed but only to two servers.
	h.SubmitToServer(origLeaderId, 7)
	sleepMs(250)
	h.CheckCommittedN(7, 2)

	// Now reconnect dPeerId and wait a bit; it should find the new command too.
	h.ReconnectPeer(dPeerId)
	sleepMs(200)
	h.CheckSingleLeader()

	sleepMs(150)
	h.CheckCommittedN(7, 3)
}

func TestNoCommitWithNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	// Submit a couple of values to a fully connected cluster.
	origLeaderId, origTerm := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	// Disconnect both followers.
	dPeer1 := (origLeaderId + 1) % 3
	dPeer2 := (origLeaderId + 2) % 3
	h.DisconnectPeer(dPeer1)
	h.DisconnectPeer(dPeer2)
	sleepMs(250)

	h.SubmitToServer(origLeaderId, 8)
	sleepMs(250)
	h.CheckNotCommitted(8)

	// Reconnect both other servers, we'll have quorum now.
	h.ReconnectPeer(dPeer1)
	h.ReconnectPeer(dPeer2)
	sleepMs(600)

	// 8 is still not committed because the term has changed.
	h.CheckNotCommitted(8)

	// A new leader will be elected. It could be a different leader, even though
	// the original's log is longer, because the two reconnected peers can elect
	// each other.
	newLeaderId, againTerm := h.CheckSingleLeader()
	if origTerm == againTerm {
		t.Errorf("got origTerm==againTerm==%d; want them different", origTerm)
	}

	// But new values will be committed for sure...
	h.SubmitToServer(newLeaderId, 9)
	h.SubmitToServer(newLeaderId, 10)
	h.SubmitToServer(newLeaderId, 11)
	sleepMs(350)

	for _, v := range []int{9, 10, 11} {
		h.CheckCommittedN(v, 3)
	}
}

func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	// Submit a couple of values to a fully connected cluster.
	origLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(origLeaderId, 5)
	h.SubmitToServer(origLeaderId, 6)

	sleepMs(150)
	h.CheckCommittedN(6, 5)

	// Leader disconnected...
	h.DisconnectPeer(origLeaderId)
	sleepMs(10)

	// Submit 7 to original leader, even though it's disconnected.
	h.SubmitToServer(origLeaderId, 7)

	sleepMs(150)
	h.CheckNotCommitted(7)

	newLeaderId, _ := h.CheckSingleLeader()

	// Submit 8 to new leader.
	h.SubmitToServer(newLeaderId, 8)
	sleepMs(150)
	h.CheckCommittedN(8, 4)

	// Reconnect old leader and let it settle. The old leader shouldn't be the one
	// winning.
	h.ReconnectPeer(origLeaderId)
	sleepMs(600)

	finalLeaderId, _ := h.CheckSingleLeader()
	if finalLeaderId == origLeaderId {
		t.Errorf("got finalLeaderId==origLeaderId==%d, want them different", finalLeaderId)
	}

	// Submit 9 and check it's fully committed.
	h.SubmitToServer(newLeaderId, 9)
	sleepMs(150)
	h.CheckCommittedN(9, 5)
	h.CheckCommittedN(8, 5)

	// But 7 is not committed...
	h.CheckNotCommitted(7)
}
