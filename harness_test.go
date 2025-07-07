package main

import (
	"log"
	"testing"
	"time"
)

type Harness struct {
	cluster   []*Server
	connected []bool
	n         int
	t         *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	ready := make(chan any)

	for i := 0; i < n; i++ {
		peerIds := []int{}
		for j := 0; j < n; j++ {
			if j != i {
				peerIds = append(peerIds, j)
			}
		}
		ns[i] = NewServer(i, peerIds, ready)
		ns[i].Serve()

	}

	// Connect all peers to each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	return &Harness{
		cluster:   ns,
		connected: connected,
		n:         n,
		t:         t,
	}
}

func (h *Harness) TestSingleLeader() (leaderId int, currentTerm int) {
	leaderId, currentTerm = -1, -1
	for r := 0; r < 5; r++ {
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				cm := h.cluster[i].cm
				id, term, isLeader := cm.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = id
						currentTerm = term
					} else {
						h.t.Fatal("Duplicate Leader.")
					}
				}
			}
		}
		if leaderId >= 0 {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	h.t.Fatal("leader not found")
	return
}

func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
}

func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		cm := h.cluster[i].cm
		_, _, isLeader := cm.Report()
		if isLeader {
			log.Fatal("Leader Exists")
		}
	}
}

func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
