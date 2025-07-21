package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("Unreachable")
	}
}

type CommitEntry struct {
	Command any
	Term    int
	Index   int
}

type LogEntry struct {
	Command any
	Term    int
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int

	server *Server

	state       CMState
	currentTerm int
	votedFor    int

	// Leader에게 AE 받을 때 마다 초기화
	// 랜덤한 시간 동안 응답을 받지 못하면 Candidate가 됨.
	electionResetEvent time.Time

	commitChan  chan<- CommitEntry
	commitIndex int
	lastApplied int
	log         []LogEntry

	nextIndex  map[int]int
	matchIndex map[int]int

	newCommitReadyChan chan struct{}
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan any, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1
	cm.commitChan = commitChan
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()

	return cm
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.state = Dead
	cm.dlog("Consensus Module Stopped")
	close(cm.newCommitReadyChan)

}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		currentTerm := cm.currentTerm
		lastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, lastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   lastApplied + i + 1,
				Term:    currentTerm,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	termStarted := cm.currentTerm

	ticker := time.NewTicker(10 * time.Millisecond)

	for {
		<-ticker.C
		cm.mu.Lock()

		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}
		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}

		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	cm.electionResetEvent = time.Now()
	currentTerm := cm.currentTerm
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", currentTerm)

	votesReceived := 1

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			currentLastLogIndex, currentLastLogTerm := cm.lastLogIndexAndTerm()
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  cm.id,
				LastLogIndex: currentLastLogIndex,
				LastLogTerm:  currentLastLogTerm,
			}
			cm.mu.Unlock()

			var reply RequestVoteReply
			cm.dlog("sending RequestVote to %d: %+v", peerId, args)

			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				if cm.state != Candidate {
					cm.dlog("while waiting for reply, state = %v", cm.state)
					return
				}

				if currentTerm < cm.currentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				}

				if currentTerm == cm.currentTerm {
					if reply.VoteGranted {
						votesReceived++
					}
					if votesReceived*2 > len(cm.peerIds)+1 {
						cm.dlog("wins election with %d votes", votesReceived)
						cm.startLeader()
						return
					}
				}
			}
		}(peerId)
	}

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term)

	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("becomes Leader; term=%d, log=%v", cm.currentTerm)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	currentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			nextIndex := cm.nextIndex[peerId]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[nextIndex:]
			args := AppendEntriesArgs{
				Term:     currentTerm,
				LeaderId: cm.id,

				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, nextIndex, args)

			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > currentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && currentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = nextIndex + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)
						currentCommitIndex := cm.commitIndex
						for i := currentCommitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						if currentCommitIndex != cm.commitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
						}
					} else {
						cm.nextIndex[peerId] = nextIndex - 1
						cm.dlog("AppendEntries reply from %d failed: nextIndex := %v", peerId, cm.nextIndex)
					}
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) Submit(command any) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.dlog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.dlog("log=%v", cm.log)
		return true
	}
	return false
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogIndex > lastLogIndex || (args.LastLogIndex == lastLogIndex && args.LastLogTerm >= lastLogTerm)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}
	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		}
	}
	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)

	return nil
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// dlog logs a debugging message if DebugCM > 0.
func (cm *ConsensusModule) dlog(format string, args ...any) {
	format = fmt.Sprintf("[%d] ", cm.id) + format
	log.Printf(format, args...)
}
