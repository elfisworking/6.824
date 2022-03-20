package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 
func (rf *Raft)calDuration() time.Duration {
	return time.Duration(rf.timeout + rand.Intn(300)) * time.Millisecond
}

func (rf *Raft)resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.calDuration())
}

// raft peer change their identity
func (rf * Raft) changeIdentity(identity State) {
	rf.logger("%d peer chagne its identity to %s", rf.me, identity)
	switch identity {
	case Candidate:
		rf.currentTerm += 1
		rf.votedFor = rf.me // vote for me
		rf.identity = Candidate
		rf.resetElectionTimer()
	case Leader:
		rf.identity = Leader
		// stop election timer
		rf.electionTimer.Stop()
		go rf.sendRegularHeartBeats()
	case Follower:
		rf.identity = Follower
		rf.resetElectionTimer()
	}
}

func (rf *Raft) reveivedLargerTerm(largeTerm int) {
	rf.currentTerm = largeTerm
	rf.votedFor = -1
	rf.changeIdentity(Follower)
}

func (rf *Raft) lock(format string, a...interface {}) {
	rf.mu.Lock()
	rf.logger(format, a...)
}

func (rf * Raft) unlock(format string, a...interface {}) {
	rf.logger(format, a...)
	rf.mu.Unlock()
}

func (rf * Raft) logger(format string, a...interface{}) {
	DPrintf("me: %d, identity:%v, term: %d, leader:%d\n", rf.me, rf.identity, rf.currentTerm, rf.votedFor)
	DPrintf(format, a...)
}