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
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			rf.nextIndex[index] = rf.absoluteLength()
			rf.matchIndex[index] = 0
		}
		// stop election timer
		rf.electionTimer.Stop()
		go rf.sendRegularHeartBeats()
	case Follower:
		rf.identity = Follower
		rf.resetElectionTimer()
	}
}
func (rf *Raft) checkConsistency(args * AppendEntirsArgs, reply *AppendEntirsReply) bool {
	if rf.absoluteLength() <= args.PrevLogIndex {
		reply.Success = false
		reply.NewNextIndex =rf.absoluteLength()
		return false
	} else if rf.findLogTermByAbsoulteIndex(args.PrevLogIndex) != args.PrevLigTerm {
		// judge same index and term
		reply.Success = false
		// reply.NewNextIndex = rf.findBadIndex(rf.findLogTermByAbsoulteIndex(args.PrevLogIndex))
		reply.NewNextIndex = args.PrevLogIndex - 1
		return false
	}
	return true
}

// func (rf *Raft) findBadIndex(badTerm int) int {
// 	for index, entry := range rf.log {
// 		if entry.Term == badTerm {
// 			return rf.absoluteIndex(index)
// 		}
// 	}
// 	return -1
// }

func (rf *Raft) absoluteLength() int {
	// may there has problem
	return len(rf.log)
}
 

func (rf * Raft) absoluteIndex(relativeIndex int) int {
	// return relativeIndex + rf.lastInstalledIndex + 1
	return relativeIndex
}


func (rf *Raft)relatvieIndex(absoluteIndex int) int {
	// snapshot
	// return absoluteIndex - rf.lastInstallIndex - 1
	return absoluteIndex
}

func (rf *Raft) findLogTermByAbsoulteIndex(absoluteIndex int) int {
	return rf.log[rf.relatvieIndex(absoluteIndex)].Term
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

func min(a int, b int) int {
	if a > b {
		return b
	} 
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}