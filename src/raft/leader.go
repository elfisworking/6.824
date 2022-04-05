package raft

import (
	"time"
)
func (rf *Raft) sendRegularHeartBeats() {
	for !rf.killed() {
		rf.lock("Create Heart Beat Lock")
		if rf.identity != Leader {
			rf.unlock("Create Heart Beat Unlock identity is not leader")
			return 
		}
		args := AppendEntirsArgs{
			Term: rf.currentTerm,
			LeaderID: rf.me,
			PrevLogIndex: rf.absoluteLength() - 1,
			PrevLigTerm: rf.findLogTermByAbsoulteIndex(rf.absoluteLength() - 1),
			Entrirs: make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
		}
		rf.unlock("Create Heart Beat Unlock after create args")
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int){
				reply := AppendEntirsReply{}
				ok := rf.sendAppendEntries(index, &args, &reply)
				if !ok {
					return 
				}
				if reply.Term > args.Term {
					rf.lock("start_HB_change_term")
					if rf.currentTerm < reply.Term {
						rf.reveivedLargerTerm(reply.Term)
					}
					rf.unlock("start_HB_change_term")
				} else if !reply.Success { // if log inconsistency detected, force update
					go rf.forceUpdate(index)
				}
			}(index)
		}
		time.Sleep(100 * time.Millisecond)
	}
}


// sycn leader and follower
func (rf *Raft) forceUpdate(index int ) {
	for !rf.killed() {
		rf.lock("force update lock, updating server %d", index)
		if rf.identity != Leader {
			rf.unlock("force update lock, updating server %d", index)
			return
		}
		logLength := rf.absoluteLength()
		nextIndex := rf.nextIndex[index] // absolute index
		if nextIndex <= rf.lastIncludedIndex {
			rf.sendInstall(index)
			return 
		}
		args := AppendEntirsArgs{
			Term: rf.currentTerm,
			LeaderID: rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLigTerm: rf.findLogTermByAbsoulteIndex(nextIndex - 1),
			Entrirs: rf.log[rf.relatvieIndex(nextIndex) : ],
			LeaderCommit: rf.commitIndex,
		}
		rf.unlock("force update unlock, unpdating server %d", index)
		reply := AppendEntirsReply{}
		ok := rf.sendAppendEntries(index, &args, &reply)
		if !ok {
			// this handle maybe have problem
			return 
		}
		if reply.Term > args.Term {
			rf.lock("force update change term, locking")
			if rf.currentTerm < reply.Term {
				// has persist
				rf.reveivedLargerTerm(reply.Term)
				rf.unlock("force update change term, unlocking")
				return 
			}
			rf.unlock("force update change term, unlocking")
		}

		if reply.Success {
			rf.lock("force update success lock")
			rf.nextIndex[index] = logLength
			rf.matchIndex[index] = logLength - 1
			rf.checkMatchIndex()
			rf.unlock("force update success unlock")
			return
		} 
		rf.lock("force update fail lock")
		rf.nextIndex[index] = reply.NewNextIndex
		rf.unlock("force update unfail unlock")
	}
} 

func (rf *Raft) sendAppendEntries(index int, args *AppendEntirsArgs, reply *AppendEntirsReply) bool {
	ok := rf.peers[index].Call("Raft.AppendEntrisHandler", args, reply)
	return ok
}

// has checked
func (rf *Raft) checkMatchIndex() {
	rf.matchIndex[rf.me] = rf.absoluteLength() - 1
	threshold := len(rf.peers) / 2
	count := 0
	minValue := 0
	for _, value := range rf.matchIndex {
		if value > rf.commitIndex {
			count++
		if value < minValue || minValue == 0 {
			minValue = value
		}
		}
	}
	if count > threshold {
		for i := minValue; i > rf.commitIndex; i-- {
			if rf.findLogTermByAbsoulteIndex(i) == rf.currentTerm {
				prevCommit  := rf.commitIndex
				rf.commitIndex = i
				for j := prevCommit + 1; j <= i; j++ {
					rf.sendApplyMsg(j)
					rf.lastApplied = j
				}
				break
			}
		}
	}
}

