package raft

type InstallSnapshot struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}
// has checked
func (rf *Raft) sendInstall(index int) {
	snapshotArgs := InstallSnapshot{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.snapshot,
	}
	snapshotReply := InstallSnapshotReply{}
	rf.unlock("send install unlock %d", rf.me)
	ok := rf.sendInstallSnapshotRequest(index, &snapshotArgs, &snapshotReply)
	if !ok {
		return
	}
	if snapshotReply.Term > snapshotArgs.Term {
		rf.lock("change term for snapshot, lock")
		if rf.currentTerm < snapshotReply.Term {
			rf.reveivedLargerTerm(snapshotReply.Term)
			rf.unlock("change term for snapshot success, unlcok")
			return
		}
		rf.unlock("change term for snapshot unsuccess, unlock")
	}
	rf.lock("snapshot update matchindex lock %d", rf.me)
	// 吐了  
	rf.nextIndex[index] = rf.lastIncludedIndex + 1
	rf.unlock("snapshot update matchindex unlock %d", rf.me)
}


func (rf *Raft) sendInstallSnapshotRequest(server int, args *InstallSnapshot, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShotHandler", args, reply)
	return ok
}
// has check
func (rf *Raft) InstallSnapShotHandler(args *InstallSnapshot, reply *InstallSnapshotReply) {
	rf.lock("sendInstallSnapshotHandler lock")
	defer rf.unlock("sendInstallSnapShotHandler unlcok")
	if args.LeaderId == rf.votedFor {
		rf.resetElectionTimer()
	}
	if args.Term > rf.currentTerm {
		rf.reveivedLargerTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	if reply.Term > args.Term {
		// not leader
		return
	}
	rf.done = true
	rf.sendSnapshotApply(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)

}
// has check
func (rf *Raft) sendSnapshotApply(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) {
	applyMsy := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  lastIncludedTerm,
		SnapshotIndex: lastIncludedIndex,
	}
	rf.applyCh <- applyMsy
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// has check
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock("snapshot function lock")
	defer rf.unlock("snapshot function unlock")
	rf.snapshot = snapshot
	rf.persist()
	// rf.logger("snapshot index %d", index)
	relIndex := rf.relatvieIndex(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[relIndex].Term
	// ?
	if relIndex == len(rf.log)-1 {
		rf.log = make([]LogEntry, 0)
	} else {
		rf.log = rf.log[relIndex+1:]
	}
	// 持久化
	rf.persist()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// has check
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// rule  4 如果 done为 false，则回复并继续等待之后的数据
	// rf.lock("CondInstallSnapshot()")
	// rf.unlock("exiting CondInstallSnapshot()")
	if !rf.done {
		return false
	}
	rf.logger("CondInstalSnapshot function lastIncludedIndex %d, peer is %d, log length: %d", lastIncludedIndex, rf.me, rf.absoluteLength() - 1)
	if rf.containsIndexTerm(lastIncludedIndex, lastIncludedTerm) {
		// rule 6 如果现存的日志拥有相同的最后任期号和索引值，则后面的数据继续保留并且回复
		rf.log = rf.log[rf.relatvieIndex(lastIncludedIndex+1):]
	} else {
		//  rule 7 丢弃全部日志
		rf.log = make([]LogEntry, 0)
	}
	rf.persist()
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	rf.persist()
	// 这里参考的可能有问题 snapshot部分都是commited and applied
	rf.commitIndex = max(rf.commitIndex, lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, lastIncludedIndex)
	// Your code here (2D).

	return true
}

func (rf *Raft) containsIndexTerm(absIndex int, term int) bool {
	if absIndex < rf.lastIncludedIndex {
		panic("findLogTermByAbsoluteIndex(): invalid index")
	}
	inLastSnapshot := (absIndex == rf.lastIncludedIndex && term == rf.lastIncludedTerm)
	if inLastSnapshot {
		return true
	}
	notBeyondLogLength := rf.relatvieIndex(absIndex) < len(rf.log)
	if !notBeyondLogLength {
		return false
	}
	termMatches := rf.log[rf.relatvieIndex(absIndex)].Term == term
	return termMatches
}
