package raft


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock("snapshot fucntion lock")
	defer rf.lock("snapshot function unlock")
	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	relIndex := rf.relatvieIndex(index)
	rf.lastIncludedTerm = rf.log[relIndex].Term
	if relIndex == len(rf.log) - 1 {
		rf.log = make([]LogEntry, 0)
	} else {
		rf.log = rf.log[relIndex + 1 : ]
	}
	// 持久化
	rf.persist()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}


