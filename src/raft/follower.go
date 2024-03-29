package raft
// has checked
func (rf *Raft) AppendEntrisHandler(args * AppendEntirsArgs, reply * AppendEntirsReply) {
	rf.lock("Append Entries Handler lock")
	defer rf.unlock("Append Entires Handler unlock")
	if args.LeaderID == rf.votedFor {
		rf.resetElectionTimer()
	}
	if args.Term > rf.currentTerm {
		rf.reveivedLargerTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	// 合法的leader 应该拥有最大的Term
	if reply.Term > args.Term {
		reply.Success = false
		return 
	}
	if len(args.Entrirs) == 0 {
		if !rf.checkConsistency(args, reply) {
			return
		}
	} else {
		// 不是heart beat packet
		// 找不到prev
		if !rf.checkConsistency(args, reply) {
			return
		} else {
			absAppendIndex := args.PrevLogIndex + 1
			// 将绝对路径转化为follower的相对路径
			relAppendIndex := rf.relatvieIndex(absAppendIndex)
			rf.log = rf.log[:relAppendIndex]
			rf.persist()
			rf.log = append(rf.log, args.Entrirs...) 
			rf.persist()
		}
	}
	// append 里面附带了已经commit的信息，这里也需要同步以下
	if args.LeaderCommit > rf.commitIndex {
		prevIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.absoluteLength()-1)
		// 向state machine 发送 信息
		for i := prevIndex + 1; i <= rf.commitIndex; i++ {
			rf.sendApplyMsg(i)
		}
		rf.lastApplied = rf.commitIndex
	}
	rf.done = false
}
// has checked
func (rf *Raft) sendApplyMsg(index int) {
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command: rf.log[rf.relatvieIndex(index)].Command,
		CommandIndex: index,
	}
	// for 2D if not unlock functon, it will cause deadblock
	rf.unlock("send Apply Msg unlock")
	rf.applyCh <- applyMsg
	rf.lock("send Apply Msg lock")
}