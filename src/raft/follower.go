package raft
func (rf *Raft) AppendEntrisHandler(args * AppendEntirsArgs, reply * AppendEntirsReply) {
	rf.lock("Append Entries Handler lock")
	defer rf.unlock("Append Entires Handler unlock")
	if args.LeaderID == rf.votedFor {
		rf.resetElectionTimer()
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeIdentity(Follower)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	if reply.Term > args.Term {
		reply.Success = false
		return 
	}
	if len(args.Entrirs) == 0 {

		reply.Term = rf.currentTerm
		rf.logger("peer %d is Hearing a heart beart, args term is %d, reply term is %d", rf.me, args.Term, reply.Term)
		if reply.Term > args.Term {
			reply.Success = false
		} else {
			rf.resetElectionTimer()
		}
	}
}