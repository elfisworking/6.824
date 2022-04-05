package raft


// start election
// has checked 
func (rf *Raft) startElection() {
	if rf.killed() {
		return 
	}
	rf.lock("start election lock")
	rf.changeIdentity(Candidate)
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.absoluteLength() - 1,
		LastLogTerm: rf.findLogTermByAbsoulteIndex(rf.absoluteLength() - 1),
	}
	rf.unlock("start election unlcok")

	votech := make(chan bool, len(rf.peers))
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}

		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			ch <- reply.VoteGranted
			rf.lock("election terminated lock")
			rf.logger("In election: peer is %d, current term is %d, reply term %d\n", rf.me, rf.currentTerm, reply.Term)
			if rf.currentTerm < reply.Term {
				rf.reveivedLargerTerm(reply.Term)
			}
			rf.unlock("election terminated unlock")

		}(votech, index)
	}
	total_peers := len(rf.peers)
	granted_num := 1
	total_res := 1
	for !rf.killed(){
		// there has an error
		r := <-votech
		total_res += 1
		if r {
			granted_num += 1
		}
		if total_res == total_peers || granted_num > total_peers / 2 || total_res - granted_num >= total_peers / 2 {
			break
		}
	}

	if granted_num <= total_peers / 2 {

		rf.logger("grantedCount <= len/2 : count: %d, total_res: %d, total_peers: %d", granted_num, total_res, total_peers)
		return 
	}
	rf.lock("win election lock")
	if rf.currentTerm == args.Term && rf.identity == Candidate {
		rf.changeIdentity(Leader)
	}
	rf.unlock("win election unlock")

}