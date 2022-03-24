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
			Entrirs: make([]LogEntry, 0),
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

				rf.lock("heart beat change term lock")
				rf.logger("leader receive heart beat reponse : leader peer %d, args term is %d, reply term is %d", rf.me, args.Term, args.Term)
				if rf.currentTerm < args.Term {
					rf.reveivedLargerTerm(args.Term)
				}
				rf.unlock("heart beat change term unlock")
				if !reply.Success {
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
		if rf.identity != Leader {
			break
		}
		logLength := len(rf.log)
		nextIndex := rf.nextIndex[index]
		args := AppendEntirsArgs{
			Term: rf.currentTerm,
			// pass
		}
	}
} 

func (rf *Raft) sendAppendEntries(index int, args *AppendEntirsArgs, reply *AppendEntirsReply) bool {
	ok := rf.peers[index].Call("Raft.AppendEntrisHandler", args, reply)
	return ok
}


