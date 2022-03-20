package raft
type AppendEntirsArgs struct {
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLigTerm int
	Entrirs  []LogEntry
	LeaderCommit int
}

type AppendEntirsReply struct {
	Term int
	Success bool
}