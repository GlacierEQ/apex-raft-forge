package raft

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    uint64
	Command []byte
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateId  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
}

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
}

type Storage interface {
	SaveStateAndLog(term uint64, votedFor string, log []LogEntry) error
	LoadStateAndLog() (uint64, string, []LogEntry, error)
}

type Transport interface {
	Send(peer string, msg interface{}) error
	Recv() <-chan Message
}

type Message struct {
	Source  string
	Payload interface{}
}
