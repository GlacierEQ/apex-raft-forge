package raft

import (
	"math/rand"
	"sync"
	"time"
)

type Raft struct {
	mu sync.RWMutex

	id    string
	peers []string

	currentTerm uint64
	votedFor    string
	log         *RaftLog

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	state State
	votes int

	storage   Storage
	transport Transport
	applyCh   chan ApplyMsg

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func NewRaft(id string, peers []string, storage Storage, transport Transport, applyCh chan ApplyMsg) *Raft {
	rLog, _ := NewRaftLog(storage)
	term, votedFor, _, _ := storage.LoadStateAndLog()
	
	r := &Raft{
		id:             id,
		peers:          peers,
		currentTerm:    term,
		votedFor:       votedFor,
		log:            rLog,
		nextIndex:      make(map[string]uint64),
		matchIndex:     make(map[string]uint64),
		state:          Follower,
		storage:        storage,
		transport:      transport,
		applyCh:        applyCh,
		electionTimer:  time.NewTimer(randomElectionTimeout()),
		heartbeatTimer: time.NewTimer(50 * time.Millisecond),
	}

	for _, peer := range peers {
		r.nextIndex[peer] = 1
		r.matchIndex[peer] = 0
	}

	go r.tick()
	return r
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (r *Raft) persist() {
	r.log.mu.RLock()
	defer r.log.mu.RUnlock()
	r.storage.SaveStateAndLog(r.currentTerm, r.votedFor, r.log.entries)
}

func (r *Raft) resetElectionTimer() {
	if !r.electionTimer.Stop() {
		select {
		case <-r.electionTimer.C:
		default:
		}
	}
	r.electionTimer.Reset(randomElectionTimeout())
}

func (r *Raft) tick() {
	for {
		select {
		case <-r.electionTimer.C:
			r.mu.Lock()
			if r.state != Leader {
				r.startElection()
			}
			r.mu.Unlock()
		case <-r.heartbeatTimer.C:
			r.mu.Lock()
			if r.state == Leader {
				r.broadcastAppendEntries()
			}
			r.heartbeatTimer.Reset(50 * time.Millisecond)
			r.mu.Unlock()
		case msg := <-r.transport.Recv():
			r.mu.Lock()
			switch req := msg.Payload.(type) {
			case RequestVoteRequest:
				resp := r.handleRequestVote(req)
				r.transport.Send(msg.Source, resp)
			case RequestVoteResponse:
				r.handleRequestVoteResponse(req)
			case AppendEntriesRequest:
				resp := r.handleAppendEntries(req)
				r.transport.Send(msg.Source, resp)
			case AppendEntriesResponse:
				r.handleAppendEntriesResponse(msg.Source, req)
			}
			r.mu.Unlock()
		}
	}
}

func (r *Raft) becomeFollower(term uint64, leader string) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.persist()
	r.resetElectionTimer()
}

func (r *Raft) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.votes = 1
	r.persist()
	r.resetElectionTimer()
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	for _, peer := range r.peers {
		r.nextIndex[peer] = r.log.getLastIndex() + 1
		r.matchIndex[peer] = 0
	}
	r.broadcastAppendEntries()
}

func (r *Raft) startElection() {
	r.becomeCandidate()
	req := RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: r.log.getLastIndex(),
		LastLogTerm:  r.log.getLastTerm(),
	}
	for _, peer := range r.peers {
		r.transport.Send(peer, req)
	}
}

func (r *Raft) broadcastAppendEntries() {
	for _, peer := range r.peers {
		prevLogIndex := r.nextIndex[peer] - 1
		var prevLogTerm uint64
		if prevLogIndex > 0 {
			entry, err := r.log.getEntry(prevLogIndex)
			if err == nil {
				prevLogTerm = entry.Term
			}
		}
		
		entries := r.log.slice(r.nextIndex[peer], r.log.getLastIndex()+1)
		
		req := AppendEntriesRequest{
			Term:         r.currentTerm,
			LeaderId:     r.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		r.transport.Send(peer, req)
	}
}

func (r *Raft) handleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term, "")
	}
	
	resp := RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	
	if req.Term < r.currentTerm {
		return resp
	}
	
	lastLogIndex := r.log.getLastIndex()
	lastLogTerm := r.log.getLastTerm()
	
	logUpToDate := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)
	
	if (r.votedFor == "" || r.votedFor == req.CandidateId) && logUpToDate {
		resp.VoteGranted = true
		r.votedFor = req.CandidateId
		r.persist()
		r.resetElectionTimer()
	}
	return resp
}

func (r *Raft) handleRequestVoteResponse(resp RequestVoteResponse) {
	if r.state != Candidate {
		return
	}
	if resp.Term > r.currentTerm {
		r.becomeFollower(resp.Term, "")
		return
	}
	if resp.VoteGranted {
		r.votes++
		if r.votes > (len(r.peers)+1)/2 {
			r.becomeLeader()
		}
	}
}

func (r *Raft) handleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	if req.Term > r.currentTerm {
		r.becomeFollower(req.Term, req.LeaderId)
	}
	
	resp := AppendEntriesResponse{Term: r.currentTerm, Success: false, MatchIndex: 0}
	
	if req.Term < r.currentTerm {
		return resp
	}
	
	r.resetElectionTimer()
	
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > r.log.getLastIndex() {
			return resp
		}
		entry, _ := r.log.getEntry(req.PrevLogIndex)
		if entry.Term != req.PrevLogTerm {
			return resp
		}
	}
	
	r.log.truncate(req.PrevLogIndex)
	r.log.append(req.Entries...)
	r.persist()
	
	if req.LeaderCommit > r.commitIndex {
		lastIndex := r.log.getLastIndex()
		if req.LeaderCommit < lastIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastIndex
		}
		r.applyLog()
	}
	
	resp.Success = true
	resp.MatchIndex = req.PrevLogIndex + uint64(len(req.Entries))
	return resp
}

func (r *Raft) handleAppendEntriesResponse(peer string, resp AppendEntriesResponse) {
	if r.state != Leader {
		return
	}
	if resp.Term > r.currentTerm {
		r.becomeFollower(resp.Term, "")
		return
	}
	if resp.Success {
		r.nextIndex[peer] = resp.MatchIndex + 1
		r.matchIndex[peer] = resp.MatchIndex
		r.maybeCommit()
	} else {
		if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]--
		}
	}
}

func (r *Raft) maybeCommit() {
	for n := r.log.getLastIndex(); n > r.commitIndex; n-- {
		entry, _ := r.log.getEntry(n)
		if entry.Term != r.currentTerm {
			continue
		}
		matchCount := 1
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				matchCount++
			}
		}
		if matchCount > (len(r.peers)+1)/2 {
			r.commitIndex = n
			r.applyLog()
			break
		}
	}
}

func (r *Raft) applyLog() {
	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		entry, _ := r.log.getEntry(r.lastApplied)
		r.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: r.lastApplied,
		}
	}
}

func (r *Raft) Propose(cmd []byte) (index uint64, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.state != Leader {
		return 0, 0, false
	}
	
	entry := LogEntry{Term: r.currentTerm, Command: cmd}
	r.log.append(entry)
	r.persist()
	
	index = r.log.getLastIndex()
	term = r.currentTerm
	r.broadcastAppendEntries()
	
	return index, term, true
}
