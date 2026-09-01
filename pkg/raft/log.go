package raft

import (
	"errors"
	"sync"
)

type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry
	storage Storage
}

func NewRaftLog(storage Storage) (*RaftLog, error) {
	_, _, entries, err := storage.LoadStateAndLog()
	if err != nil {
		entries = make([]LogEntry, 0)
	}
	return &RaftLog{
		entries: entries,
		storage: storage,
	}, nil
}

func (l *RaftLog) append(entries ...LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

func (l *RaftLog) getEntry(index uint64) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 || index > uint64(len(l.entries)) {
		return LogEntry{}, errors.New("index out of bounds")
	}
	return l.entries[index-1], nil
}

func (l *RaftLog) getLastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.entries))
}

func (l *RaftLog) getLastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *RaftLog) slice(startIndex uint64, endIndex uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if startIndex == 0 {
		startIndex = 1
	}
	if endIndex > uint64(len(l.entries)) {
		endIndex = uint64(len(l.entries)) + 1
	}
	if startIndex >= endIndex {
		return nil
	}
	res := make([]LogEntry, endIndex-startIndex)
	copy(res, l.entries[startIndex-1:endIndex-1])
	return res
}

func (l *RaftLog) truncate(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index > uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:index]
}
