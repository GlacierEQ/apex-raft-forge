package raft

// LogUpToDate is Raft §5.4.1: a candidate’s log is at least as up-to-date as
// the voter’s if its last term is higher, or the terms match and its index is
// not shorter. Pure function so election safety can be tested without the
// ticker goroutine.
func LogUpToDate(candLastTerm, candLastIndex, voterLastTerm, voterLastIndex uint64) bool {
	if candLastTerm != voterLastTerm {
		return candLastTerm > voterLastTerm
	}
	return candLastIndex >= voterLastIndex
}
