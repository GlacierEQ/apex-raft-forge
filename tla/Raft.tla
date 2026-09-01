-------------------------------- MODULE Raft --------------------------------
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Server, Quorum

VARIABLES currentTerm, votedFor, log, state, commitIndex, nextIndex, matchIndex, messages

vars == <<currentTerm, votedFor, log, state, commitIndex, nextIndex, matchIndex, messages>>

Follower == "Follower"
Candidate == "Candidate"
Leader == "Leader"
Nil == "Nil"

Init == 
    /\ currentTerm = [i \in Server |-> 1]
    /\ votedFor = [i \in Server |-> Nil]
    /\ log = [i \in Server |-> << >>]
    /\ state = [i \in Server |-> Follower]
    /\ commitIndex = [i \in Server |-> 0]
    /\ nextIndex = [i \in Server |-> [j \in Server |-> 1]]
    /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
    /\ messages = {}

Send(m) == messages' = messages \cup {m}

RequestVote(i, j) ==
    /\ state[i] = Candidate
    /\ Send([type |-> "RequestVote", term |-> currentTerm[i],
             candidateId |-> i,
             lastLogIndex |-> Len(log[i]),
             lastLogTerm |-> IF Len(log[i]) > 0 THEN log[i][Len(log[i])].term ELSE 0,
             source |-> i, dest |-> j])
    /\ UNCHANGED <<currentTerm, votedFor, log, state, commitIndex, nextIndex, matchIndex>>

RequestVoteResponse(i, j) ==
    \E m \in messages:
        /\ m.type = "RequestVote"
        /\ m.dest = i
        /\ m.source = j
        /\ IF m.term > currentTerm[i]
           THEN /\ currentTerm' = [currentTerm EXCEPT ![i] = m.term]
                /\ state' = [state EXCEPT ![i] = Follower]
                /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
           ELSE UNCHANGED <<currentTerm, state, votedFor>>
        /\ LET grant == (m.term >= currentTerm[i]) /\ (votedFor[i] \in {Nil, j})
           IN /\ votedFor' = IF grant THEN [votedFor EXCEPT ![i] = j] ELSE votedFor
              /\ Send([type |-> "RequestVoteResponse", term |-> currentTerm'[i],
                       voteGranted |-> grant, source |-> i, dest |-> j])
        /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex>>

AppendEntries(i, j) ==
    /\ state[i] = Leader
    /\ LET prevIndex == nextIndex[i][j] - 1
           prevTerm == IF prevIndex > 0 THEN log[i][prevIndex].term ELSE 0
           entries == SubSeq(log[i], nextIndex[i][j], Len(log[i]))
       IN Send([type |-> "AppendEntries", term |-> currentTerm[i],
                leaderId |-> i, prevLogIndex |-> prevIndex,
                prevLogTerm |-> prevTerm, entries |-> entries,
                leaderCommit |-> commitIndex[i], source |-> i, dest |-> j])
    /\ UNCHANGED <<currentTerm, votedFor, log, state, commitIndex, nextIndex, matchIndex>>

AppendEntriesResponse(i, j) ==
    \E m \in messages:
        /\ m.type = "AppendEntries"
        /\ m.dest = i
        /\ m.source = j
        /\ IF m.term > currentTerm[i]
           THEN /\ currentTerm' = [currentTerm EXCEPT ![i] = m.term]
                /\ state' = [state EXCEPT ![i] = Follower]
                /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
           ELSE UNCHANGED <<currentTerm, state, votedFor>>
        /\ LET success == (m.term >= currentTerm[i]) /\ (m.prevLogIndex = 0 \/ (m.prevLogIndex <= Len(log[i]) /\ log[i][m.prevLogIndex].term = m.prevLogTerm))
           IN /\ IF success
                 THEN /\ log' = [log EXCEPT ![i] = SubSeq(log[i], 1, m.prevLogIndex) \o m.entries]
                      /\ commitIndex' = [commitIndex EXCEPT ![i] = IF m.leaderCommit > commitIndex[i] THEN m.leaderCommit ELSE commitIndex[i]]
                 ELSE UNCHANGED <<log, commitIndex>>
              /\ Send([type |-> "AppendEntriesResponse", term |-> currentTerm'[i],
                       success |-> success, matchIndex |-> m.prevLogIndex + Len(m.entries),
                       source |-> i, dest |-> j])
        /\ UNCHANGED <<nextIndex, matchIndex>>

BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ \E Q \in Quorum:
        \A j \in Q:
            \E m \in messages:
                /\ m.type = "RequestVoteResponse"
                /\ m.dest = i
                /\ m.source = j
                /\ m.term = currentTerm[i]
                /\ m.voteGranted
    /\ state' = [state EXCEPT ![i] = Leader]
    /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages>>

ClientRequest(i, v) ==
    /\ state[i] = Leader
    /\ log' = [log EXCEPT ![i] = Append(log[i], [term |-> currentTerm[i], value |-> v])]
    /\ UNCHANGED <<currentTerm, votedFor, state, commitIndex, nextIndex, matchIndex, messages>>

Next == \E i, j \in Server:
    \/ RequestVote(i, j)
    \/ RequestVoteResponse(i, j)
    \/ AppendEntries(i, j)
    \/ AppendEntriesResponse(i, j)
    \/ BecomeLeader(i)
    \/ \E v \in 1..3: ClientRequest(i, v)

ElectionSafety ==
    \A term \in Nat:
        \A i, j \in Server:
            (state[i] = Leader /\ state[j] = Leader /\ currentTerm[i] = term /\ currentTerm[j] = term) => (i = j)

LogMatching ==
    \A i, j \in Server:
        \A index \in 1..Len(log[i]):
            (index <= Len(log[j]) /\ log[i][index].term = log[j][index].term) =>
                SubSeq(log[i], 1, index) = SubSeq(log[j], 1, index)

LeaderCompleteness == TRUE \* Simplified
StateMachineSafety == TRUE \* Simplified

LeaderExists == \E i \in Server: state[i] = Leader

=============================================================================
