------------------------------ MODULE MC_Raft ------------------------------
EXTENDS Raft

CONSTANTS s1, s2, s3
MC_Server == {s1, s2, s3}

MC_Quorum == {{s1, s2}, {s1, s3}, {s2, s3}, {s1, s2, s3}}

StateConstraint ==
    /\ \A i \in MC_Server: currentTerm[i] <= 3
    /\ \A i \in MC_Server: Len(log[i]) <= 3

=============================================================================
