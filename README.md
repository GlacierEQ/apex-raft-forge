# apex-raft-forge
Production Raft consensus library in Go, backed by TLA+ specification.

## Architecture
- Core Raft library inside `pkg/raft`
- Pluggable Transport and Storage layers
- HTTP Daemon in `cmd/raftd`

## TLA+ Spec
The specification in `tla/Raft.tla` formalizes ElectionSafety, LogMatching, and LeaderCompleteness invariants.

## Running
```bash
make run-node1
```
