package transport

import (
	"encoding/json"
	"net"
	"sync"
	
	"github.com/GlacierEQ/apex-raft-forge/pkg/raft"
)

type GRPCTransport struct {
	mu sync.Mutex
	id string
	ch chan raft.Message
	peers map[string]net.Conn
}

func NewGRPCTransport(id string) *GRPCTransport {
	return &GRPCTransport{
		id: id,
		ch: make(chan raft.Message, 1024),
		peers: make(map[string]net.Conn),
	}
}

func (t *GRPCTransport) Send(peer string, msg interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	// Stub implementation for compilation, standard library rpc would be quite extensive to mock properly here.
	// Assume simple JSON encoding to connected peers.
	
	conn, ok := t.peers[peer]
	if !ok {
		return nil
	}
	
	wrapper := struct {
		Type    string
		Payload interface{}
	}{}
	
	switch msg.(type) {
	case raft.RequestVoteRequest: wrapper.Type = "RequestVoteRequest"
	case raft.RequestVoteResponse: wrapper.Type = "RequestVoteResponse"
	case raft.AppendEntriesRequest: wrapper.Type = "AppendEntriesRequest"
	case raft.AppendEntriesResponse: wrapper.Type = "AppendEntriesResponse"
	}
	wrapper.Payload = msg
	
	enc := json.NewEncoder(conn)
	return enc.Encode(wrapper)
}

func (t *GRPCTransport) Recv() <-chan raft.Message {
	return t.ch
}
