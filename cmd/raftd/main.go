package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/GlacierEQ/apex-raft-forge/pkg/raft"
	"github.com/GlacierEQ/apex-raft-forge/pkg/raft/transport"
)

type MemoryStorage struct {
	term     uint64
	votedFor string
	log      []raft.LogEntry
}
func (s *MemoryStorage) SaveStateAndLog(term uint64, votedFor string, log []raft.LogEntry) error {
	s.term = term; s.votedFor = votedFor; s.log = log
	return nil
}
func (s *MemoryStorage) LoadStateAndLog() (uint64, string, []raft.LogEntry, error) {
	return s.term, s.votedFor, s.log, nil
}

func main() {
	id := flag.String("id", "node1", "node id")
	peersFlag := flag.String("peers", "", "comma separated peer ids")
	port := flag.Int("port", 8080, "http port")
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	storage := &MemoryStorage{}
	tr := transport.NewGRPCTransport(*id)
	applyCh := make(chan raft.ApplyMsg, 10)

	node := raft.NewRaft(*id, peers, storage, tr, applyCh)

	http.HandleFunc("/propose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, _ := ioutil.ReadAll(r.Body)
		index, term, isLeader := node.Propose(body)
		if !isLeader {
			http.Error(w, "Not leader", http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "Proposed at index %d term %d\n", index, term)
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		// Needs mutex for safe read normally, quick mock here
		status := map[string]interface{}{
			"id": *id,
		}
		json.NewEncoder(w).Encode(status)
	})

	go func() {
		for msg := range applyCh {
			fmt.Printf("Applied: %v\n", msg)
		}
	}()

	fmt.Printf("Starting %s on port %d...\n", *id, *port)
	http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	time.Sleep(1 * time.Hour) // block main
}
