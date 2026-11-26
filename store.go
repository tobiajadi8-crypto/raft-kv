package main

import (
	"encoding/json"
	"io"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
)

type command struct {
	Method string
	Key    string
	Value  string
}

type result struct {
	Value string
	Found bool
}

type store struct {
	mu     sync.RWMutex
	values map[string]string
}

func (s *store) Apply(entry *raft.Log) any {
	var op command
	if err := json.Unmarshal(entry.Data, &op); err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op.Method {
	case "PUT":
		s.values[op.Key] = op.Value
	case "GET":
	default:
		panic("unexpected log entry")
	}
	v, ok := s.values[op.Key]
	return result{v, ok}
}

type snapshot map[string]string

func (s *store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return snapshot(maps.Clone(s.values)), nil
}
func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (snapshot) Release() {}
func (s *store) Restore(source io.ReadCloser) error {
	defer source.Close()
	values := make(map[string]string)
	if err := json.NewDecoder(source).Decode(&values); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values = values
	return nil
}
