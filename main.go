package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/raft"
	bolt "github.com/hashicorp/raft-boltdb/v2"
)

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func handler(r *raft.Raft, state *store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/digest", func(w http.ResponseWriter, q *http.Request) {
		state.mu.RLock()
		data, _ := json.Marshal(state.values)
		state.mu.RUnlock()
		reply(w, 200, map[string]string{"sha256": fmt.Sprintf("%x", sha256.Sum256(data))})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, q *http.Request) {
		_, leader := r.LeaderWithID()
		reply(w, 200, map[string]any{"role": r.State().String(), "leader": leader, "applied_index": r.AppliedIndex()})
	})
	mux.HandleFunc("POST /snapshot", func(w http.ResponseWriter, q *http.Request) {
		err := r.Snapshot().Error()
		if err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
			http.Error(w, err.Error(), 503)
			return
		}
		w.WriteHeader(204)
	})
	kv := func(w http.ResponseWriter, q *http.Request) {
		key := q.PathValue("key")
		if len(key) > 256 || !utf8.ValidString(key) {
			http.Error(w, "invalid key", 400)
			return
		}
		if r.State() != raft.Leader {
			_, leader := r.LeaderWithID()
			reply(w, 503, map[string]any{"error": "leader required", "leader": leader})
			return
		}
		op := command{Method: q.Method, Key: key}
		if q.Method == "PUT" {
			body, err := io.ReadAll(http.MaxBytesReader(w, q.Body, 65536))
			if err != nil {
				http.Error(w, "cannot read value (limit 64 KiB)", 413)
				return
			}
			if !utf8.Valid(body) {
				http.Error(w, "UTF-8 value required", 400)
				return
			}
			op.Value = string(body)
		}
		data, _ := json.Marshal(op)
		future := r.Apply(data, 2*time.Second)
		if err := future.Error(); err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		value := future.Response().(result)
		if !value.Found {
			http.Error(w, "key missing", 404)
			return
		}
		if q.Method == "PUT" {
			w.WriteHeader(204)
			return
		}
		reply(w, 200, map[string]string{"value": value.Value})
	}
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, q *http.Request) {
		if q.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		kv(w, q)
	})
	mux.HandleFunc("PUT /kv/{key}", kv)
	return mux
}

func run() error {
	id := flag.String("id", "", "node ID")
	httpAddr := flag.String("http", "127.0.0.1:8001", "HTTP listen address")
	peers := flag.String("peers", "", "fixed voters in id=raft-host:port format, comma separated; first ID bootstraps")
	dir := flag.String("data", "data", "persistent node directory")
	flag.Parse()
	var config raft.Configuration
	var local string
	ids, addresses := map[string]bool{}, map[string]bool{}
	for _, peer := range strings.Split(*peers, ",") {
		name, address, ok := strings.Cut(peer, "=")
		if !ok || name == "" || ids[name] || addresses[address] {
			return fmt.Errorf("invalid or duplicate peer %q", peer)
		}
		if _, _, err := net.SplitHostPort(address); err != nil {
			return err
		}
		ids[name], addresses[address] = true, true
		if name == *id {
			local = address
		}
		config.Servers = append(config.Servers, raft.Server{ID: raft.ServerID(name), Address: raft.ServerAddress(address), Suffrage: raft.Voter})
	}
	if local == "" || len(config.Servers) < 3 || len(config.Servers) > 5 {
		return errors.New("supply 3 to 5 peers including this node")
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		return err
	}
	db, err := bolt.NewBoltStore(filepath.Join(*dir, "raft.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	snaps, err := raft.NewFileSnapshotStore(*dir, 2, os.Stderr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(local, nil, 3, time.Second, os.Stderr)
	if err != nil {
		return err
	}
	defer transport.Close()
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(*id)
	cfg.SnapshotThreshold = 128
	cfg.SnapshotInterval = 20 * time.Second
	cfg.TrailingLogs = 32
	existing, err := raft.HasExistingState(db, db, snaps)
	if err != nil {
		return err
	}
	state := &store{values: make(map[string]string)}
	r, err := raft.NewRaft(cfg, state, db, db, snaps, transport)
	if err != nil {
		return err
	}
	defer func() { r.Shutdown().Error() }()
	if !existing && cfg.LocalID == config.Servers[0].ID {
		if err := r.BootstrapCluster(config).Error(); err != nil {
			return err
		}
	}
	server := &http.Server{Addr: *httpAddr, Handler: handler(r, state), ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); server.Close() }()
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
