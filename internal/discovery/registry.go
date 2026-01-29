// Package discovery holds the in-memory worker registry used for routing and health checks.
package discovery

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/autonomous-ate/engine/internal/models"
)

// Worker describes a registered Python worker node.
type Worker struct {
	ID        string
	Host      string
	Port      int
	Capacity  int
	Load      int
	Healthy   bool
	Missed    int
	UpdatedAt time.Time
	mu        sync.RWMutex
}

// HealthURL returns the base URL for worker health probes.
func (w *Worker) HealthURL() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return "http://" + w.Host + ":" + strconv.Itoa(w.Port) + "/health"
}

// Utilization returns active load divided by capacity (0 if capacity unset).
func (w *Worker) Utilization() float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.Capacity <= 0 {
		return 0
	}
	return float64(w.Load) / float64(w.Capacity)
}

// OperationalWithThreshold reports whether the worker is considered up for routing
// before/after a poll given consecutive-miss threshold semantics.
func (w *Worker) OperationalWithThreshold(threshold int) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.Healthy && w.Missed < threshold
}

// WorkerID returns the worker identifier (thread-safe).
func (w *Worker) WorkerID() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ID
}

// Registry is a thread-safe map of workers keyed by worker_id.
type Registry struct {
	mu       sync.RWMutex
	workers  map[string]*Worker
	capacity map[string]int
}

// NewRegistry constructs an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		workers:  make(map[string]*Worker),
		capacity: make(map[string]int),
	}
}

// Upsert registers or updates a worker and marks it healthy on successful registration.
func (r *Registry) Upsert(reg models.WorkerRegistration) *Worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[reg.WorkerID]
	if !ok {
		w = &Worker{ID: reg.WorkerID}
		r.workers[reg.WorkerID] = w
	}
	cap := reg.Capacity
	if cap <= 0 {
		cap = 1
	}
	w.mu.Lock()
	w.Host = reg.Host
	w.Port = reg.Port
	w.Capacity = cap
	w.Healthy = true
	w.Missed = 0
	w.UpdatedAt = time.Now().UTC()
	w.mu.Unlock()
	r.capacity[reg.WorkerID] = cap
	return w
}

// Remove deletes a worker from the registry.
func (r *Registry) Remove(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, workerID)
	delete(r.capacity, workerID)
}

// Snapshot returns a shallow copy list of workers for iteration.
func (r *Registry) Snapshot() []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Worker, 0, len(r.workers))
	for _, w := range r.workers {
		out = append(out, w)
	}
	return out
}

// Get returns a worker by id.
func (r *Registry) Get(workerID string) (*Worker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.workers[workerID]
	return w, ok
}

// IncLoad increments in-flight count for a worker (best effort).
func (r *Registry) IncLoad(workerID string) {
	r.mu.RLock()
	w, ok := r.workers[workerID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	w.mu.Lock()
	w.Load++
	w.mu.Unlock()
}

// DecLoad decrements in-flight count (never below zero).
func (r *Registry) DecLoad(workerID string) {
	r.mu.RLock()
	w, ok := r.workers[workerID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	w.mu.Lock()
	if w.Load > 0 {
		w.Load--
	}
	w.mu.Unlock()
}

// MarkUnhealthy sets healthy=false for a worker.
func (r *Registry) MarkUnhealthy(workerID string) {
	r.mu.RLock()
	w, ok := r.workers[workerID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	w.mu.Lock()
	w.Healthy = false
	w.mu.Unlock()
}

// RecordPollResult updates consecutive miss tracking and health flag.
func (r *Registry) RecordPollResult(workerID string, ok bool, threshold int) {
	r.mu.RLock()
	w, exists := r.workers[workerID]
	r.mu.RUnlock()
	if !exists {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if ok {
		w.Missed = 0
		w.Healthy = true
		w.UpdatedAt = time.Now().UTC()
		return
	}
	w.Missed++
	if w.Missed >= threshold {
		w.Healthy = false
	}
}

// LeastLoadedWorker picks the healthy worker with the lowest utilization ratio.
func (r *Registry) LeastLoadedWorker() (*Worker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var best *Worker
	bestScore := float64(1000)
	for _, w := range r.workers {
		w.mu.RLock()
		healthy := w.Healthy
		load := w.Load
		cap := w.Capacity
		host := w.Host
		w.mu.RUnlock()
		if !healthy || host == "" || cap <= 0 {
			continue
		}
		if load >= cap {
			continue
		}
		score := float64(load) / float64(cap)
		if score < bestScore {
			bestScore = score
			best = w
		}
	}
	return best, best != nil
}

// RegisterHTTP binds registration routes onto a ServeMux-compatible router.
func RegisterHTTP(mux interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}, reg *Registry) {
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body models.WorkerRegistration
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.WorkerID == "" || body.Host == "" || body.Port == 0 {
			http.Error(w, "worker_id, host, port required", http.StatusBadRequest)
			return
		}
		reg.Upsert(body)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"registered"}`))
	})
	mux.HandleFunc("/register/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := r.URL.Path[len("/register/"):]
		if id == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		reg.Remove(id)
		w.WriteHeader(http.StatusNoContent)
	})
}
