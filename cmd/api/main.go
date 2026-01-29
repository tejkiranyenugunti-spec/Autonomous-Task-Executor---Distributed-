package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/autonomous-ate/engine/internal/decomposer"
	"github.com/autonomous-ate/engine/internal/models"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type jobStore struct {
	mu   sync.RWMutex
	jobs map[string]*models.Job
}

func (s *jobStore) put(j *models.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
}

func (s *jobStore) get(id string) (*models.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

func (s *jobStore) list() []*models.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*models.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}

func main() {
	addr := getenv("HTTP_ADDR", ":8080")
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:29092"), ",")
	topic := getenv("KAFKA_TOPIC_SUBTASKS", "subtasks")
	dispatcherURL := strings.TrimRight(getenv("DISPATCHER_URL", "http://localhost:8081"), "/")

	store := &jobStore{jobs: make(map[string]*models.Job)}
	if err := ensureTopic(brokers, topic); err != nil {
		log.Printf("warn: ensure topic: %v", err)
	}
	kw := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
	}
	defer kw.Close()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle("/metrics", promhttp.Handler())

	r.Post("/jobs", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Instruction string `json:"instruction"`
			Priority    string `json:"priority"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		body.Instruction = strings.TrimSpace(body.Instruction)
		if body.Instruction == "" {
			http.Error(w, "instruction required", http.StatusBadRequest)
			return
		}
		if body.Priority == "" {
			body.Priority = models.PriorityNormal
		}
		if body.Priority != models.PriorityHigh && body.Priority != models.PriorityNormal {
			http.Error(w, "priority must be high or normal", http.StatusBadRequest)
			return
		}
		id := newJobID()
		msgs := decomposer.Decompose(id, body.Instruction, body.Priority)
		now := time.Now().UTC()
		job := &models.Job{
			ID:           id,
			Instruction:  body.Instruction,
			Priority:     body.Priority,
			Status:       models.JobRunning,
			CreatedAt:    now,
			UpdatedAt:    now,
			SubtaskCount: len(msgs),
		}
		store.put(job)
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		for _, m := range msgs {
			b, err := json.Marshal(m)
			if err != nil {
				http.Error(w, "encode error", http.StatusInternalServerError)
				return
			}
			if err := kw.WriteMessages(ctx, kafka.Message{Key: []byte(m.SubtaskID), Value: b}); err != nil {
				http.Error(w, "kafka write: "+err.Error(), http.StatusBadGateway)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(job)
	})

	r.Get("/jobs", func(w http.ResponseWriter, r *http.Request) {
		items := store.list()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(items)
	})

	r.Get("/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		job, ok := store.get(id)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		st, err := fetchDispatcherState(r.Context(), dispatcherURL, id)
		if err != nil {
			log.Printf("dispatcher merge failed: %v", err)
		} else {
			job.Subtasks = st
			job.SubtaskCount = len(st)
			job.Status = aggregateJobStatus(job.Status, st)
			job.UpdatedAt = time.Now().UTC()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(job)
	})

	r.Delete("/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		job, ok := store.get(id)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err := callDispatcherCancel(r.Context(), dispatcherURL, id); err != nil {
			log.Printf("dispatcher cancel: %v", err)
		}
		job.Status = models.JobCancelled
		job.UpdatedAt = time.Now().UTC()
		w.WriteHeader(http.StatusNoContent)
	})

	log.Printf("api listening on %s", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatal(err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func newJobID() string {
	return "job-" + time.Now().UTC().Format("20060102150405") + "-" + strconv.FormatInt(time.Now().UnixNano()%1_000_000, 36)
}

func ensureTopic(brokers []string, topic string) error {
	if len(brokers) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", strings.TrimSpace(brokers[0]))
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	}); err != nil {
		// Topic may already exist depending on broker/kafka-go version.
		log.Printf("ensureTopic: %v", err)
	}
	return nil
}

type dispatcherStateResp struct {
	Subtasks []models.SubtaskView `json:"subtasks"`
}

func fetchDispatcherState(ctx context.Context, base, jobID string) ([]models.SubtaskView, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/jobs/"+jobID+"/state", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dispatcher status %d", resp.StatusCode)
	}
	var body dispatcherStateResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Subtasks, nil
}

func callDispatcherCancel(ctx context.Context, base, jobID string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/jobs/"+jobID+"/cancel", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("dispatcher cancel status %d", resp.StatusCode)
	}
	return nil
}

func aggregateJobStatus(current models.JobStatus, subs []models.SubtaskView) models.JobStatus {
	if current == models.JobCancelled {
		return models.JobCancelled
	}
	if len(subs) == 0 {
		return current
	}
	var completed, failed, cancelled int
	for _, s := range subs {
		switch s.Status {
		case models.SubtaskCompleted:
			completed++
		case models.SubtaskFailed:
			failed++
		case models.SubtaskCancelled:
			cancelled++
		}
	}
	if failed > 0 {
		return models.JobFailed
	}
	if cancelled == len(subs) {
		return models.JobCancelled
	}
	if completed == len(subs) {
		return models.JobCompleted
	}
	return models.JobRunning
}
