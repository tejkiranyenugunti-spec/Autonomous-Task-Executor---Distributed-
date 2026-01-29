package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/autonomous-ate/engine/internal/discovery"
	"github.com/autonomous-ate/engine/internal/dispatcher"
	"github.com/autonomous-ate/engine/internal/healthcheck"
	"github.com/autonomous-ate/engine/internal/models"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	addr := getenv("HTTP_ADDR", ":8081")
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:29092"), ",")
	topic := getenv("KAFKA_TOPIC_SUBTASKS", "subtasks")
	group := getenv("KAFKA_GROUP_ID", "ate-dispatcher")
	rabbitURL := getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	reg := discovery.NewRegistry()
	eng := dispatcher.NewEngine(reg, rabbitURL, brokers, topic, group)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := eng.RunKafkaConsumer(ctx); err != nil && err != context.Canceled {
			log.Printf("kafka consumer exited: %v", err)
		}
	}()

	poller := healthcheck.DefaultPollerFromEnv(reg, eng)
	go poller.Run(ctx)

	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				eng.RefreshUtilizationMetrics()
			}
		}
	}()

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle("/metrics", promhttp.Handler())

	r.Post("/register", func(w http.ResponseWriter, r *http.Request) {
		var body models.WorkerRegistration
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if body.WorkerID == "" || body.Host == "" || body.Port == 0 {
			http.Error(w, "worker_id, host, port required", http.StatusBadRequest)
			return
		}
		if body.Capacity <= 0 {
			body.Capacity = 4
		}
		reg.Upsert(body)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"registered"}`))
	})

	r.Delete("/register/{id}", func(w http.ResponseWriter, r *http.Request) {
		reg.Remove(chi.URLParam(r, "id"))
		w.WriteHeader(http.StatusNoContent)
	})

	r.Post("/jobs/{jobID}/subtasks/{subtaskID}/status", func(w http.ResponseWriter, r *http.Request) {
		jobID := chi.URLParam(r, "jobID")
		subID := chi.URLParam(r, "subtaskID")
		var rep models.SubtaskStatusReport
		if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := eng.HandleStatus(jobID, subID, rep); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	r.Get("/jobs/{id}/state", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		subs, _ := eng.JobState(id)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"subtasks": subs})
	})

	r.Post("/internal/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		eng.CancelJob(chi.URLParam(r, "id"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"cancelled"}`))
	})

	log.Printf("dispatcher listening on %s", addr)
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
