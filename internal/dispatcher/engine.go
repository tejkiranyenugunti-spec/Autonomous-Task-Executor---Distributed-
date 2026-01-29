// Package dispatcher contains routing, workload tracking, and orchestration glue.
package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/autonomous-ate/engine/internal/discovery"
	"github.com/autonomous-ate/engine/internal/metrics"
	"github.com/autonomous-ate/engine/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

// SubtaskRecord is dispatcher-authoritative execution state.
type SubtaskRecord struct {
	JobID       string
	SubtaskID   string
	Instruction string
	Order       int
	Priority    string
	Status      models.SubtaskStatus
	WorkerID    string
	Attempt     int
	QueuedAt    time.Time
	StartedAt   time.Time
	UpdatedAt   time.Time
}

// Engine wires Kafka intake, RabbitMQ routing, registry, and status updates.
type Engine struct {
	reg          *discovery.Registry
	rabbitURL    string
	kafkaBrokers []string
	kafkaTopic   string
	kafkaGroup   string

	mu            sync.RWMutex
	subtasks      map[string]*SubtaskRecord // key jobID/subtaskID
	cancelledJobs map[string]struct{}
	completed     map[string]struct{} // jobID/subtaskID terminal dedupe

	amqpConn *amqp.Connection
	amqpMu   sync.Mutex

	recoveryStart map[string]time.Time // workerID -> detection instant
}

func subKey(jobID, subID string) string {
	return jobID + "/" + subID
}

// NewEngine constructs the orchestration engine.
func NewEngine(reg *discovery.Registry, rabbitURL string, kafkaBrokers []string, topic, group string) *Engine {
	return &Engine{
		reg:           reg,
		rabbitURL:     rabbitURL,
		kafkaBrokers:  kafkaBrokers,
		kafkaTopic:    topic,
		kafkaGroup:    group,
		subtasks:      make(map[string]*SubtaskRecord),
		cancelledJobs: make(map[string]struct{}),
		completed:     make(map[string]struct{}),
		recoveryStart: make(map[string]time.Time),
	}
}

// CancelJob marks a job as cancelled; future subtasks are ignored.
func (e *Engine) CancelJob(jobID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancelledJobs[jobID] = struct{}{}
	for _, st := range e.subtasks {
		if st.JobID == jobID && st.Status != models.SubtaskCompleted && st.Status != models.SubtaskFailed {
			st.Status = models.SubtaskCancelled
			st.UpdatedAt = time.Now().UTC()
		}
	}
}

func (e *Engine) isCancelled(jobID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.cancelledJobs[jobID]
	return ok
}

// UpsertFromKafka ingests a subtask message (idempotent on key).
func (e *Engine) UpsertFromKafka(msg models.SubtaskMessage) (*SubtaskRecord, error) {
	if e.isCancelled(msg.JobID) {
		return nil, fmt.Errorf("job cancelled")
	}
	key := subKey(msg.JobID, msg.SubtaskID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, done := e.completed[key]; done {
		return e.subtasks[key], nil
	}
	now := time.Now().UTC()
	if existing, ok := e.subtasks[key]; ok {
		existing.Instruction = msg.Instruction
		existing.Order = msg.Order
		existing.Priority = msg.Priority
		existing.Attempt = msg.Attempt
		existing.UpdatedAt = now
		return existing, nil
	}
	rec := &SubtaskRecord{
		JobID:       msg.JobID,
		SubtaskID:   msg.SubtaskID,
		Instruction: msg.Instruction,
		Order:       msg.Order,
		Priority:    msg.Priority,
		Status:      models.SubtaskPending,
		Attempt:     msg.Attempt,
		QueuedAt:    now,
		UpdatedAt:   now,
	}
	e.subtasks[key] = rec
	return rec, nil
}

func (e *Engine) ensureAMQP() (*amqp.Connection, error) {
	e.amqpMu.Lock()
	defer e.amqpMu.Unlock()
	if e.amqpConn != nil && !e.amqpConn.IsClosed() {
		return e.amqpConn, nil
	}
	conn, err := amqp.Dial(e.rabbitURL)
	if err != nil {
		return nil, err
	}
	e.amqpConn = conn
	return conn, nil
}

// PublishToWorker pushes a serialized subtask to the worker-specific queue.
func (e *Engine) PublishToWorker(ctx context.Context, workerID string, body []byte) error {
	conn, err := e.ensureAMQP()
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	q := "worker." + workerID
	if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", q, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// RouteSubtask picks the least-loaded healthy worker and enqueues the subtask.
func (e *Engine) RouteSubtask(ctx context.Context, msg models.SubtaskMessage) error {
	if e.isCancelled(msg.JobID) {
		return fmt.Errorf("job cancelled")
	}
	if _, err := e.UpsertFromKafka(msg); err != nil {
		return err
	}
	w, ok := e.reg.LeastLoadedWorker()
	if !ok {
		return fmt.Errorf("no healthy workers available")
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := e.PublishToWorker(ctx, w.ID, payload); err != nil {
		return err
	}
	now := time.Now().UTC()
	e.mu.Lock()
	key := subKey(msg.JobID, msg.SubtaskID)
	if r := e.subtasks[key]; r != nil {
		r.Status = models.SubtaskQueued
		r.WorkerID = w.ID
		r.UpdatedAt = now
	}
	e.mu.Unlock()
	e.reg.IncLoad(w.ID)
	metrics.QueueDepth.WithLabelValues(e.kafkaTopic).Inc()
	return nil
}

// HandleStatus updates subtask progress reported by workers.
func (e *Engine) HandleStatus(jobID, subtaskID string, rep models.SubtaskStatusReport) error {
	key := subKey(jobID, subtaskID)
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, ok := e.subtasks[key]
	if !ok {
		return fmt.Errorf("unknown subtask")
	}
	switch rep.Status {
	case "running":
		rec.Status = models.SubtaskRunning
		rec.StartedAt = time.Now().UTC()
	case "completed":
		if _, dup := e.completed[key]; dup {
			return nil
		}
		e.completed[key] = struct{}{}
		rec.Status = models.SubtaskCompleted
		rec.UpdatedAt = time.Now().UTC()
		if rec.WorkerID != "" {
			e.reg.DecLoad(rec.WorkerID)
		}
		metrics.QueueDepth.WithLabelValues(e.kafkaTopic).Dec()
		if !rec.QueuedAt.IsZero() {
			metrics.TaskCompletionSeconds.WithLabelValues(rec.Priority).Observe(time.Since(rec.QueuedAt).Seconds())
		}
	case "failed":
		rec.Status = models.SubtaskFailed
		rec.UpdatedAt = time.Now().UTC()
		if rec.WorkerID != "" {
			e.reg.DecLoad(rec.WorkerID)
		}
		metrics.QueueDepth.WithLabelValues(e.kafkaTopic).Dec()
		metrics.FailureCount.WithLabelValues("subtask").Inc()
	default:
		return fmt.Errorf("invalid status")
	}
	return nil
}

// JobState returns merged subtask records for API consumption.
func (e *Engine) JobState(jobID string) ([]models.SubtaskView, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []models.SubtaskView
	found := false
	for _, st := range e.subtasks {
		if st.JobID != jobID {
			continue
		}
		found = true
		out = append(out, models.SubtaskView{
			ID:        st.SubtaskID,
			JobID:     st.JobID,
			Order:     st.Order,
			Task:      st.Instruction,
			Status:    st.Status,
			WorkerID:  st.WorkerID,
			UpdatedAt: st.UpdatedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Order < out[j].Order })
	return out, found
}

// RefreshUtilizationMetrics updates gauges from registry snapshot.
func (e *Engine) RefreshUtilizationMetrics() {
	for _, w := range e.reg.Snapshot() {
		metrics.WorkerUtilization.WithLabelValues(w.WorkerID()).Set(w.Utilization())
	}
}

// MarkWorkerFailed records the detection instant for recovery histograms.
func (e *Engine) MarkWorkerFailed(workerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.recoveryStart[workerID]; !ok {
		e.recoveryStart[workerID] = time.Now()
	}
}

// RerouteWorkerFailures moves in-flight work away from an unhealthy worker.
func (e *Engine) RerouteWorkerFailures(ctx context.Context, failedWorkerID string) int {
	e.mu.Lock()
	var pending []models.SubtaskMessage
	now := time.Now().UTC()
	for _, st := range e.subtasks {
		if st.WorkerID != failedWorkerID {
			continue
		}
		if st.Status != models.SubtaskQueued && st.Status != models.SubtaskRunning {
			continue
		}
		key := subKey(st.JobID, st.SubtaskID)
		if _, done := e.completed[key]; done {
			continue
		}
		if st.WorkerID != "" {
			e.reg.DecLoad(st.WorkerID)
		}
		st.Attempt++
		msg := models.SubtaskMessage{
			JobID:       st.JobID,
			SubtaskID:   st.SubtaskID,
			Instruction: st.Instruction,
			Order:       st.Order,
			Priority:    st.Priority,
			Attempt:     st.Attempt,
		}
		pending = append(pending, msg)
		st.Status = models.SubtaskPending
		st.WorkerID = ""
		st.UpdatedAt = now
	}
	start, haveStart := e.recoveryStart[failedWorkerID]
	e.mu.Unlock()

	if len(pending) == 0 {
		return 0
	}
	metrics.FailureCount.WithLabelValues("worker_health").Inc()
	rerouted := 0
	for _, msg := range pending {
		if err := e.RouteSubtask(ctx, msg); err != nil {
			log.Printf("reroute failed: %v", err)
			continue
		}
		rerouted++
	}
	if haveStart && rerouted > 0 {
		metrics.RecoverySeconds.WithLabelValues(failedWorkerID).Observe(time.Since(start).Seconds())
	}
	e.mu.Lock()
	delete(e.recoveryStart, failedWorkerID)
	e.mu.Unlock()
	return rerouted
}

// RunKafkaConsumer blocks processing the subtasks topic until ctx is cancelled.
func (e *Engine) RunKafkaConsumer(ctx context.Context) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: e.kafkaBrokers,
		Topic:   e.kafkaTopic,
		GroupID: e.kafkaGroup,
	})
	defer r.Close()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("kafka fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var msg models.SubtaskMessage
		if err := json.Unmarshal(m.Value, &msg); err != nil {
			log.Printf("bad kafka message: %v", err)
			if err := r.CommitMessages(ctx, m); err != nil {
				log.Printf("kafka commit: %v", err)
			}
			continue
		}
		if e.isCancelled(msg.JobID) {
			if err := r.CommitMessages(ctx, m); err != nil {
				log.Printf("kafka commit: %v", err)
			}
			continue
		}
		if err := e.RouteSubtask(ctx, msg); err != nil {
			log.Printf("route error (will retry after backoff): %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("kafka commit: %v", err)
		}
	}
}
