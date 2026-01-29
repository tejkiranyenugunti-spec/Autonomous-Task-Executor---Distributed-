// Package models defines shared domain types for jobs and subtasks.
package models

import "time"

// Priority levels accepted by the API.
const (
	PriorityHigh   = "high"
	PriorityNormal = "normal"
)

// JobStatus is the coarse lifecycle of a user job.
type JobStatus string

const (
	JobPending    JobStatus = "pending"
	JobRunning    JobStatus = "running"
	JobCompleted  JobStatus = "completed"
	JobFailed     JobStatus = "failed"
	JobCancelled  JobStatus = "cancelled"
	JobDecomposed JobStatus = "decomposed"
)

// SubtaskStatus is tracked by the dispatcher and reported by workers.
type SubtaskStatus string

const (
	SubtaskPending   SubtaskStatus = "pending"
	SubtaskQueued    SubtaskStatus = "queued"
	SubtaskRunning   SubtaskStatus = "running"
	SubtaskCompleted SubtaskStatus = "completed"
	SubtaskFailed    SubtaskStatus = "failed"
	SubtaskCancelled SubtaskStatus = "cancelled"
)

// Job is the user-visible unit of work.
type Job struct {
	ID           string        `json:"id"`
	Instruction  string        `json:"instruction"`
	Priority     string        `json:"priority"`
	Status       JobStatus     `json:"status"`
	CreatedAt    time.Time     `json:"created_at"`
	UpdatedAt    time.Time     `json:"updated_at"`
	SubtaskCount int           `json:"subtask_count"`
	Subtasks     []SubtaskView `json:"subtasks,omitempty"`
}

// SubtaskView is returned to clients (API + dispatcher merge).
type SubtaskView struct {
	ID        string        `json:"id"`
	JobID     string        `json:"job_id"`
	Order     int           `json:"order"`
	Task      string        `json:"task"`
	Status    SubtaskStatus `json:"status"`
	WorkerID  string        `json:"worker_id,omitempty"`
	UpdatedAt time.Time     `json:"updated_at,omitempty"`
}

// SubtaskMessage is serialized to Kafka and RabbitMQ.
type SubtaskMessage struct {
	JobID       string `json:"job_id"`
	SubtaskID   string `json:"subtask_id"`
	Instruction string `json:"instruction"`
	Order       int    `json:"order"`
	Priority    string `json:"priority"`
	Attempt     int    `json:"attempt"`
}

// WorkerRegistration is sent by workers on startup.
type WorkerRegistration struct {
	WorkerID string `json:"worker_id"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Capacity int    `json:"capacity"`
}

// SubtaskStatusReport is POSTed by workers to the dispatcher.
type SubtaskStatusReport struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
