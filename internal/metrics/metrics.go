// Package metrics registers Prometheus collectors for the execution engine.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	QueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Approximate dispatcher view of pending+queued subtasks (use kafka_exporter for broker lag).",
		},
		[]string{"topic"},
	)

	WorkerUtilization = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "worker_utilization",
			Help: "Active subtasks divided by declared worker capacity.",
		},
		[]string{"worker_id"},
	)

	TaskCompletionSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "task_completion_time",
			Help:    "Wall time from enqueue to completion for a subtask.",
			Buckets: prometheus.ExponentialBuckets(0.25, 2, 12),
		},
		[]string{"priority"},
	)

	FailureCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failure_count",
			Help: "Failures detected (worker health or subtask execution).",
		},
		[]string{"reason"},
	)

	RecoverySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "recovery_time",
			Help:    "Time from worker failure detection to successful reroute publish.",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60},
		},
		[]string{"worker_id"},
	)
)
