// Package healthcheck polls workers and triggers reroutes through the engine.
package healthcheck

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/autonomous-ate/engine/internal/discovery"
	"github.com/autonomous-ate/engine/internal/dispatcher"
)

// Poller periodically probes worker /health endpoints.
type Poller struct {
	Registry         *discovery.Registry
	Engine           *dispatcher.Engine
	Client           *http.Client
	Interval         time.Duration
	FailureThreshold int
}

// DefaultPollerFromEnv builds a poller using HEALTH_POLL_INTERVAL and HEALTH_FAILURE_THRESHOLD.
func DefaultPollerFromEnv(reg *discovery.Registry, eng *dispatcher.Engine) *Poller {
	iv := 5 * time.Second
	if v := os.Getenv("HEALTH_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			iv = d
		}
	}
	th := 2
	if v := os.Getenv("HEALTH_FAILURE_THRESHOLD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			th = n
		}
	}
	return &Poller{
		Registry:         reg,
		Engine:           eng,
		Client:           &http.Client{Timeout: 3 * time.Second},
		Interval:         iv,
		FailureThreshold: th,
	}
}

// Run blocks until context cancellation.
func (p *Poller) Run(ctx context.Context) {
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *Poller) tick(parent context.Context) {
	for _, w := range p.Registry.Snapshot() {
		id := w.WorkerID()
		url := w.HealthURL()
		preOK := w.OperationalWithThreshold(p.FailureThreshold)

		ctx, cancel := context.WithTimeout(parent, 4*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			cancel()
			continue
		}
		resp, err := p.Client.Do(req)
		httpOK := err == nil && resp != nil && resp.StatusCode == http.StatusOK
		if resp != nil {
			_ = resp.Body.Close()
		}
		cancel()

		p.Registry.RecordPollResult(id, httpOK, p.FailureThreshold)

		w2, exists := p.Registry.Get(id)
		if !exists {
			continue
		}
		postOK := w2.OperationalWithThreshold(p.FailureThreshold)
		if preOK && !postOK {
			p.Engine.MarkWorkerFailed(id)
			n := p.Engine.RerouteWorkerFailures(context.Background(), id)
			log.Printf("healthcheck: worker %s unhealthy, rerouted %d subtasks", id, n)
		}
	}
}
