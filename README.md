![Go](https://img.shields.io/badge/Go-1.21-00ADD8?logo=go&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-FF6600?logo=rabbitmq&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=white)
![Gemini](https://img.shields.io/badge/Gemini-AI-4285F4?logo=google&logoColor=white)

# Autonomous Task Execution Engine

**Tagline:** A distributed AI agent execution system that accepts plain English instructions, decomposes them into subtasks using **Gemini AI**, executes them across worker nodes autonomously, and **self-recovers from failures** without human intervention.

---

## Architecture

```
User Instruction (CLI)
        │
        ▼
   Go REST API
        │
        ▼
 Gemini AI Decomposer
  (gemini-1.5-flash)
        │
        ▼
    Kafka Queue
   (topic: subtasks)
        │
        ▼
   Go Dispatcher
  (workload-aware)
        │
        ▼
   RabbitMQ Router
   (per-worker queues)
     │      │      │
     ▼      ▼      ▼
 Worker1 Worker2 Worker3
 (Python; K8s-ready pods)
        │
        ▼
 Health-Check Service
 (5s poll; auto-reroute
  on worker failure)
        │
        ▼
 Prometheus + Grafana
 (real-time observability)
```

---

## How it works

- You submit a **plain English** instruction via the **Python CLI** to the **Go REST API**.
- **Gemini 1.5 Flash** decomposes the instruction into an **ordered list of structured subtasks** (JSON), which are published to **Kafka**.
- The **Go dispatcher** consumes subtasks, maintains a **worker registry** (service discovery), and routes each subtask to the **least-loaded** worker over **RabbitMQ** (one queue per worker).
- A **health poller** hits every worker’s `/health` every **5 seconds**; after **two consecutive failures**, the worker is marked unhealthy and **in-flight subtasks are rerouted** to healthy workers (detection is poll-bound; reroute itself is immediate once unhealthy).
- **Prometheus** scrapes API and dispatcher metrics; **Grafana** dashboards visualize queue depth, utilization, completion latency, failures, and recovery timing.

---

## Tech stack

| Layer | Technology |
|--------|------------|
| Task planner | **Gemini 1.5 Flash** (Google AI, `generative-ai-go`) |
| API + dispatcher | **Go 1.21** (Chi, Kafka consumer, RabbitMQ publisher) |
| Job queue | **Apache Kafka** |
| Task routing | **RabbitMQ** (durable queues `worker.<id>`) |
| Worker nodes | **Python 3.11** (aio-pika, FastAPI, httpx) |
| Packaging / local orchestration | **Docker** + **Docker Compose** |
| Production-style deploy | **Kubernetes** manifests (`k8s/`) |
| Observability | **Prometheus** + **Grafana** |
| CLI | **Python Click** |

---

## Quick start

**Prerequisites:** Docker, Docker Compose, Python 3.11, Go 1.21

### 1. Clone and configure

```bash
git clone <your-repo-url>
cd autonomous-task-executor
```

Create a `.env` in the project root (Compose loads it automatically):

```bash
echo 'GEMINI_API_KEY=your_key_here' > .env
```

*(If you maintain a `.env.example` in your fork, use `cp .env.example .env` and fill in `GEMINI_API_KEY`.)*

### 2. Resolve modules and run the stack

```bash
go mod tidy
docker compose up --build
```

Services: Zookeeper, Kafka, RabbitMQ, API `:8080`, dispatcher `:8081`, three workers, Prometheus `:9090`, Grafana `:3000`, Kafka exporter `:9308`.

### 3. Install CLI dependencies (host)

```bash
python3.11 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r cli/requirements.txt
export ATE_API_URL=http://localhost:8080
```

### 4. Submit a job

```bash
python cli/main.py submit \
  --instruction "Validate all healthcare records, flag anomalies, and generate a compliance report" \
  --priority high
```

### 5. Inspect state

```bash
python cli/main.py status --job-id <id>
python cli/main.py list
python cli/main.py cancel --job-id <id>
```

---

## AI-powered task planning

1. Set **`GEMINI_API_KEY`** in `.env` (see Quick start). The API reads it and the **`internal/decomposer`** package calls Gemini with **`ResponseMIMEType: application/json`** for structured output.
2. **Example input (plain English):**  
   `"Book a dentist appointment for next Tuesday at 3pm"`
3. **Example shape of Gemini output** (illustrative; IDs and wording vary per run):

```json
[
  {
    "id": "1",
    "name": "Search for dentists",
    "description": "Find available dentists near location",
    "order": 1,
    "estimated_duration_seconds": 10
  },
  {
    "id": "2",
    "name": "Check availability",
    "description": "Query available slots for Tuesday 3pm",
    "order": 2,
    "estimated_duration_seconds": 8
  },
  {
    "id": "3",
    "name": "Book appointment",
    "description": "Submit booking form and confirm",
    "order": 3,
    "estimated_duration_seconds": 12
  }
]
```

4. Subtasks are converted to **`SubtaskMessage`** records (Kafka/RabbitMQ) with a single **`instruction`** string per step for workers.
5. **Silent fallback:** If `GEMINI_API_KEY` is unset, or the Gemini call fails, or JSON is invalid, the engine **falls back to the heuristic manual decomposer** (semicolons, newlines, ` and `, etc.). **The API does not crash** on LLM failure.

---

## Self-recovery demo

**1.** Start the stack:

```bash
docker compose up --build
```

**2.** Submit a multi-step job (more subtasks = easier to catch a worker mid-flight):

```bash
python cli/main.py submit \
  --instruction "Process 1000 records and generate report" \
  --priority high
```

**3.** Stop a worker while the job runs (from another terminal, project root):

```bash
docker compose stop worker-1
```

*Container names depend on your Compose project name (often the directory name). Use `docker compose ps` to confirm service names.*

**4.** Watch the dispatcher logs:

```bash
docker compose logs -f dispatcher
```

You should see a line in this form (from the health poller when rerouting succeeds):

```text
healthcheck: worker worker-1 unhealthy, rerouted N subtasks
```

**5.** Confirm the job still converges:

```bash
python cli/main.py status --job-id <id>
```

**6.** Optional: bring the worker back:

```bash
docker compose start worker-1
```

---

## Metrics and observability

| URL | Purpose |
|-----|---------|
| `http://localhost:9090` | **Prometheus** UI |
| `http://localhost:3000` | **Grafana** (default `admin` / `admin` in Compose) |
| `http://localhost:8080/metrics` | API Prometheus endpoint |
| `http://localhost:8081/metrics` | Dispatcher Prometheus endpoint |
| `http://localhost:9308/metrics` | **Kafka exporter** (consumer lag, etc.) |

**Dispatcher metrics:**

| Concept | Prometheus metric |
|--------|---------------------|
| Pending / in-flight subtasks (dispatcher view) | `queue_depth` |
| Active load vs capacity | `worker_utilization` |
| Subtask completion latency | `task_completion_time` (histogram; `_bucket` / `_sum` / `_count`) |
| Failures (health vs subtask) | `failure_count` |
| Detection → successful reroute | `recovery_time` (histogram) |

Grafana provisioning lives under `monitoring/`; Prometheus scrape config targets API, dispatcher, and Kafka exporter.

---

## Project structure

```
.
├── cmd/
│   ├── api/main.go              # REST API: POST/GET/DELETE /jobs, Kafka produce, health, metrics
│   └── dispatcher/main.go       # Kafka consumer, RabbitMQ router, registration HTTP, health poller
├── internal/
│   ├── decomposer/
│   │   ├── decomposer.go        # Entry: Gemini path + manual fallback, heuristic splitter
│   │   └── llm.go               # Gemini client, JSON parse, normalize → SubtaskMessage
│   ├── discovery/registry.go  # In-memory worker registry (load, health flags)
│   ├── dispatcher/engine.go   # Routing, reroute on failure, Kafka loop, state
│   ├── healthcheck/poller.go    # 5s HTTP /health probes, trigger reroute
│   ├── metrics/metrics.go       # Prometheus counters/histograms/gauges
│   └── models/models.go         # Job, SubtaskMessage, shared DTOs
├── workers/
│   ├── worker.py                # RabbitMQ consumer, FastAPI /health, dispatcher callbacks
│   └── requirements.txt
├── cli/
│   ├── main.py                  # Click CLI: submit, status, list, cancel
│   └── requirements.txt
├── k8s/                         # Example Kafka, RabbitMQ, worker Deployment/Service YAML
├── monitoring/
│   ├── prometheus.yml           # Scrape targets
│   ├── grafana-dashboard.json   # Pre-built dashboard JSON
│   ├── grafana-datasources.yml  # Provisioning: Prometheus datasource
│   └── grafana-dashboards.yml   # Dashboard loader config
├── docker-compose.yml           # Full local stack (Kafka, RabbitMQ, API, workers, observability)
├── Dockerfile.go                # Multi-stage build for Go binaries
├── Dockerfile.worker            # Python worker image
├── go.mod / go.sum              # Go module definition
└── README.md                    # This file
```

---

## Why this project exists

Built to explore how **AI agents** can autonomously execute **multi-step workflows** without standing human supervision. The architecture mirrors what production **agentic** systems need: durable queues, workload-aware routing, health-aware failover, and first-class observability so failures are visible and recoverable end-to-end.
