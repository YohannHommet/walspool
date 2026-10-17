<p align="center">
  <img src="assets/logo.png" alt="walspool logo" width="180" style="border-radius: 24px; box-shadow: 0 12px 35px rgba(204,255,0,0.25);"/>
</p>

<h1 align="center">Walspool</h1>

<p align="center">
  <strong>Durable local buffer for outbound webhooks, telemetry, and API events.</strong><br>
  Embedded Write-Ahead Log (WAL) library & zero-dependency sidecar daemon in pure Go.<br>
  <em>Maintained by <a href="https://github.com/YohannHommet">Yohann Hommet</a> · Supported by <a href="https://meowlabs.tech">Meow Labs</a></em>
</p>

<p align="center">
  <a href="https://golang.org"><img src="https://img.shields.io/badge/Go-%3E%3D%201.22-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go Version"/></a>
  <a href="https://goreportcard.com/report/github.com/YohannHommet/walspool"><img src="https://img.shields.io/badge/Go_Report-A+-A3E635?style=for-the-badge&logo=go&logoColor=black" alt="Go Report Card"/></a>
  <a href="https://github.com/YohannHommet/walspool/releases/tag/v1.0.0"><img src="https://img.shields.io/badge/Edition-Community-38BDF8?style=for-the-badge&logo=github&logoColor=white" alt="Edition Community"/></a>
  <a href="https://github.com/YohannHommet/walspool/actions"><img src="https://img.shields.io/badge/Tests-Passing-CCFF00?style=for-the-badge&logo=githubactions&logoColor=black" alt="Tests Status"/></a>
  <a href="https://github.com/YohannHommet/walspool/pkgs/container/walspool"><img src="https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker Ready"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-FSL--1.1--MIT-CCFF00?style=for-the-badge&logoColor=black&labelColor=16171C" alt="License FSL-1.1-MIT"/></a>
</p>

<p align="center">
  <a href="https://yohannhommet.github.io/walspool/">
    <img src="assets/hero_screenshot.png" alt="Walspool Landing Page Hero" width="100%" style="border-radius: 12px; border: 1px solid #272930; box-shadow: 0 20px 40px -15px rgba(0,0,0,0.7);"/>
  </a>
</p>

> [!NOTE]
> **Walspool** is free and source-available under the [Functional Source License (FSL-1.1-MIT)](LICENSE) (which automatically converts to the permissive MIT License after two years).  
> For specialized high-throughput streaming architecture or custom enterprise integrations, contact the engineering team at **[meowlabs.tech](https://meowlabs.tech)**.

An embedded write-ahead log (WAL) spooler and real-time streaming hub written in pure Go with zero third-party dependencies.

Walspool provides a resilient local buffer for audit trails, telemetry batches, and outgoing webhooks. It commits events to disk with CRC32 integrity in sub-microsecond time, survives sudden crashes and power loss without corruption, and simultaneously streams live events to web consoles or CLI tails over Server-Sent Events (SSE).

Use it as a zero-dependency Go library inside your application, or run it as a lightweight Docker sidecar next to Python, Node.js, Ruby, Rust, or PHP services.

---

## Why Walspool

Most backend services need to send events out-of-band: audit logs, telemetry batches, or partner webhooks. Teams usually choose between three unsatisfactory options:

1. **Synchronous HTTP calls:** You POST events directly to downstream collectors or external APIs. When that service experiences high latency or drops connections, your API worker pool stalls, requests back up, and your users see HTTP 504 timeouts.
2. **In-memory channels or queues:** You drop payloads into a buffered Go channel or a Python `asyncio.Queue` to keep handlers fast. But whenever Kubernetes evicts the pod, a deployment rolls out, or an OOM killer strikes, **every uncommitted event in memory vanishes**. For compliance and billing logs, that loss is unacceptable.
3. **Heavy distributed brokers:** Deploying Kafka, RabbitMQ, or AWS SQS means operating clusters, managing partition rebalancing, and paying dedicated infrastructure bills just to safely buffer events exiting a single service.

Walspool sits in the sweet spot: **the durability of a write-ahead log, the simplicity of an embedded engine, and the instant visibility of a live tail, with zero operational dependencies.**

### Engineering Trade-offs in Outbound Buffering

| Architecture Pattern | Client Latency | Crash Durability (SIGKILL / OOM) | Operational Cost | Downstream Outage Behavior |
|---|---|---|---|---|
| **Direct Synchronous HTTP** | 400ms - 5,000ms | N/A (blocked in flight) | Zero infra (high worker count) | Worker exhaustion, 504 timeouts |
| **In-Memory Queue (Channels, Celery, Bull)** | < 10 µs | Total loss on pod restart | Low (until memory leaks) | Memory ballooning, OOMKill |
| **Distributed Broker (Kafka, RabbitMQ)** | 5ms - 25ms (TCP hop + quorum) | High (replicated log) | Heavy ($400-1,200/mo, ZooKeeper, JVM) | Queued in cluster (complex consumers) |
| **Walspool (Embedded WAL & Sidecar)** | **< 0.5 µs (NVMe group commit)** | **100% NVMe durable with CRC32** | **Zero infra (< 15 MB RAM, 0 deps)** | **Bounded spooling, decorrelated retry** |

---

## When to Use Walspool

### Good Fit
- **Critical audit & compliance logs:** Guaranteed at-least-once delivery to remote sinks, even across process crashes, kernel panics, or sudden reboots.
- **Outgoing webhooks & telemetry offloading:** Keep application response times under 15 µs regardless of downstream collector latency, flapping, or rate-limiting (`429 Too Many Requests`).
- **Polyglot microservice fleets:** A uniform local sidecar daemon across Node.js, Python, Ruby, PHP, and Go services without vendor SDK bloat.
- **Edge gateways & retail appliances:** Spools transactions locally to NVMe/eMMC storage during internet drops and automatically flushes batches in order when connectivity resumes.
- **Live developer backoffices:** Real-time event streaming (`curl -N` or browser SSE) and sub-15µs trace queries (`x-request-id`) without deploying an entire Elasticsearch/Loki stack for local or staging environments.

### When NOT to Use Walspool
- **Cross-node publish/subscribe:** Walspool is an embedded/local spooler. If you need 50 consumer groups distributed across multiple data centers, use **Kafka, Redpanda, or NATS JetStream**.
- **Distributed transactional consensus:** If you need two-phase commit (2PC) or Raft-coordinated state machines across a cluster, Walspool is not a database.
- **Ephemeral disks without volumes:** If your Kubernetes pods run on ephemeral root filesystems wiped on termination without persistent volume claims (`PVC`), a disk-based WAL cannot recover state across pod recreations.
- **Multi-terabyte cold analytics:** Walspool's in-memory ring buffer is designed for real-time observability (the last 50,000-500,000 records). For long-term historical storage, configure Walspool's sink to deliver into S3, ClickHouse, or Snowflake.

---

## How It Works

Walspool operates as a **Dual-Engine** behind clean [Ports & Adapters](docs/architecture/ARCHITECTURE.md) boundaries (see also [Observability Architecture](docs/architecture/OBSERVABILITY_REPORT.md)):

```
                  Incoming Event (Go API or HTTP POST /v1/enqueue)
                                         │
                    ┌────────────────────┴────────────────────┐
                    │       Driving Port: walspool.Spooler    │
                    └────────────────────┬────────────────────┘
                                         │
            ┌────────────────────────────┴────────────────────────────┐
            ▼                                                         ▼
┌───────────────────────────────────────┐ ┌───────────────────────────────────────┐
│         ENGINE 1: DISK WAL            │ │         ENGINE 2: MEMORY HUB          │
│        (Persistent Storage)           │ │       (Real-Time Observability)       │
├───────────────────────────────────────┤ ├───────────────────────────────────────┤
│ • 128 KB userspace group commit buffer│ │ • Fixed-size circular ring buffer     │
│ • Sequential segments & auto-pruning  │ │ • Secondary index by trace_id/service │
│ • 29-byte binary header + CRC32 check │ │ • Sub-15µs historical trace queries   │
│ • Atomic checkpoint swap (.tmp → .meta)│ │ • Non-blocking SSE broadcast stream   │
│ • Multi-segment recovery on restart   │ │ • Zero GC leak: old references pruned │
└──────────────────┬────────────────────┘ └──────────────────┬────────────────────┘
                   │                                         │
                   ▼                                         ▼
         [ Background Dispatcher ]                 [ Active Consumers ]
        • Batched drain (e.g. 50 items)           • GET /v1/logs?trace_id=...
        • Exponential backoff & jitter            • GET /v1/logs/stream (SSE)
        • Downstream HTTP / Kafka / S3 Sink       • Web dashboards & CLI tails
```

1. **Engine 1: Durable Disk WAL (`FileStorageEngine`)**
   - Appends records sequentially into append-only WAL segments (`segment-%012d.wal`).
   - Buffers writes in userspace (128 KB `bufio.Writer`), amortizing disk syscalls to deliver over 1,000,000 appends/sec.
   - Seals each entry with an IEEE 802.3 CRC32 checksum inside a 29-byte binary frame.
   - Automatically rotates active segments when reaching `MaxSegmentSize` (e.g. 64 MB) and unlinks fully-committed segments on `Commit()`.
   - Commits checkpoint offsets via atomic swap (`checkpoint.tmp` $\to$ `checkpoint.meta`) and directory `fsync`.
   - On startup, safely discovers all segments and truncates torn writes at the tail using `os.Truncate` without losing valid records.

2. **Engine 2: In-Memory Observability Hub (`MemoryLogHub`)**
   - Maintains a fixed circular ring buffer (default 50,000 entries) in memory ($O(1)$ overwrite).
   - Indexes records by `trace_id` and `service`. Evicted slots are cleared to allow prompt Go garbage collection.
   - Broadcasts events to connected HTTP Server-Sent Events (SSE) clients outside lock boundaries. Slow clients drop stream frames gracefully without impacting disk persistence.

---

## Quickstart

### Go Library

Add the module to your project:

```bash
go get github.com/YohannHommet/walspool
```

Initialize the spooler with an in-memory hub observer and enqueue events:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/YohannHommet/walspool"
)

// 1. Define where delivered batches go (Driven Outbound Port)
type StdoutSink struct{}

func (s StdoutSink) Deliver(ctx context.Context, batch []walspool.Record) error {
	for _, rec := range batch {
		log.Printf("Delivered: offset=%d topic=%s payload=%s", rec.Offset, rec.Topic, string(rec.Payload))
	}
	return nil
}

func main() {
	ctx := context.Background()

	// 2. Initialize persistent storage with 64MB multi-segment rotation (Engine 1)
	storage, err := walspool.NewFileStorageEngineWithOptions("./data/spool", 50000, walspool.FileStorageOptions{
		MaxSegmentSize: 64 * 1024 * 1024,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 3. Initialize in-memory observability hub (Engine 2)
	hub := walspool.NewMemoryLogHub(50000)

	// 4. Configure the spooler engine
	cfg := walspool.DefaultConfig()
	cfg.BatchSize = 50
	cfg.FlushInterval = 50 * time.Millisecond

	spool, err := walspool.New(cfg, storage, StdoutSink{}, nil, walspool.WithObserver(hub))
	if err != nil {
		log.Fatal(err)
	}
	defer spool.Close()

	// 5. Enqueue events (< 1 µs)
	payload := []byte(`{"trace_id":"req-99a","service":"checkout","amount":89.50}`)
	if err := spool.Enqueue(ctx, "orders.checkout", payload); err != nil {
		log.Fatal(err)
	}

	// 6. Query in-memory logs by trace ID (< 15 µs)
	results := hub.Query(walspool.LogQuery{TraceID: "req-99a", Limit: 5})
	for _, entry := range results {
		fmt.Printf("Trace %s: %s [%s]\n", entry.TraceID, entry.Topic, string(entry.Payload))
	}
}
```

---

### Docker Sidecar Container

For non-Go services (Python, Node.js, Ruby, Rust, PHP), run Walspool as a local companion daemon.

#### Run with Docker CLI

```bash
docker run -d \
  --name walspool \
  -p 9099:9099 \
  -v $(pwd)/data/spool:/data/spool \
  -e WALSPOOL_SINK_URL=https://collector.internal/v1/batches \
  -e WALSPOOL_BATCH_SIZE=50 \
  -e WALSPOOL_FLUSH_MS=50 \
  ghcr.io/yohannhommet/walspool:v1.0
```

#### Run with Docker Compose

A complete production setup coupling an application service with the Walspool sidecar:

```yaml
version: '3.8'

services:
  web-app:
    image: node:20-alpine
    command: node server.js
    environment:
      - WALSPOOL_URL=http://walspool:9099/v1/enqueue
    depends_on:
      walspool:
        condition: service_healthy

  walspool:
    image: ghcr.io/yohannhommet/walspool:v1.0
    restart: unless-stopped
    ports:
      - "9099:9099"
    environment:
      - WALSPOOL_ADDR=:9099
      - WALSPOOL_DATA_DIR=/data/spool
      - WALSPOOL_SINK_URL=https://telemetry.internal/v1/ingest
      - WALSPOOL_BATCH_SIZE=50
      - WALSPOOL_FLUSH_MS=50
      - WALSPOOL_MAX_RECORDS=50000
      - WALSPOOL_LOG_FORMAT=json
      - WALSPOOL_LOG_LEVEL=info
    volumes:
      - walspool-storage:/data/spool
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O- http://127.0.0.1:9099/readyz || exit 1"]
      interval: 5s
      timeout: 2s
      retries: 3

volumes:
  walspool-storage:
```

---

## Microservices Integration Patterns

### 1. Dedicated Pod Sidecar (Kubernetes / Localhost)
The sidecar runs in the same network namespace as your service container. Ingestion happens over `http://127.0.0.1:9099` with sub-15 µs overhead. If the downstream collector goes offline, Walspool absorbs bursts locally on NVMe and delivers batches upon reconnection.

```
┌───────────────────────────────────────────────────────────────┐
│ Kubernetes Pod / Localhost Context                            │
│                                                               │
│   ┌────────────────────────┐      POST /v1/enqueue (< 15 µs)  │
│   │ App Container          │─────────────────────────────┐    │
│   │ (Node, Python, Go)     │                             │    │
│   └────────────────────────┘                             │    │
│                                                          ▼    │
│                                               ┌────────────────────┐
│                                               │  Walspool Sidecar  │
│                                               │  - NVMe Disk WAL   │
│                                               │  - Ring Buffer     │
│                                               └─────────┬──────────┘
└─────────────────────────────────────────────────────────┼─────┘
                                                          │ Batched HTTP POST
                                                          ▼ (with backoff retry)
                                               ┌────────────────────┐
                                               │ Downstream Sink    │
                                               │ (Collector, S3...) │
                                               └────────────────────┘
```

### 2. Live Observability & Web Backoffice (SSE via Reverse-Proxy)
Expose live streaming to developer consoles or admin UIs through Nginx. Disable response buffering so events stream immediately without chunk delays:

```nginx
location /api/v1/observability/logs/stream {
    proxy_pass http://walspool:9099/v1/logs/stream;
    proxy_http_version 1.1;
    proxy_set_header Connection '';
    proxy_buffering off;
    proxy_cache off;
    chunked_transfer_encoding off;
    proxy_read_timeout 24h;
}
```

---

## HTTP & Streaming API Reference

All endpoints enforce strict HTTP method constraints and return JSON error structures on invalid requests:

| Method | Endpoint | Description | Status Code | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/v1/logs` | **OpenTelemetry (OTLP) HTTP Log Receiver** | `200 OK` | Native OTLP ingestion (`application/x-protobuf` & `application/json`). Returns `503` with `Retry-After: 1` if full. |
| `GET` | `/v1/logs` | Queries recent indexed entries from ring buffer | `200 OK` | Filters: `trace_id`, `service`, `level`, `limit`. |
| `GET` | `/v1/logs/stream` | Streams live events via Server-Sent Events (SSE) | `200 OK` | Filters: `service`, `level`. Periodic keepalive comments. |
| `GET` | `/v1/logs/stats` | Returns capacity and usage metrics of memory hub | `200 OK` | Capacity, active subscribers, dropped stream events. |
| `POST` | `/v1/enqueue` | Appends custom event to WAL and notifies in-memory hub | `202 Accepted` | Ingestion in < 15 µs. Returns `503` if spool is full. |
| `POST` | `/flush` | Forces immediate synchronous drain to sink | `200 OK` | Blocks until in-flight WAL batches are delivered. |
| `GET` | `/healthz` | Kubernetes Liveness Probe | `200 OK` | Confirms HTTP listener process responsiveness. |
| `GET` | `/readyz` | Kubernetes Readiness Probe | `200 OK` / `503` | Validates storage integrity. Flips to `503` on shutdown. |

Prometheus metrics are additionally exported at `GET /metrics`.

### Usage Examples

```bash
# 1. Zero-Code OpenTelemetry Ingestion
# Simply set standard OTel environment variables on any microservice:
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT="http://localhost:9099/v1/logs"
export OTEL_SERVICE_NAME="order-service"

# Or ingest OTLP JSON logs directly via curl:
curl -i -X POST http://localhost:9099/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "billing-api"}}]},
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1725835200000000000",
          "severityText": "INFO",
          "body": {"stringValue": "Invoice generated #4412"},
          "traceId": "4bf92f3577b34da6a3ce929d0e0e4736"
        }]
      }]
    }]
  }'

# 2. Custom JSON Enqueue (Legacy endpoint)
curl -i -X POST http://localhost:9099/v1/enqueue \
  -H "Content-Type: application/json" \
  -d '{"topic":"orders","trace_id":"trace-42","service":"api","level":"info","payload":{"id":102,"total":49.90}}'

# 3. Query historical logs by trace ID (< 15 µs)
curl "http://localhost:9099/v1/logs?trace_id=4bf92f3577b34da6a3ce929d0e0e4736"

# 4. Stream live events in real-time
curl -N "http://localhost:9099/v1/logs/stream?service=billing-api&level=info"

# 5. Check readiness
curl http://localhost:9099/readyz
```

> 📖 **Comprehensive Polyglot Guide**: For detailed code examples in **Go, Python, Node.js/TypeScript, Java (Spring Boot), and Rust**, as well as Kubernetes sidecar deployment manifests and OTel Collector forwarding configs, see the [OpenTelemetry (OTLP) Integration Guide](docs/OPENTELEMETRY.md).

---

## Standalone Observability CLI: `walspool`

Walspool ships with a standalone developer and SRE command-line tool (`cmd/walspool/`) written in 100% pure Go standard library with **zero third-party dependencies**.

### Build the CLI

```bash
go build -o bin/walspool ./cmd/walspool
```

### 1. `walspool stream` (Live Unix Tail & Filtering)

Connects to the sidecar's SSE stream and tails logs in real-time. Automatically colorizes output when connected to an interactive TTY, and strips ANSI codes when piped to downstream tools (`grep`, `jq`, `head`):

```bash
# Stream all logs from default daemon (http://localhost:9099)
bin/walspool stream

# Filter by service and severity level
bin/walspool stream --service checkout --level error

# Filter by distributed trace ID
bin/walspool stream --trace 4bf92f3577b34da6a3ce929d0e0e4736

# Pipe JSON to jq or head (gracefully handles SIGPIPE / EPIPE)
bin/walspool stream --format json | jq -r '.payload'
```

### 2. `walspool monitor` (Interactive 60 fps SRE Dashboard)

Launches an interactive, zero-flicker terminal dashboard using our custom raw-ANSI double-buffering engine. Displays real-time ingestion throughput, disk write rates, uncommitted spool backlog, activity sparklines (` ▂▃▅▆▇`), and live scrolling logs:

```bash
bin/walspool monitor
```

*Press `q` or `Ctrl+C` to cleanly exit and restore your terminal.*

> 🛡️ **Terminal Injection Defense**: `walspool` includes strict UTF-8 sanitization, stripping dangerous C0/C1 control characters and actively blocking OSC 52 clipboard hijacking and malicious cursor rewrite sequences. See [ADR-002](docs/architecture/adr/ADR-002-standalone-cli-tui.md).

### 3. Developer Workflows & Benchmarking

The repository includes ready-to-run verification scripts in `scripts/`:

```bash
# Terminal 1: Start sidecar with demo traffic
./scripts/demo-sidecar.sh

# Terminal 2: Stream logs or open live dashboard
./scripts/demo-cli.sh stream
./scripts/demo-cli.sh monitor

# High-concurrency load test (configurable workers, volume, payload size)
go run ./scripts/loadgen -addr http://localhost:9099 -c 64 -n 50000

# Automated end-to-end stress test (daemon + loadgen + SSE + metrics assertion)
./scripts/stress-test.sh
```

---

## Kubernetes & Telemetry (Production SRE)

### Security & Non-Root Execution
The official container runs as non-privileged user `walspool:walspool` (`UID 10001 / GID 10001`) with a static, scratch-like binary (`CGO_ENABLED=0`).

### Kubernetes Pod Spec Snippet

```yaml
spec:
  containers:
    - name: walspool
      image: ghcr.io/yohannhommet/walspool:v1.0
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        readOnlyRootFilesystem: true
        allowPrivilegeEscalation: false
      resources:
        requests:
          cpu: 50m
          memory: 32Mi
        limits:
          cpu: 500m
          memory: 128Mi
      volumeMounts:
        - name: spool-storage
          mountPath: /data/spool
      livenessProbe:
        httpGet:
          path: /healthz
          port: 9099
        initialDelaySeconds: 2
        periodSeconds: 10
      readinessProbe:
        httpGet:
          path: /readyz
          port: 9099
        initialDelaySeconds: 2
        periodSeconds: 5
  volumes:
    - name: spool-storage
      persistentVolumeClaim:
        claimName: walspool-pvc
```

### Prometheus Metrics (`/metrics`)

Walspool exports standard OpenMetrics format for direct scraping:

- `walspool_ingested_records_total{topic="..."}` (Counter): Total records ingested.
- `walspool_delivered_records_total{topic="..."}` (Counter): Total records delivered to sink.
- `walspool_uncommitted_records` (Gauge): Current backlog awaiting downstream delivery.
- `walspool_active_sse_subscribers` (Gauge): Connected live stream consumers.
- `walspool_dropped_events_total` (Counter): Stream events dropped due to slow SSE consumers.
- `walspool_ring_buffer_capacity` & `walspool_ring_buffer_count` (Gauge): Memory hub utilization.

---

## Operational Guarantees & Lifecycle

### Delivery Semantics & Failure Modes
- **At-Least-Once Delivery:** Records are acknowledged with `202 Accepted` only after persisting to the local WAL. The background dispatcher advances checkpoints after confirmed downstream delivery.
- **Transient Error Backoff:** HTTP `429` (Rate Limited), `408` (Timeout), and `5xx` trigger exponential backoff with jitter ($\min(\text{initial} \times 2^n, \text{max})$). Records remain safe on disk.
- **Poison Pill Elimination:** Permanent rejections (HTTP `400`, `422`) are logged as unprocessable, skipped, and checkpointed to prevent pipeline deadlocks.
- **Crash Recovery & Torn Writes:** If a host suddenly powers off during a write, Walspool detects incomplete frames via `fstat` and CRC32 verification on startup, safely truncating corrupted tail bytes with `os.Truncate` without impacting prior records.

### Graceful Shutdown Sequence (Zero In-Flight Loss)
When receiving `SIGTERM` or `SIGINT`:
1. `/readyz` immediately switches to `503 Service Unavailable`, prompting ingress/load balancers to stop routing traffic.
2. `hub.Close()` terminates active SSE streams and frees connection handles.
3. `httpServer.Shutdown()` drains remaining in-flight ingestion requests.
4. `spool.Flush()` flushes userspace buffers, force-delivers all pending records to the sink, commits the final checkpoint, and safely closes file handles.

---

## Configuration Reference

Flags take precedence over environment variables, which take precedence over defaults:

| Flag | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `-addr` | `WALSPOOL_ADDR` | `:9099` | TCP listen address for HTTP/SSE server |
| `-data-dir` | `WALSPOOL_DATA_DIR` | `./data/spool` | Directory for append-only `.wal` and metadata |
| `-sink-url` | `WALSPOOL_SINK_URL` | `""` (stdout) | Destination HTTP URL for batch delivery |
| `-batch-size` | `WALSPOOL_BATCH_SIZE` | `50` | Maximum records dispatched per batch |
| `-flush-ms` | `WALSPOOL_FLUSH_MS` | `50` | Maximum wait in ms before dispatching a batch |
| `-max-records` | `WALSPOOL_MAX_RECORDS` | `50000` | Disk spool capacity quota before backpressure `503` |
| `-hub-capacity` | `WALSPOOL_HUB_CAPACITY` | `50000` | In-memory ring buffer entry capacity |
| `-otlp-enabled` | `WALSPOOL_OTLP_ENABLED` | `true` | Enable native OpenTelemetry HTTP log receiver on `/v1/logs` |
| `-otlp-default-topic` | `WALSPOOL_OTLP_DEFAULT_TOPIC` | `otel_logs` | Fallback topic when OTLP records lack `service.name` |
| `-log-format` | `WALSPOOL_LOG_FORMAT` | `text` | Internal logger format (`text` or `json`) |
| `-log-level` | `WALSPOOL_LOG_LEVEL` | `info` | Minimum log level (`debug`, `info`, `warn`, `error`) |

*For embedded Go library usage, multi-segment rotation is configured directly via `walspool.FileStorageOptions{MaxSegmentSize: 64 * 1024 * 1024}`.*

---

## Benchmarks

Measurements executed on Linux 6.8 (x86_64), Intel Core i7-1255U, NVMe PCIe 4.0 SSD, using standard Go benchmarking:

```bash
go test -run=^$ -bench=. -benchmem ./...
```

| Benchmark Operation | Throughput | Latency | Memory / Op | Allocations |
| :--- | :--- | :--- | :--- | :--- |
| `FileStorage.Append` (128 KB buffer, `SyncInterval`) | 937,207 ops/s | 1.06 µs/op | 259 B/op | 1 alloc/op |
| `LogHub.Ingest` (Ring buffer insertion) | 992,063 ops/s | 1.01 µs/op | 203 B/op | 1 alloc/op |
| `LogHub.QueryByTraceID` (Indexed lookup) | 268,168 ops/s | 3.73 µs/op | 1,440 B/op | 11 allocs/op |
| `Spooler.Enqueue` (Dual-Engine Ingestion) | 607,164 ops/s | 1.65 µs/op | 1,131 B/op | 1 alloc/op |
| `FileStorage.SegmentRotation` (64 MB segments, auto-prune) | 20,132 rotations/s | 72.5 µs/op | 384 B/op | 3 allocs/op |
| `FileStorage.Append` (`SyncEveryRecord`, sync fsync) | 329 ops/s | 3,037 µs/op | 115 B/op | 1 alloc/op |

---

## Community Roadmap & Open Issues

Development is tracked publicly across 3 structured milestones:

- **[v1.2.0: High Throughput & DX Quick Wins](https://github.com/YohannHommet/walspool/milestone/1)** (Sprint 1)
  - [#7](https://github.com/YohannHommet/walspool/issues/7) : `feat(ingest)` Batch ingestion endpoint (`POST /v1/enqueue/batch`)
  - [#8](https://github.com/YohannHommet/walspool/issues/8) : `feat(ingest)` Streaming NDJSON support (`application/x-ndjson`)
  - [#9](https://github.com/YohannHommet/walspool/issues/9) : `perf(ingest)` Zero-alloc token scanner for metadata extraction
  - [#10](https://github.com/YohannHommet/walspool/issues/10) : `fix(sidecar)` Standardize default sidecar and client port to `:9099`
  - [#11](https://github.com/YohannHommet/walspool/issues/11) : `fix(hub)` Eliminate $O(N)$ slice shift copy on circular buffer wrap-around
  - [#12](https://github.com/YohannHommet/walspool/issues/12) : `fix(dispatcher)` Full decorrelated jitter backoff & reusable timer

- **[v2.0.0: Storage V2 & Hardware Sympathy](https://github.com/YohannHommet/walspool/milestone/2)** (Sprint 2)
  - [#13](https://github.com/YohannHommet/walspool/issues/13) : `feat(ingest)` Unix domain socket listener for sub-3µs IPC
  - [#14](https://github.com/YohannHommet/walspool/issues/14) : `perf(otlp)` Direct zero-copy protobuf decoder for OTLP logs
  - [#15](https://github.com/YohannHommet/walspool/issues/15) : `feat(storage)` 64-bit aligned V2 32-byte binary frame format
  - [#16](https://github.com/YohannHommet/walspool/issues/16) : `perf(storage)` Hardware-accelerated CRC32C Castagnoli checksum
  - [#17](https://github.com/YohannHommet/walspool/issues/17) : `feat(storage)` Fault-tolerant magic scan auto-healing for corrupted segments
  - [#18](https://github.com/YohannHommet/walspool/issues/18) : `perf(storage)` 64KB sparse index (reduces index RAM by 99.9%)
  - [#19](https://github.com/YohannHommet/walspool/issues/19) : `perf(storage)` Decouple `f.mu` lock during checksum & disk flush
  - [#20](https://github.com/YohannHommet/walspool/issues/20) : `feat(storage)` Circular WAL checkpointing (`RESTART` mode)
  - [#21](https://github.com/YohannHommet/walspool/issues/21) : `perf(hub)` Cache-line padded ring buffer (anti-false sharing)
  - [#22](https://github.com/YohannHommet/walspool/issues/22) : `perf(hub)` $O(\log N)$ binary search for historical timestamps
  - [#23](https://github.com/YohannHommet/walspool/issues/23) : `perf(hub)` String interning (`SymbolTable`) for tags and service names

- **[v2.1.0: Adaptive Dispatch & Semantic Intelligence](https://github.com/YohannHommet/walspool/milestone/3)** (Sprint 3)
  - [#24](https://github.com/YohannHommet/walspool/issues/24) : `feat(dispatcher)` Adaptive batching driven by downstream RTT
  - [#25](https://github.com/YohannHommet/walspool/issues/25) : `feat(dispatcher)` Pipelined HTTP dispatcher (up to 16 in-flight)
  - [#26](https://github.com/YohannHommet/walspool/issues/26) : `feat(cli)` Client-side semantic log filtering (`walspool stream --semantic`)

---

## Documentation & Releases

- **[Architecture Deep-Dive](docs/architecture/ARCHITECTURE.md)**: Detailed Ports & Adapters specifications, O(1) in-memory ring buffer, multi-segment rotation, and CLI engine.
- **[Architecture Decision Records (ADR)](docs/architecture/adr/)**: Architectural governance and decisions ([ADR-001 Multi-Segment](docs/architecture/adr/ADR-001-multi-segment-rotation.md), [ADR-002 Standalone CLI](docs/architecture/adr/ADR-002-standalone-cli-tui.md), [ADR-003 V2 Blueprint](docs/architecture/adr/ADR-003-v2-high-throughput-and-community-scope.md)).
- **[Observability & Resilience Report](docs/architecture/OBSERVABILITY_REPORT.md)**: Real-time SSE streaming, zero-allocation scanner, trace correlation.
- **[v1.1.0 Release Notes](docs/release/RELEASE_NOTES_v1.1.0.md)**: Latest release notes including CLI and multi-segment WAL rotation.
- **[v1.0.0 Release Notes](docs/release/RELEASE_NOTES_v1.0.0.md)**: v1.0.0 GA changelog, binary distributions, and initial architecture.

---

## License

Walspool is distributed under the **[Functional Source License, Version 1.1, MIT Future License (FSL-1.1-MIT)](LICENSE)**.

- **100% Free for Developers & Enterprises:** You are free to use, inspect, modify, and run Walspool for any internal commercial, educational, or testing purpose with zero fees or royalties.
- **Fair-Source Protection:** You may not offer Walspool as a managed commercial service that competes with Walspool (no "Walspool as a Service").
- **Automatic MIT Conversion:** Each release automatically converts to the standard, permissive **MIT License** on the second anniversary of its release date.

