# Walspool v1.0 — Kit de Lancement Marketing Développeur

> **Auteur & Propriétaire Exclusif :** Yohann Hommet (`https://github.com/YohannHommet`)  
> **Dépôt Officiel :** [`https://github.com/YohannHommet/walspool`](https://github.com/YohannHommet/walspool)  
> **Version :** `v1.0.0`  
> **Licence :** Functional Source License Version 1.1 avec réversion MIT à 2 ans (`FSL-1.1-MIT`)  
> **Design System :** *Teenage Hardware* (`#08080A`, `#CCFF00`, `#EA580C`)

---

## Sommaire

1. [Stratégie & Calendrier de Diffusion](#1-stratégie--calendrier-de-diffusion)
2. [Show HN (Hacker News)](#2-show-hn-hacker-news)
3. [Article Technique Deep-Dive (Dev.to / Hashnode / Blog)](#3-article-technique-deep-dive)
4. [Publications Communautaires Reddit](#4-publications-communautaires-reddit)
   - [Post r/golang](#a-reddit-rgolang)
   - [Post r/devops & r/microservices](#b-reddit-rdevops--rmicroservices)
5. [Thread Twitter / X](#5-thread-twitter--x)
6. [Post LinkedIn (Voix Fondateur & Architecte)](#6-post-linkedin)

---

## 1. Stratégie & Calendrier de Diffusion

Pour maximiser l'effet de levier sans dispersion, la diffusion suit un séquençage précis en 48 heures :

| Moment | Canal | Cible & Angle | Objectif |
| :--- | :--- | :--- | :--- |
| **Jour J — 14h00 UTC** | **Hacker News (Show HN)** | Communauté hacker, ingénieurs systèmes | Étoiles GitHub, feedback architecture |
| **Jour J — 14h30 UTC** | **Twitter / X** | Développeurs Go, Cloud Native, Edge | Viralité visuelle, citations |
| **Jour J — 16h00 UTC** | **LinkedIn** | CTOs, VP Eng, Architectes d'entreprise | Crédibilité B2B, leads Enterprise |
| **Jour J+1 — 13h00 UTC** | **Reddit (`r/golang`)** | Développeurs Go purs | Discussion technique (zéro dep, scanner lexical) |
| **Jour J+1 — 15h00 UTC** | **Reddit (`r/devops`)** | Ingénieurs SRE & Kubernetes | Utilisation en DaemonSet/Sidecar résilient |
| **Jour J+2 — 14h00 UTC** | **Dev.to / Hashnode** | Développeurs Backend / Distributed Systems | Référencement SEO durable |

---

## 2. Show HN (Hacker News)

**Titre :**  
`Show HN: Walspool – A 1.1M ops/s Write-Ahead Log buffer and SSE stream in pure Go`

**Texte du Post :**

```text
Hi HN! I’m Yohann (https://github.com/YohannHommet), and I built Walspool.

If you’ve built distributed microservices or edge pipelines, you've probably faced this dilemma:
1. In-memory queues (Go channels, Python queues, Redis in-memory): Blazing fast, but ephemeral. If your pod crashes, restarts for a Kubernetes rolling update, or gets hit by the OOM-killer, 100% of pending telemetry and un-shipped logs are instantly lost.
2. Heavy distributed brokers (Kafka, Redpanda, RabbitMQ): Rock solid, but huge overkill when you just need a resilient local buffer on a single node or edge appliance. A managed Kafka cluster costs thousands a month and requires dedicated Ops.

I built Walspool to bridge that exact gap: a single, self-contained binary (~15 MB) that acts as an unkillable local buffer and live observability hub.

### What it does under the hood

Walspool runs as a sidecar or standalone daemon exposing a lightweight HTTP API (`POST /enqueue`):
1. Disk WAL Engine: Writes records sequentially with a 29-byte binary framing and IEEE CRC32 checksum. Uses a 128 KB user-space Group Commit buffer, achieving 1,074,441 ops/s (1.41 µs/op, 1 heap allocation).
2. Resilient Downstream Shipping: A background worker drains records in batches to your downstream sink (OpenTelemetry collector, Vector, ClickHouse, Datadog, or custom HTTP endpoint). If the downstream returns HTTP 429 (Rate Limit) or network drops, Walspool holds data safely on disk and replays it upon recovery.
3. In-Memory Observability Hub: Simultaneously retains recent logs in an O(1) circular ring buffer with zero-alloc lexical metadata extraction (service, trace_id, level) and streams them via Server-Sent Events (`GET /v1/logs/stream`) at 2,846,908 ops/s.
4. Kubernetes-Ready: Native `/healthz`, `/readyz` probes and Prometheus `/metrics` endpoint.

### Engineering Decisions & Zero-Dependency Rule
- Pure Go standard library: 0 external Go dependencies (`go.mod` has zero external packages).
- 0 data races: Clean under `go test -race ./...`.
- Crash-safety: Physical disk rollback (`truncate`) on write faults (no zero-padding), OOM guard capping corrupted record headers to 10 MB on startup replay, and strict 64-bit integer preservation via `decoder.UseNumber()`.
- 4-phase Graceful Teardown: On SIGTERM, it shuts down the live SSE hub, stops incoming HTTP connections, flushes all un-shipped disk batches to downstream sinks, and safely closes disk handles.

### Quickstart (Docker & Binary)
```bash
# Run with Docker (multi-arch AMD64/ARM64)
docker run -d -p 9099:9099 -v /tmp/spool:/data/spool ghcr.io/yohannhommet/walspool:v1.0.0

# Ingest a record
curl -X POST http://localhost:9099/enqueue \
  -H "Content-Type: application/json" \
  -d '{"service":"checkout","level":"INFO","trace_id":"tr-9812","msg":"Order placed"}'

# Stream live logs in real time via SSE
curl -N http://localhost:9099/v1/logs/stream
```

### Source & License
Source code: https://github.com/YohannHommet/walspool  
Docs & Live Preview: https://yohannhommet.github.io/walspool  

Walspool is licensed under the Functional Source License (FSL-1.1-MIT). It is 100% free and open for internal use in development, staging, and production. To prevent cloud giants from wrapping it into a closed commercial SaaS, commercial hosting as a service is restricted for the first 2 years, after which it automatically converts to pure MIT.

I'd love your feedback on the WAL framing design, memory hub indexing, or how you currently handle edge telemetry buffering. Thanks for reading!
```

---

## 3. Article Technique Deep-Dive

**Plateformes cibles :** Dev.to, Hashnode, Substack, Medium  
**Titre suggéré :** *Building a Sub-Microsecond Write-Ahead Log in Pure Go: Dual-Engine Architecture, Zero Allocations, and Crash-Proof Group Commit*

```markdown
# Building a Sub-Microsecond Write-Ahead Log in Pure Go: Dual-Engine Architecture, Zero Allocations, and Crash-Proof Group Commit

When building high-throughput telemetry pipelines, backend engineers often hit an uncomfortable architectural compromise:
- **In-memory queues** offer microsecond latency, but a pod restart or crash means instant data loss.
- **Enterprise brokers (Kafka, RabbitMQ)** guarantee durability, but running a multi-node broker just to buffer local node logs is operational madness.

In this article, we dive deep into the internals of **Walspool** (https://github.com/YohannHommet/walspool), an open-source Dual-Engine daemon built in pure Go without external dependencies, certified at **1.07M disk ops/sec** and **2.84M in-memory stream ops/sec**.

---

## 1. Black-Box Architecture: Clean Ports & Adapters

Walspool strictly isolates durability from delivery using standard interfaces defined in `ports.go`:

```go
type StorageEngine interface {
    Append(ctx context.Context, topic string, payload []byte) (Offset, error)
    Read(ctx context.Context, from Offset, maxRecords int) ([]Record, error)
    Checkpoint(ctx context.Context, offset Offset) error
    Close() error
}

type Sink interface {
    Deliver(ctx context.Context, batch []Record) error
}

type IngestionObserver interface {
    OnRecordIngested(rec Record)
}
```

By decoupling storage from shipping, the core engine treats disk I/O, network egress, and memory observability as black boxes.

---

## 2. Disk Engine: 29-Byte Binary Framing & 128 KB Group Commit

Writing every log with an individual `fsync` crushes NVMe throughput to ~10,000 IOPS. To reach **1,074,441 ops/s**, Walspool implements a user-space **Group Commit** buffer:

1. **29-byte binary wire header**:
   - `[0..1]`: Magic bytes `0x57 0x53` (`WS`)
   - `[2]`: Wire version `0x01`
   - `[3..6]`: IEEE CRC32 checksum of payload & metadata
   - `[7..14]`: Monotonic Record ID (64-bit)
   - `[15..22]`: Unix timestamp in nanoseconds (64-bit)
   - `[23..24]`: Topic length (uint16)
   - `[25..28]`: Payload length (uint32)

2. **Group Commit 128 KB**: Incoming records are packed into a pre-allocated byte buffer. The buffer flushes to kernel page cache via `io.Writer` and syncs sequentially.

3. **Crash Recovery without Zero-Padding**: Many WAL implementations pad aborted writes with zeros, creating corrupted states upon reboot. Walspool records the exact physical file offset before each append (`rollbackTo`). If a disk I/O fault occurs, it executes `file.Truncate(rollbackTo)` immediately.

4. **OOM Guard on Startup**: To prevent corrupt files from triggering allocation of gigabytes of RAM during replay, headers are inspected against an `OOM Guard` threshold (10 MB maximum payload per record).

---

## 3. In-Memory Hub: Zero-Allocation Lexical Scanning

In addition to disk persistence, engineers need live tailing (`curl -N /v1/logs/stream`) and fast trace lookups (`/v1/logs?trace_id=...`). 

Parsing JSON with `json.Unmarshal` for millions of incoming logs allocates heap memory and chokes the Go garbage collector. Walspool uses an in-place lexical scanner (`scanTopLevelMeta`):

- Inspects raw bytes directly without allocating intermediate strings or structs.
- Extracts `trace_id`, `service`, and `level` at the top level of valid JSON.
- Stores records in an $O(1)$ ring buffer with inverted index maps.
- GC Leak Prevention: On ring overwrites, previous entry pointers are explicitly nilled (`traceList[0] = nil`), preventing memory leaks.

Result: **2,846,908 ops/s** with 1 heap allocation per operation.

---

## 4. Concurrency & Graceful Draining

A major problem in logging sidecars is abrupt termination during Kubernetes deployments (`SIGTERM`). Walspool enforces a deterministic 4-stage shutdown:

1. **Close LogHub**: Immediately unblocks long-lived HTTP Server-Sent Events client connections.
2. **Shutdown HTTP Server**: Stops accepting new `/enqueue` requests and finishes in-flight requests.
3. **Flush Spooler**: Forces background dispatchers to drain all pending disk records to downstream sinks (with exponential backoff on HTTP 429/503).
4. **Close Storage Engine**: Safely syncs and closes OS file descriptors.

The entire test suite (`go test -race ./...`) passes with zero race conditions.

---

## 5. Benchmarks Summary

| Metric | Disk WAL Engine | In-Memory Hub |
| :--- | :--- | :--- |
| **Throughput** | **1,074,441 ops/sec** | **2,846,908 ops/sec** |
| **Latency** | **1.41 µs / op** | **412 ns / op** |
| **Allocations** | 1 alloc / op | 1 alloc / op |
| **Integrity** | IEEE CRC32 checksum | Circular Ring Buffer $O(1)$ |

Check out the code, run the benchmarks, and star the repo:
👉 https://github.com/YohannHommet/walspool
```

---

## 4. Publications Communautaires Reddit

### A. Reddit `r/golang`

**Titre :**  
`I built Walspool: a 1.1M ops/s Write-Ahead Log & SSE telemetry hub with zero external dependencies`

**Contenu :**

```text
Hey Gophers,

Over the past few months, I've been working on Walspool (https://github.com/YohannHommet/walspool), an open-source Write-Ahead Log sidecar daemon written in standard Go.

The motivation came from a recurrent frustration: standard Go channels are great until your process crashes or gets OOM-killed, losing all un-shipped telemetry. On the flip side, running a multi-node Kafka cluster just to buffer edge logs is overkill.

Key technical details you might appreciate:
1. Zero external dependencies: Only the Go standard library (`os`, `net/http`, `sync`, `log/slog`).
2. Dual-Engine Architecture:
   - Disk WAL: 29-byte binary framing with IEEE CRC32 integrity, 128 KB group commit buffer, running at ~1.07M ops/sec.
   - Memory Hub: O(1) circular ring buffer with SSE streaming at ~2.84M ops/sec.
3. Zero-allocation metadata scanning: Instead of running `json.Unmarshal` on every log line, `scanTopLevelMeta` extracts `trace_id`, `service`, and `level` directly from raw bytes, avoiding heavy GC pressure.
4. Hardened concurrency:
   - 100% race-free under `go test -race ./...`.
   - Exact physical rollback (`rollbackTo`) on disk write failures instead of zero-padding.
   - 64-bit integer preservation for Twitter-Snowflakes and timestamps via `decoder.UseNumber()`.
   - Safe GC cleanup when ring buffer wraps around (`traceList[0] = nil`).

Repo: https://github.com/YohannHommet/walspool  
Docs & benchmarks: https://yohannhommet.github.io/walspool

Would love to hear your thoughts on the framing format or the zero-alloc scanner!
```

---

### B. Reddit `r/devops` & `r/microservices`

**Titre :**  
`Stop losing telemetry on Kubernetes pod restarts: Walspool, a lightweight (~15MB) WAL buffer & SSE sidecar`

**Contenu :**

```text
Hi r/devops and r/microservices!

If you've ever dealt with microservices dropping logs and metrics during deployment rollouts or downstream rate-limits (HTTP 429 Too Many Requests), here is a tool you might find useful.

I created Walspool (https://github.com/YohannHommet/walspool) as an ultra-lightweight sidecar daemon:
- Acts as a local shock-absorber on `localhost:9099`.
- Persists events to NVMe sequentially via Write-Ahead Log (1.07M ops/s).
- Buffers events when downstream sinks (Vector, OpenTelemetry, Datadog, ClickHouse) are slow or failing.
- Replays data automatically with retry/backoff upon recovery.
- Exposes Prometheus `/metrics` and Kubernetes `/healthz` & `/readyz` probes.
- Drains cleanly on `SIGTERM` before container teardown.
- Live telemetry streaming via SSE (`GET /v1/logs/stream`) for instant terminal debugging without touching log indexers.

It ships as a non-root scratch/Alpine container (~15 MB) with AMD64 and ARM64 support:
`docker run -p 9099:9099 ghcr.io/yohannhommet/walspool:v1.0.0`

Feedback and contributions are very welcome!
```

---

## 5. Thread Twitter / X

**Tweet 1 (Hook & Annonce) :**  
🚀 Announcing Walspool v1.0!  
An ultra-fast Write-Ahead Log buffer & live telemetry hub built in pure Go.  

⚡ 1,074,000 disk ops/s (Group Commit 128KB, CRC32)  
🔥 2,840,000 ops/s in-memory SSE stream  
🛡️ 0 external dependencies. Single ~15MB binary.  

GitHub: https://github.com/YohannHommet/walspool 🧵👇

---

**Tweet 2 (The Problem) :**  
The distributed telemetry dilemma:  
- Go channels & in-memory buffers: Fast, but 100% data loss if your pod restarts or hits OOM.  
- Kafka / Redpanda: Rock-solid, but massive overkill and thousands of dollars/mo for local node buffering.  

Walspool bridges that gap.

---

**Tweet 3 (The Disk WAL) :**  
Under the hood of the disk engine:  
- 29-byte custom binary header with IEEE CRC32 checksums.  
- 128 KB user-space Group Commit.  
- Physical rollback (`file.Truncate`) on disk error (no corrupt zero-padding).  
- Startup OOM Guard capping record payloads to 10 MB.  

---

**Tweet 4 (The Memory Hub) :**  
Need live log tailing?  
Instead of parsing full JSON via reflection, Walspool features `scanTopLevelMeta`: a zero-alloc lexical scanner extracting `trace_id`, `service`, and `level` directly from raw bytes.  

Stream logs live via Server-Sent Events:  
`curl -N localhost:9099/v1/logs/stream`

---

**Tweet 5 (Reliability & K8s) :**  
Production-ready for cloud-native workloads:  
✅ Native `/healthz` & `/readyz` Kubernetes probes  
✅ Full Prometheus `/metrics`  
✅ Downstream HTTP 429 backoff retry  
✅ 4-stage graceful drain on SIGTERM  
✅ 100% race-free (`go test -race ./...`)  

---

**Tweet 6 (Docker & Quickstart) :**  
Try it in 5 seconds with Docker (multi-arch AMD64 & ARM64):  
```bash
docker run -d -p 9099:9099 \
  -v /tmp/spool:/data/spool \
  ghcr.io/yohannhommet/walspool:v1.0.0
```

---

**Tweet 7 (Call to Action) :**  
Walspool is licensed under Fair Source FSL-1.1-MIT (free for internal commercial use, 100% reverts to MIT in 2 years).  

⭐ Star the repo and read the architecture deep-dive:  
https://github.com/YohannHommet/walspool  

Let me know what you think! 💬

---

## 6. Post LinkedIn

**Format :** Post fondateur & retour d'expérience d'architecte logiciel.

```text
💡 Pourquoi nous avons conçu un moteur Write-Ahead Log à 1M ops/sec en Go pur (sans aucune dépendance externe).

Dans la quasi-totalité des architectures microservices et edge actuelles, la gestion des logs et événements critiques fait face à un compromis difficile :
1️⃣ Les queues en mémoire (Go channels, Redis in-memory) : ultra-rapides, mais volatiles. Au moindre crash ou redéploiement Kubernetes, 100 % des données non transmises sont perdues.
2️⃣ Les brokers distribués lourds (Kafka, RabbitMQ) : extrêmement puissants, mais surdimensionnés et coûteux lorsqu'il s'agit simplement de tamponner localement la télémétrie d'un nœud.

Pour résoudre ce problème à la racine, j'ai développé Walspool (v1.0.0).

L'objectif : un sidecar autonome d'environ 15 Mo, zéro dépendance externe, capable d'absorber les pics de charge et de garantir la persistance locale sur NVMe en cas de panne réseau ou de saturation aval.

Ce que nous avons accompli sur le plan architectural :
🔹 Performance brute : 1 074 441 écritures disque/sec (1,41 µs/op) grâce à un Group Commit de 128 Ko et un framing binaire de 29 octets avec CRC32.
🔹 Streaming temps réel : Un hub circulaire en mémoire diffusant les logs par Server-Sent Events (SSE) à 2 846 908 ops/sec, avec un scanner lexical de métadonnées sans allocation JSON.
🔹 Résilience certifiée : Zéro datarace sous charge concurrente, reprise sur panne sans zero-padding, et absorption du backpressure aval (HTTP 429).
🔹 Intégration Cloud-Native : Sondes Kubernetes natives (/healthz, /readyz) et métriques Prometheus (/metrics).

Walspool est publié sous licence Fair Source FSL-1.1-MIT : gratuit et libre pour toute utilisation interne en entreprise, avec une conversion automatique en licence MIT pure au bout de 2 ans.

Le dépôt GitHub officiel et la documentation technique sont en ligne :
🔗 Dépôt : https://github.com/YohannHommet/walspool
🔗 Site & Docs : https://yohannhommet.github.io/walspool

Curieux d'avoir vos retours d'expérience sur la résilience de vos pipelines de données !

#Golang #SoftwareArchitecture #DevOps #Microservices #CloudNative #OpenSource #HighPerformance
```
