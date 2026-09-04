# Walspool v1.0.0 — Official Release Notes

> **Date :** 05 Septembre 2026  
> **Auteur & Mainteneur Unique :** Yohann Hommet ([@YohannHommet](https://github.com/YohannHommet))  
> **Tag :** [`v1.0.0`](https://github.com/YohannHommet/walspool/releases/tag/v1.0.0)  
> **Licence :** Functional Source License Version 1.1 avec réversion MIT à 2 ans (`FSL-1.1-MIT`)

---

We are thrilled to announce the official release of **Walspool v1.0.0**, the high-performance Dual-Engine Write-Ahead Log (WAL) buffer and live observability hub written in pure Go with zero external dependencies.

Walspool bridges the critical reliability gap between volatile in-memory queues (Go channels, asyncio, Redis) that drop data on crash or Kubernetes pod restart, and heavyweight distributed message brokers (Kafka, Redpanda, RabbitMQ) that require dedicated operations and high cloud costs.

---

## 🚀 Key Highlights & Architecture

### 1. Dual-Engine Black-Box Architecture
- **Moteur Disque Séquentiel (WAL)** : Câblage binaire ultra-compact de 29 octets avec intégrité IEEE CRC32 par enregistrement, Group Commit en mémoire utilisateur de 128 Ko, atteignant **1 074 441 ops/sec** (1,41 µs/op, 1 allocation sur disque NVMe standard).
- **Hub d'Observabilité Mémoire** : Ring Buffer circulaire $O(1)$ (50 000 entrées) avec scanner lexical de métadonnées sans allocation (`scanTopLevelMeta`) et diffusion Server-Sent Events (SSE) non bloquante à **2 846 908 ops/sec** (412 ns/op, 1 allocation).

### 2. Durcissement Technique v1.0 (Production-Grade)
- **Rollback Physique Disque (`rollbackTo`)** : En cas d'échec d'écriture disque ou coupure I/O, le fichier WAL est tronqué exactement au dernier offset intègre via `file.Truncate()`, éliminant tout risque de corruption par *zero-padding*.
- **OOM Guard sur Crash Recovery** : Plafond strict de sécurité (10 Mo) lors de la relecture des en-têtes à l'initialisation, immunisant le binaire contre l'épuisement mémoire sur fichier corrompu.
- **Préservation Entiers 64 bits** : Décodage JSON via `decoder.UseNumber()` préservant strictement les identifiants Snowflake (Twitter, Discord) et les horodatages nanosecondes.
- **Tolérance aux Pannes Aval & Backpressure** : Gestion intelligente des codes transitoires `HTTP 429 Too Many Requests` et `HTTP 408 Request Timeout` avec reprise exponentielle sans perte de données.
- **Arrêt Propre en 4 Phases (Graceful Teardown)** : Sur `SIGTERM`, fermeture du Hub SSE pour libérer immédiatement les abonnés, arrêt des connexions entrantes, drainage et flush forcé des tampons disque vers le downstream sink, et clôture étanche des descripteurs de fichiers.
- **100% Race-Free** : Zéro datarace sous charge concurrente maximale (`go test -race ./...`).

### 3. Zéro Dépendance Externe & Binaire Autonome
- Binaire autonome statique d'environ 15 Mo (`CGO_ENABLED=0`).
- [`go.mod`](https://github.com/YohannHommet/walspool/blob/main/go.mod) utilise exclusivement la bibliothèque standard de Go (1.22+).

---

## 📊 Benchmarks Officiels Certifiés

| Composant / Opération | Débit (Throughput) | Latence Moyenne | Allocations / Op | Intégrité & Format |
| :--- | :--- | :--- | :--- | :--- |
| **Moteur WAL Disque (Group Commit 128 Ko)** | **1 074 441 ops/s** | **1,41 µs / op** | 1 alloc / op | Header 29B + IEEE CRC32 |
| **Hub Mémoire (SSE & Indexation)** | **2 846 908 ops/s** | **412 ns / op** | 1 alloc / op | Ring Buffer $O(1)$ circulaire |
| **Requête Trace Ciblée (`/v1/logs?trace_id=`)** | **> 500 000 req/s** | **< 2 µs / op** | 0 alloc additionnelle | Index inversé `byTraceID` |

---

## 📦 Installation & Démarrage Rapide

### Option A : Image Docker Officielle (GHCR Multi-Arch)
Compatible `linux/amd64` et `linux/arm64` (Apple Silicon & serveurs ARM) :

```bash
docker run -d \
  --name walspool-sidecar \
  -p 9099:9099 \
  -v /tmp/spool:/data/spool \
  ghcr.io/yohannhommet/walspool:v1.0.0
```

### Option B : Module Go (Bibliothèque Intégrable)
```bash
go get github.com/YohannHommet/walspool@v1.0.0
```

```go
package main

import (
	"context"
	"github.com/YohannHommet/walspool"
)

func main() {
	storage, _ := walspool.NewFileStorageEngine("./data/spool", 50000)
	hub := walspool.NewMemoryLogHub(10000)
	spool, _ := walspool.New(walspool.DefaultConfig(), storage, nil, nil, walspool.WithObserver(hub))
	defer spool.Close()

	offset, _ := spool.Enqueue(context.Background(), "orders", []byte(`{"order_id":9982,"status":"confirmed"}`))
	_ = offset
}
```

### Option C : Binaires Autonomes Précompilés
Téléchargeables directement dans les assets de cette release :
- `walspool_1.0.0_linux_amd64.tar.gz`
- `walspool_1.0.0_linux_arm64.tar.gz`
- `walspool_1.0.0_darwin_amd64.tar.gz`
- `walspool_1.0.0_darwin_arm64.tar.gz`
- `walspool_1.0.0_windows_amd64.zip`

Vérification de la somme de contrôle SHA-256 via `checksums.txt`.

---

## 🛠️ Endpoints HTTP du Sidecar

| Verbe & Chemin | Rôle |
| :--- | :--- |
| `POST /enqueue` | Ingestion d'un log ou événement dans le WAL disque et le Hub mémoire |
| `GET /v1/logs` | Consultation historique avec filtres (`?service=`, `?trace_id=`, `?limit=`) |
| `GET /v1/logs/stream` | Flux Server-Sent Events (SSE) en temps réel |
| `GET /v1/logs/stats` | Statistiques du Hub mémoire (capacité, total ingéré, abonnés actifs) |
| `POST /flush` | Forçage immédiat du Group Commit disque et du drain vers le downstream sink |
| `GET /readyz` | Sonde Kubernetes Readiness (retourne 503 si stockage défaillant ou en arrêt) |
| `GET /healthz` | Sonde Kubernetes Liveness (retourne 200 si le processus répond) |
| `GET /metrics` | Métriques Prometheus complètes au format standard text/plain |

---

## ⚖️ Gouvernance & Licence

Walspool v1.0.0 est distribué sous licence **Functional Source License Version 1.1** avec réversion irrévocable en **MIT au bout de 2 ans** (`FSL-1.1-MIT`).  
- **Usage Interne** : 100% gratuit et illimité pour toutes les entreprises, développeurs et chercheurs.
- **Protection SaaS** : Interdiction faite aux tiers d'exploiter commercialement Walspool comme un service managé concurrent sans accord préalable.
- **Propriété Intellectuelle** : Créé et maintenu exclusivement par **Yohann Hommet**.
