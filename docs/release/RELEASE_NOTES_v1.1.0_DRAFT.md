# Walspool v1.1.0 — Preliminary Release Notes (Draft)

> **Date de Sortie Cible :** Q4 2026  
> **Auteur & Mainteneur Unique :** Yohann Hommet ([@YohannHommet](https://github.com/YohannHommet))  
> **Tag Prévu :** `v1.1.0`  
> **Licence :** Functional Source License Version 1.1 avec réversion MIT à 2 ans (`FSL-1.1-MIT`)  
> **Branche de Travail :** `feature/cli-stream-monitor` & `feature/segment-rotation` (PR #6)  

---

## 🚀 Vue d'Ensemble & Faits Marquants de la Version v1.1.0

La version **v1.1.0** de Walspool constitue une étape majeure dans l'évolution du projet, renforçant simultanément la robustesse opérationnelle du stockage sur disque, l'expérience développeur (DX) avec l'arrivée d'une interface terminal native, et la traçabilité architecturale formelle.

---

## 🌟 Nouvelles Fonctionnalités Majeures

### 1. Binaire CLI Autonome `walspool` (`cmd/walspool/`)
Walspool intègre désormais son propre outil en ligne de commande, compilé en pur Go sans **aucune dépendance tierce** (`go.mod` préservé à 0 dépendance) :

* **`walspool stream` (Composabilité Unix)** :
  - Consomme le flux Server-Sent Events (SSE) du sidecar en direct.
  - Filtrage granulaire par microservice (`--service`), trace distribuée (`--trace`), niveau de log (`--level`) ou expression régulière (`--filter`).
  - Détection automatique de TTY : mise en forme colorée ANSI pour l'humain, ou bascule en mode texte/JSON compact pour les pipelines de commande (`grep`, `jq`, `head`).
  - Respect strict des signaux de pipe Unix (`SIGPIPE` / `EPIPE`).

* **`walspool monitor` (Tableau de Bord Live SRE 60 fps)** :
  - Moteur de rendu exclusif en **double-buffering ANSI** : affichage fluide et sans clignotement (*flicker-free*) dans l'écran alternatif du terminal.
  - Métriques clés en temps réel : débit d'ingestion (ev/s), vitesse disque (Ko/s), événements non commités, volume disque, santé générale.
  - Mini-histogramme dynamique (sparkline Unicode ` ▂▃▅▆▇`) affichant l'activité des 60 dernières secondes.
  - Restauration garantie de l'état de la console lors de l'arrêt (`Ctrl+C` ou touche `q`).

* **Défense en Profondeur du Terminal (`security.go`)** :
  - Validation UTF-8 stricte et dépouillement des séquences d'échappement malveillantes (codes C0/C1, réécriture d'écran CSI non approuvée).
  - Neutralisation totale des tentatives d'exploitation du presse-papier système de l'opérateur via la commande OSC 52.
  - Protection OOM par troncature automatique des payloads démesurés (> 64 Ko).

* **Portabilité Multi-Plateformes Native** :
  - Support Linux, macOS et Windows (gestion des terminaux virtuels *Virtual Terminal Processing*).

---

### 2. Moteur de Rotation Multi-Segments & Auto-Élagage (PR #6)
Le composant `storage_file.go` gère désormais le découpage déterministe du journal physique sur disque :

* **Seuil de Taille de Segment (`MaxSegmentSize`)** :
  - Clôture automatique du segment actif lorsqu'il atteint la taille configurée (ex: 64 Mo).
  - Création transparente d'un nouveau segment `segment-<baseOffset>.wal` sans interrompre l'ingestion applicative.
* **Segments Immuables en Lecture Seule** :
  - Tout segment clôturé devient strictement en lecture seule, garantissant l'intégrité des données historiques face aux erreurs de concurrence.
* **Auto-Pruning Synchronisé sur le Checkpoint** :
  - Dès qu'un lot est acquitté par le sink aval et que le checkpoint avance, les segments dont tous les enregistrements sont validés (`MaxOffset < Checkpoint`) sont supprimés atomiquement (`os.Remove`).
  - Élimine tout risque de saturation disque sur les volumes NVMe contraints (pods Kubernetes, edge nodes).
* **Crash Recovery Multi-Segments Ordonné** :
  - Au redémarrage, le moteur inspecte, trie et valide tous les segments séquentiellement.
  - Réparation chirurgicale en cas d'écriture interrompue (*torn write*) sur la queue du segment actif.

---

### 3. Harnais d'Évaluation de Charge & Benchmarking Haute Cadence
* **Générateur de Charge Concurrente `loadgen` (`scripts/loadgen/`)** :
  - Binaire dédié permettant d'injecter des dizaines de milliers de requêtes concurrentes avec réglage précis des workers (`-c`), du volume (`-n`) et de la taille des payloads (`-payload-bytes`).
  - Mesure instantanée des latences p50, p90, p99 et du débit réel.
* **Script de Test Automatisé de Bout en Bout (`scripts/stress-test.sh`)** :
  - Démarre le sidecar, purge l'environnement, lance une charge massive, consomme le flux avec `walspool stream`, et vérifie l'intégrité des compteurs Prometheus.

---

### 4. Gouvernance & Registre des Décisions d'Architecture (ADRs)
Formalisation de la traçabilité technique dans `docs/architecture/adr/` :
* **[ADR-001](adr/ADR-001-multi-segment-rotation.md)** : *Multi-Segment Log Rotation and Checkpoint-Synchronized Auto-Pruning*.
* **[ADR-002](adr/ADR-002-standalone-cli-tui.md)** : *Standalone CLI with Zero-Dependency ANSI Double-Buffering & Security Sanitization*.
* **[ADR-003](adr/ADR-003-v2-high-throughput-and-community-scope.md)** : *V2 Target Architecture Blueprint, Ingestion Bottleneck Resolution & Community Scope*.

---

## 📊 Données de Performance Validées (Plateforme Intel Ultra 9 185H)

```
------------------------------------------------------------------------------------------------------
COMPOSANT / OPÉRATION                            DÉBIT MESURÉ               LATENCE MOYENNE
------------------------------------------------------------------------------------------------------
Hub Mémoire Ingestion (Ring Buffer O(1))         3 120 151 ops/s            401.3 ns / op
Hub Mémoire Ingestion Parallèle (16 cœurs)       2 775 534 ops/s            385.8 ns / op
Stockage WAL Fichier (Group Commit 128 Ko)       3 194 890 ops/s            467.2 ns / op
Stockage WAL Rotation de Segment                 20 132 rotations/s         72.5 µs / op
------------------------------------------------------------------------------------------------------
```

---

## 🛠️ Instructions de Compilation & Utilisation

### Compiler la CLI et le Sidecar
```bash
# Compilation de la CLI walspool
go build -o bin/walspool ./cmd/walspool

# Compilation du Sidecar daemon
go build -o bin/walspool-sidecar ./cmd/sidecar
```

### Lancer l'Observabilité en Direct
```bash
# Dans un premier terminal : démarrer le sidecar
bin/walspool-sidecar -listen :9099 -spool-dir /tmp/spool

# Dans un second terminal : ouvrir le dashboard interactif
bin/walspool monitor

# Ou streamer les logs d'un service spécifique en filtrant les erreurs
bin/walspool stream --service checkout --level error
```

---

## 🗺️ Aperçu de la Version V2 (Roadmap)
La prochaine itération introduira :
1. L'endpoint d'ingestion par lots `POST /v1/enqueue/batch` et le streaming continu NDJSON (`application/x-ndjson`).
2. La trame binaire V2 32 octets alignée 64 bits avec double CRC32C matériel (Castagnoli SSE4.2 / ARMv8) et auto-healing sans troncature.
3. Le Sparse Index 64 Ko divisant la mémoire d'index par 1 000.
4. Les raccourcis magiques `walspool dev` et `walspool demo`.
