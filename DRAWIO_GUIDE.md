# 🎨 Guide d'Architecture & Index des Schémas Draw.io

Ce document détaille l'architecture globale, le fonctionnement interne de **Walspool** (vulgarisé pour les non-développeurs Go) et les flux de communication inter-services modélisés au format **Draw.io (`.drawio`)**.

---

## 1. Fichiers Draw.io Disponibles

Tous les diagrammes ont été générés au format XML Draw.io natif non compressé, 100% valides et éditables directement dans **VS Code** (extension Draw.io Integration), **diagrams.net** ou **Draw.io Desktop** :

| Fichier Draw.io | Contenu & Objectifs | Nombre de Pages |
| :--- | :--- | :---: |
| **[`master_observability_architecture.drawio`](file:///home/yohann/pilot4it/walspool/master_observability_architecture.drawio)** | **Classeur Maître regroupant l'intégralité des 6 planches sous forme d'onglets.** | **6 onglets** |
| **[`01_architecture_overview.drawio`](file:///home/yohann/pilot4it/walspool/01_architecture_overview.drawio)** | Vue d'ensemble conteneurs Docker, réseau bridge `172.99.0.0/24`, ports, reverse-proxy Nginx, passerelle vers l'hôte `172.99.0.1:9099` et débuffering SSE. | 1 page |
| **[`02_walspool_internals.drawio`](file:///home/yohann/pilot4it/walspool/02_walspool_internals.drawio)** | Fonctionnement interne de Walspool vulgarisé en 4 onglets : métaphores du Notaire et du Château d'eau, Ring Buffer $O(1)$, index inversés, moteur WAL disque et reprise sur panne. | 4 onglets |
| **[`03_trace_and_sse_lifecycle.drawio`](file:///home/yohann/pilot4it/walspool/03_trace_and_sse_lifecycle.drawio)** | Cycle de vie de bout en bout d'une requête en 12 étapes : corrélation par `x-request-id`, émission asynchrone non-bloquante et cascade Gantt Waterfall. | 1 page |
| **[`04_why_go_and_docker_microservices.drawio`](file:///home/yohann/pilot4it/walspool/04_why_go_and_docker_microservices.drawio)** | Justification technique Go (Go vs Node/Python) et modèles d'intégration microservices Docker. | 1 page |

---

## 2. Synthèse de la Planche 1 : Vue d'Ensemble & Orchestration Réseau

```
[Navigateur Web / Platform UI]
       │  (HTTPS / TLS 1.3 - Route /platform/)
       ▼
[docker-cms-1 (Nginx 1.25+)]
       │  (proxy_pass avec X-Accel-Buffering: no & proxy_buffering: off)
       ▼
[docker-api-gateway-1 (LoopBack 4 - Node.js)]  ◄───►  [docker-aipi-1 (Django - Python 3.12)]
       │ (x-request-id dans les headers HTTP)                │ (Worker démon asynchrone)
       │                                                     │
       └─────────────────────────┬───────────────────────────┘
                                 │
                   (Réseau Docker Bridge 172.99.0.0/24)
                                 ▼
                     [Passerelle Hôte 172.99.0.1:9099]
                                 │
                     [Démon Walspool Sidecar (Go)]
                     ├── Ingestion express (< 50µs)
                     ├── Ring Buffer RAM (50 000 logs, O(1))
                     ├── Index Trace ID & Service (< 1ms)
                     └── Disque WAL séquentiel avec CRC32
```

### Points Clés de l'Orchestration Réseau :
1. **Routage Nginx & SSE Anti-Buffering** : Par défaut, Nginx met en mémoire tampon les paquets HTTP jusqu'à 4 Ko avant de les envoyer au navigateur. Les directives `proxy_buffering off;` et `add_header X-Accel-Buffering "no";` forcent l'envoi immédiat de chaque log dès son émission.
2. **Communication Conteneur ➔ Hôte** : Les conteneurs Docker tournent sur le sous-réseau `172.99.0.0/24`. Pour joindre le démon Walspool tournant sur la machine hôte sans passer par `localhost` (qui pointerait vers l'intérieur du conteneur), la passerelle réseau par défaut `http://172.99.0.1:9099` est utilisée.
3. **Keepalive Ticker (10s)** : Pour éviter que les pare-feux d'entreprise ou les reverse-proxies ne ferment la connexion SSE après un délai d'inactivité, Walspool émet un battement de cœur `: keepalive\n\n` toutes les 10 secondes.

---

## 3. Synthèse de la Planche 2 : Walspool Vulgarisé (Comprendre sans connaître Go)

### Pourquoi « WAL-SPOOL » ?

* **WAL = Write-Ahead Log (*Le Notaire et le Grand Livre Inaltérable*)** :
  - Dans une base de données classique, écrire partout sur le disque prend du temps.
  - Walspool utilise le principe du **journal séquentiel** : chaque nouveau log est écrit immédiatement **à la fin du fichier** sur le disque SSD sans jamais déplacer la tête de lecture.
  - Chaque message est scellé par une somme de contrôle **CRC32** (une empreinte mathématique de 4 octets). Même en cas de coupure de courant brutale, le journal est intact et garanti non corrompu.

* **SPOOL = Réservoir Tampon (*Le Château d'Eau et l'Orage*)** :
  - Lorsqu'un serveur subit un pic soudain (ex: 10 000 requêtes/seconde), contacter directement un système de stockage distant (Elasticsearch, CloudWatch) peut ralentir ou bloquer les threads de l'application métier.
  - Le Spool agit comme un réservoir : il absorbe le pic en mémoire en **< 50 microsecondes** (`202 Accepted`), libère l'application instantanément, et expédie les données calmement en tâche de fond par paquets de 50 logs.

### Les 5 Organes Internes de Walspool :

| Organe | Métaphore | Rôle & Performance |
| :--- | :--- | :--- |
| **1. Guichet d'Ingestion** (`POST /v1/enqueue`) | Le Dépose-Minute Express | Valide le log et répond au microservice en **< 50 microsecondes**. |
| **2. Le Ring Buffer** (`MemoryLogHub`) | Le Carrousel à 50 000 Places | File circulaire allouée en RAM une fois pour toutes. Dès qu'un 50 001ème log arrive, le plus ancien est remplacé en **$O(1)$**. **Zéro fuite mémoire**. |
| **3. Les Index Inversés** | Les Tiroirs de Tri Postal | Tables de correspondance instantanées par `trace_id` et par `service`. Recherche chirurgicale exécutée en **< 1 milliseconde**. |
| **4. L'Antenne SSE** | La Radio en Direct | Dès qu'un log arrive, il est poussé instantanément aux navigateurs web ouverts sur la console d'observabilité. |
| **5. Le Camion de Drain** | La Livraison par Lots | En tâche de fond, regroupe les logs par paquets de 50 pour les archiver ou les expédier vers un stockage externe si configuré. |

---

## 4. Synthèse de la Planche 3 : Cycle de Vie d'une Requête Métier & Corrélation

Le cycle complet s'exécute selon une corrélation distribuée de bout en bout :

```
Navigateur                API Gateway                AIPI (Django)             Walspool             Console UI
    │                          │                          │                        │                    │
 1. │─── POST /ai/generate ───►│                          │                        │                    │
    │   (x-request-id absent)  │                          │                        │                    │
 2. │                          │── Génère Trace ID        │                        │                    │
    │                          │── AsyncLocalStorage.run  │                        │                    │
 3. │                          │──── POST /inference ────►│                        │                    │
    │                          │   (avec x-request-id)    │                        │                    │
 4. │                          │                          │── ContextVars.set()    │                    │
 5. │                          │                          │── Log("Start IA") ────►│                    │
 6. │                          │                          │   (Thread asynchrone)  │─── Push SSE ──────►│
    │                          │                          │                        │  (Affiche Span 1)  │
 7. │                          │◄─── 200 OK (185ms) ──────│                        │                    │
 8. │◄── 200 OK (210ms) ───────│                          │                        │                    │
 9. │                          │── Log("Gateway 210ms") ──────────────────────────►│                    │
10. │                          │                                                   │─── Push SSE ──────►│
    │                          │                                                   │  (Cascade Gantt)   │
```

---

## 5. Comment Consulter et Utiliser les Diagrammes

1. **Dans VS Code** :
   - Installez l'extension **Draw.io Integration** (d'Henning Dieterichs).
   - Cliquez simplement sur [`master_observability_architecture.drawio`](file:///home/yohann/pilot4it/walspool/master_observability_architecture.drawio) : le diagramme s'ouvre avec ses **6 onglets interactifs** en bas de fenêtre.

2. **Dans le Navigateur (En ligne)** :
   - Rendez-vous sur [app.diagrams.net](https://app.diagrams.net/).
   - Choisissez *« Ouvrir un diagramme existant »* et sélectionnez le fichier de votre choix.

3. **Export & Présentation** :
   - Vous pouvez exporter directement chaque planche au format **PNG HD (300 DPI)**, **SVG vectoriel** ou **PDF** pour intégration dans vos documentations techniques ou présentations d'équipe.

---

## 6. Pourquoi Walspool est Écrit en Go ? (Avantages, Inconvénients & Trade-offs)

Le choix de **Go (Golang)** pour le cœur de Walspool a été guidé par des contraintes système strictes : absorber des dizaines de milliers d'événements par seconde avec une empreinte mémoire plate et une latence d'ingestion sous les 50 microsecondes, sans bloquer les applications métiers.

```
                    ┌────────────────────────────────────────────────────────┐
                    │  LE DÉFI : Absorber 50 000 req/s à < 50µs d'ingestion   │
                    │  avec 10 000 clients SSE et < 20 Mo de RAM totale      │
                    └───────────────────────────┬────────────────────────────┘
                                                │
                 ┌──────────────────────────────┴─────────────────────────────┐
                 ▼                                                            ▼
    [ POURQUOI PAS NODE.JS ? ]                                   [ POURQUOI PAS PYTHON ? ]
    • Event Loop mono-thread :                                   • GIL (Global Interpreter Lock) :
      l'I/O disque et le parsing JSON                              concurrence CPU et réseau bloquée
      gèlent le traitement des flux SSE.                           sur un seul cœur processeur.
    • Consommation RAM élevée (> 150 Mo).                        • Latence d'ingestion élevée (> 5ms).
                 │                                                            │
                 └──────────────────────────────┬─────────────────────────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │    LE CHOIX DE GO     │
                                    │  (Performance Native) │
                                    └───────────────────────┘
```

### A. Les 4 Avantages Majeurs de Go pour Walspool

| Avantage | Explication Technique | Bénéfice Concret pour Pilot4IT |
| :--- | :--- | :--- |
| **1. Binaire Statique Autonome** | Go compile directement en code machine natif sans runtime externe (~15 Mo). | **Zéro dépendance sur le serveur hôte**. Pas besoin d'installer de JVM, d'interpréteur Python ou de Node.js. Démarre en moins de 10 millisecondes. |
| **2. Concurrence Ultra-Légère (Goroutines & Channels)** | Une goroutine Go démarre avec **2 Ko de pile mémoire** (contre 1 à 2 Mo pour un thread OS en Java/C++). | **10 000 connexions SSE simultanées consomment ~20 Mo de RAM**, là où une architecture multi-thread classique exigerait plusieurs gigaoctets. |
| **3. Latence & GC Prédictibles (< 1 ms)** | Le ramasse-miettes tricolore concurrent de Go s'exécute en continu avec des temps de pause sous la milliseconde. | **Aucun gel de flux**. Dans Walspool, le Ring Buffer réutilise la mémoire en place ($O(1)$), ce qui réduit la pression sur le ramasse-miettes à quasi zéro. |
| **4. I/O Séquentielles Système Directes** | Accès direct aux primitives système (`syscall`, `sync.RWMutex`, `os.File`). | **Écriture Append-Only ultra-rapide** sur disque NVMe avec calcul de CRC32 au vol sans aucune couche intermédiaire d'ORM ou de framework. |

### B. Les Inconvénients et Compromis Assumés

| Inconvénient | Impact Réel | Mesure d'Atténuation (Doctrine Black-Box) |
| :--- | :--- | :--- |
| **1. Barrière linguistique dans l'équipe** | L'équipe Pilot4IT est experte en **Python (Django)** et **TypeScript (Vue/Node)**. Contribuer au code interne de Walspool nécessite des compétences Go. | **Découplage Black-Box absolu** : Les développeurs n'ont jamais besoin de toucher au Go. Toute interaction se fait par des contrats HTTP standard (`POST /v1/enqueue` et `GET /v1/logs/stream`). |
| **2. Verbosité du contrôle d'erreurs** | Go impose de vérifier chaque retour d'erreur (`if err != nil`). Pas d'exceptions magiques. | Rend le code du démon extrêmement robuste et prévisible : aucun crash imprévu en production par exception non catchée. |
| **3. Présence d'un GC (vs Rust/C++)** | Contrairement à Rust, Go n'a pas de gestion mémoire manuelle sans GC. | Walspool alloue son buffer de 50 000 logs une fois pour toutes au démarrage. La réutilisation circulaire élimine les cycles d'allocation/désallocation continue. |

---

## 7. Implémentation du Système dans une Architecture Microservices avec Docker

### A. Les 3 Topologies de Déploiement Possibles

```
  TOPOLOGIE A (Dev Actuel)          TOPOLOGIE B (Docker Compose Recommandé)        TOPOLOGIE C (Kubernetes Pod)
┌───────────────────────────┐      ┌──────────────────────────────────────┐       ┌───────────────────────────┐
│ Machine Hôte Linux        │      │ Réseau Docker Bridge (docker-compose)│       │ Pod Kubernetes            │
│ [ Walspool Daemon :9099 ] │      │                                      │       │ ┌───────────────────────┐ │
│        ▲                  │      │  [ Service : walspool ]              │       │ │ Conteneur Microservice│ │
│        │ (172.99.0.1:9099)│      │  [ Image : pilot4it/walspool:latest ]│       │ └───────────┬───────────┘ │
│ ┌──────┴──────────────┐   │      │  [ Port interne : 9099              ]│       │             │ localhost   │
│ │ Conteneurs Docker   │   │      │            ▲                         │       │ ┌───────────▼───────────┐ │
│ │ (172.99.0.0/24)     │   │      │            │ (http://walspool:9099)  │       │ │ Conteneur Sidecar     │ │
│ └─────────────────────┘   │      │  ┌─────────┴─────────┐               │       │ │ Walspool (:9099)      │ │
│                           │      │  │ Microservices     │               │       │ └───────────────────────┘ │
│                           │      │  │ (gateway, aipi)   │               │       │                           │
└───────────────────────────┘      └──────────────────────────────────────┘       └───────────────────────────┘
```

#### Comparatif des Topologies :
1. **Topologie A (Démon Hôte partagé - Configuration actuelle de développement)** :
   * Walspool tourne directement sur la machine hôte (`:9099`).
   * Les conteneurs Docker (`172.99.0.0/24`) joignent le service via la passerelle par défaut `http://172.99.0.1:9099`.
   * **Avantage** : Performance disque brute native, isolation totale des conteneurs.
2. **Topologie B (Service conteneurisé dans `docker-compose.yml` - Recommandé en Staging/Production)** :
   * Définition d'un service `walspool` dans `docker-compose.yml` (image scratch/alpine de ~18 Mo).
   * Volume monté : `./data/spool:/data/spool` pour conserver les logs en cas de redémarrage du conteneur.
   * Communication inter-conteneurs par DNS interne Docker : `http://walspool:9099`.
3. **Topologie C (Sidecar Pattern Kubernetes)** :
   * Chaque Pod métier embarque un conteneur Walspool dédié partageant `localhost`.
   * **Avantage** : Isolation parfaite par tenant ou service critique, zéro latence réseau.

---

### B. Les 4 Règles d'Or de l'Intégration Microservices & Docker

#### Règle 1 : Découplage Black-Box Universel (Polyglot)
Aucun microservice ne doit inclure de bibliothèque propriétaire lourde. L'émission d'un log se résume à une simple requête HTTP standard :
```http
POST /v1/enqueue HTTP/1.1
Host: 172.99.0.1:9099
Content-Type: application/json

{
  "topic": "pilot4it.aipi.logs",
  "service": "aipi",
  "level": "info",
  "payload": {
    "trace_id": "c4a2e881-3b71-48f9-902e-1d5e3170a821",
    "message": "Calcul d'inférence achevé",
    "duration_ms": 185
  }
}
```

#### Règle 2 : Résilience Client-Side (Zéro Crash Applicatif)
Pour qu'un microservice ne soit jamais ralenti ni planté si Walspool est en cours de redémarrage ou indisponible :
- **En Python (AIPI)** : Utilisation d'une file d'attente bornée (`queue.Queue(maxsize=5000)`) et d'un thread démon (`WalspoolLogWorker`). Si la file est pleine ou le service inaccessible, le message est rejeté silencieusement sans jamais bloquer l'inférence.
- **En Node.js (API Gateway)** : Appel HTTP asynchrone non-bloquant avec gestionnaire d'erreur `.catch(() => {})` et timeout court (500ms).

#### Règle 3 : Directives Nginx Anti-Buffering pour le Streaming SSE
Dans une architecture conteneurisée avec Nginx en reverse proxy, Nginx retient par défaut les flux en mémoire tampon par blocs de 4 Ko. Pour garantir l'affichage en direct sans délai dans la console :
```nginx
location ^~ /api/v1/observability/logs/stream {
    proxy_pass              https://api-gateway:8443/api/v1/observability/logs/stream;
    proxy_set_header        Host $host;
    proxy_set_header        X-Forwarded-Ssl on;
    
    # INDISPENSABLE POUR LE STREAMING TEMPS RÉEL
    proxy_buffering         off;
    add_header              X-Accel-Buffering "no";
    proxy_read_timeout      3600s;
    proxy_send_timeout      3600s;
}
```

#### Règle 4 : Maintien des Sockets Réseau (Heartbeat Keepalive)
Les pare-feux cloud (AWS ALB, Cloudflare) et les proxies Docker coupent silencieusement les connexions TCP inactives après 60 secondes. Walspool intègre un ticker d'arrière-plan qui injecte un battement de cœur `: keepalive\n\n` toutes les 10 secondes pour maintenir le tuyau ouvert en continu.

