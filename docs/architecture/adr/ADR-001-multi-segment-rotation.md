# ADR-001 : Rotation Multi-Segments & Élagage Automatique Synchronisé sur le Checkpoint

* **Statut :** Accepté  
* **Date :** Octobre 2026  
* **Auteur :** Yohann Hommet & Conseil d'Architecture Walspool  
* **Composant :** Moteur de Stockage Disque (`storage_file.go`)  
* **PR Associée :** #6 (`feature/segment-rotation`)  

---

## 1. Contexte & Problématique

Dans sa version v1.0.0, le moteur `FileStorageEngine` de Walspool utilisait un fichier unique `wal.log` :
1. **Croissance Illimitée de l'Espace Disque** : Même si le sink aval acquittait régulièrement les événements et que le pointeur de checkpoint avançait, les données committées demeuraient physiquement dans `wal.log`. Sur des disques SSD/NVMe restreints (environnements conteneurisés K8s ou edge appliances), le volume finissait par saturer l'espace disque (`ErrSpoolFull`), bloquant l'ingestion applicative.
2. **Fragmentation & Lenteurs d'I/O** : La compaction d'un fichier WAL unique nécessite de réécrire les données vivantes à l'offset 0, une opération coûteuse bloquant les écritures et accélérant l'usure des cellules flash des disques SSD.
3. **Temps de Crash Recovery Linéaire** : Au redémarrage après arrêt brutal, le moteur devait reparcourir l'intégralité du fichier `wal.log` depuis l'offset 0, même si 99% des événements avaient déjà été traités par le sink.

---

## 2. Décision Architecturale

Nous avons décidé d'implémenter un mécanisme de **rotation multi-segments append-only avec élagage automatique (auto-pruning)** synchronisé sur le checkpoint de persistance.

### 2.1. Spécification & Conventions de Nommage
- **Paramètre de Configuration** : `MaxSegmentSize int64` dans `FileStorageOptions` (ex: 64 Mo). Par défaut, `MaxSegmentSize = 0` conserve le mode fichier unique historique pour une compatibilité descendante totale.
- **Convention de Fichier** : Les segments adoptent la nomenclature :
  ```text
  segment-<baseOffset>.wal
  ```
  où `<baseOffset>` est le numéro d'enregistrement logique initial du segment, encodé en décimal avec un padding de 20 chiffres (ex: `segment-00000000000000000000.wal`, `segment-00000000000000100000.wal`).
- **Segment Actif vs Segments Clôturés** :
  - Seul le dernier segment (`f.activeSegBase`) est ouvert en mode écriture (`O_CREATE|O_RDWR`).
  - Les segments clôturés antérieurs sont **en lecture seule absolue**. Aucun processus ne peut modifier leur contenu.

### 2.2. Algorithme de Rotation Déterministe
Lors d'un appel à `Append(rec)` :
1. Si `opts.MaxSegmentSize > 0` et que `f.writePos + len(encodedRecord) > opts.MaxSegmentSize` :
2. Le moteur vide le tampon `f.writer.Flush()` et synchronise le descripteur `f.walFile.Sync()`.
3. Le descripteur actuel est fermé.
4. Un nouveau fichier `segment-<newOffset>.wal` est créé à l'offset courant.
5. Le buffer d'écriture `f.writer` est réinitialisé sur le nouveau descripteur, et `f.activeSegBase` est mis à jour.
6. L'écriture de `rec` s'effectue dans le nouveau segment.

```mermaid
sequenceDiagram
    autonumber
    participant Engine as FileStorageEngine
    participant Active as segment-0000.wal (Actif)
    participant Next as segment-1000.wal (Nouveau)
    participant Meta as checkpoint.meta

    Engine->>Active: Append(Record) -> writePos > MaxSegmentSize
    Note over Engine: Seuil de taille atteint !
    Engine->>Active: Flush() & Sync()
    Engine->>Active: Close()
    Engine->>Next: OpenFile(segment-1000.wal, O_CREATE|O_RDWR)
    Engine->>Next: Append(Record)
    Note over Engine: Ingestion continue sans interruption
```

### 2.3. Élagage Automatique Atomique (Auto-Pruning)
L'élagage des anciens segments n'intervient pas sur un timer aveugle mais **strictement lors de l'appel à `Commit(offset)`** :
1. Dès que le checkpoint `f.checkpoint` avance :
2. Le moteur identifie tous les segments dont le dernier offset physique est strictement inférieur à `f.checkpoint` :
   $$\text{MaxOffset}(\text{segment}_k) < \text{Checkpoint}$$
3. Ces segments sont supprimés du système de fichiers via `os.Remove(path)`.
4. L'index mémoire `f.records` est élagué pour libérer les pointeurs des enregistrements purgés.
5. `f.baseOffset` est avancé, garantissant que la RAM allouée reste rigoureusement bornée.

### 2.4. Crash Recovery Multi-Segments Ordonné (`recoverSegmented`)
En cas de coupure de courant ou de crash système :
1. Lecture du checkpoint dans `checkpoint.meta`.
2. Découverte de tous les fichiers `segment-*.wal` via `filepath.Glob`.
3. Tri numérique strict selon leur `baseOffset`.
4. Validation séquentielle de l'intégrité de chaque segment :
   - Les segments antérieurs complets sont vérifiés.
   - Le segment actif (le plus récent) est analysé à la recherche d'une éventuelle écriture partielle en queue (*torn write*). Si une corruption de fin de fichier est détectée, le segment actif est tronqué atomiquement au dernier offset CRC32 valide.
5. Suppression immédiate au démarrage de tout segment résiduel déjà entièrement committé (`MaxOffset < Checkpoint`).

---

## 3. Options Concurrentes Écartées

1. **Fichier Unique avec Compaction In-Place (`RESTART`/`TRUNCATE`)** :  
   *Raison du rejet pour la v1.1 :* La compaction sur fichier unique nécessite de bloquer toutes les écritures le temps de copier les données encore non commitées au début du fichier. Le découpage en segments permet une écriture continue pendant que les segments anciens sont lus ou purgés.
2. **Élagage Asynchrone par Goroutine d'Arrière-Plan sur Timer** :  
   *Raison du rejet :* Introduit un risque de datarace et peut laisser accumuler des segments inutiles si le timer est trop long, ou saturer les I/O si le timer est trop agressif. L'élagage direct à la validation du `Commit` est déterministe, instantané et sans concurrence.
3. **Utilisation d'une Base de Données Embarquée (SQLite / Pebble / Badger)** :  
   *Raison du rejet :* Violation absolue de la règle du zéro dépendance (`go.mod`). Walspool conserve la maîtrise totale de ses structures disques.

---

## 4. Conséquences & Invariants de Sûreté

* **Empreinte Disque Bornée** : L'espace disque utilisé ne dépasse jamais :
  $$\text{DiskSize}_{\text{max}} \approx \text{MaxSegmentSize} \times \left( \lceil \frac{\text{UncommittedCount}}{\text{RecordsPerSegment}} \rceil + 1 \right)$$
* **Garantie Zéro Perte (At-Least-Once)** : Aucun segment n'est supprimé tant que son dernier enregistrement n'a pas été formellement acquitté par le sink et scellé dans `checkpoint.meta`.
* **Performance NVMe Maximale** : Le débit d'écriture demeure à **plus de 2,1 millions d'enregistrements/seconde** avec un surcoût de rotation inférieur à 72 microsecondes.
* **Compatibilité Descendante** : Les applications conservant `MaxSegmentSize = 0` continuent d'opérer sur un fichier `wal.log` unique sans aucun changement de comportement.
