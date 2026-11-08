# ADR-004 : Spool Sidecar Borné par Quota d'Octets Non Commités

* **Statut :** Accepté
* **Date :** Novembre 2026
* **Auteur :** Yohann Hommet & Conseil d'Architecture Walspool
* **Composant :** Sidecar d'ingestion (`cmd/sidecar/`) & Moteur de Stockage Disque (`storage_file.go`)
* **Ticket :** OPS-01

---

## 1. Contexte & Problématique

Le sidecar construisait son stockage avec le constructeur historique :

```go
storage, err := walspool.NewFileStorageEngine(cfg.DataDir, cfg.MaxRecords)
```

`DefaultFileStorageOptions()` laisse `MaxSegmentSize = 0`, ce qui désactive la rotation. `Commit()` n'élague donc jamais de segment clôturé. Deux conséquences en découlent :

1. **Croissance illimitée en régime sain** : même lorsque le sink acquitte les lots, `active.wal` conserve indéfiniment tous les enregistrements committés. L'index mémoire `records []offsetPos` croît avec le nombre total d'enregistrements de la durée de vie du processus, et le recovery reparcourt toute la WAL à chaque démarrage.
2. **Backlog non borné pendant une panne du sink** : `-max-records` est un quota de **nombre** (50 000 par défaut), pas un quota d'octets. Avec `MaxPayloadSize = 1 MiB`, le backlog non commité peut atteindre plusieurs dizaines de Go. La rotation seule ne borne pas ce backlog : elle ne borne que la rétention des données déjà committées.

## 2. Décision Architecturale

Nous décidons de combiner deux garde-fous complémentaires.

### 2.1. Taille de segment par défaut du sidecar

Le sidecar utilise désormais `NewFileStorageEngineWithOptions` avec un segment de 64 MiB :

```go
const defaultSidecarMaxSegmentSize int64 = 64 * 1024 * 1024

func sidecarStorageOptions(maxSpoolBytes int64) walspool.FileStorageOptions {
    opts := walspool.DefaultFileStorageOptions()
    opts.MaxSegmentSize = defaultSidecarMaxSegmentSize
    opts.MaxUncommittedBytes = maxSpoolBytes
    return opts
}
```

Aucun flag `-max-segment-size` n'est exposé dans cette version. 64 MiB est un défaut produit de classe etcd, et le défaut de la bibliothèque reste `0` pour préserver la compatibilité ADR-001. Un `active.wal` existant est découvert comme segment de base 0 par `recoverSegmented`, puis élagué après commit de ses enregistrements : aucune migration de données n'est nécessaire.

### 2.2. Quota d'octets non committés dans `FileStorageEngine`

`FileStorageOptions` gagne un champ additif :

```go
// MaxUncommittedBytes caps the serialized bytes of records that have not yet been
// committed. When <= 0, the byte quota is disabled (legacy behavior).
MaxUncommittedBytes int64
```

Sémantique :

- Le quota compte les **octets de trame WAL sérialisée** (`offsetPos.Length` = en-tête 29 octets + topic + payload). Il ne compte ni les octets de payload seuls, ni les octets committés, ni l'espace disque total.
- `Append` rejette avec `ErrSpoolFull` **avant toute écriture ou rotation** dès que le prochain enregistrement ferait dépasser le quota. Le garde-fou de nombre existant reste actif : le premier des deux qui déclenche gagne.
- `Commit` libère exactement la plage des trames nouvellement committées, après la réussite du renommage atomique du checkpoint. Un commit obsolète reste un no-op.
- Le recovery initialise le compteur depuis le checkpoint borné jusqu'à l'index récupéré. Une queue déchirée est tronquée avant indexation : ses octets ne sont jamais comptés.
- La rotation et l'élagage ne touchent jamais le compteur : ils ne concernent que les enregistrements sous le checkpoint.
- `rollbackTo` soustrait uniquement les trames abandonnées situées à ou au-dessus du checkpoint, avec un plancher à zéro défensif.
- `MaxUncommittedBytes <= 0` conserve le comportement historique de la bibliothèque. Le sidecar, lui, impose toujours une valeur strictement positive.

### 2.3. Configuration du sidecar

- Champ `MaxSpoolBytes int64` dans `SidecarConfig`.
- Flag `-max-spool-bytes`, variable d'environnement `WALSPOOL_MAX_SPOOL_BYTES`.
- Défaut : `1 << 30` (1 GiB).
- Validation : strictement supérieur à 0. Contrairement à l'option bibliothèque, le sidecar n'expose pas `0 = illimité`, car désactiver le quota réintroduirait le risque de backlog non borné.

La configuration effective d'admission est donc le minimum des deux garde-fous : nombre d'enregistrements non committés et octets non committés.

## 3. Options Concurrentes Écartées

1. **Taille de segment par défaut seule** : corrige la croissance des données committées et le temps de recovery, mais ne borne pas le backlog non commité pendant une panne du sink. Nécessaire mais insuffisant.
2. **Ajout d'un flag `-max-segment-size`** : c'est un bouton de réglage avancé légitime, mais il ne fixe pas le défaut et n'est pas un plafond disque. La valeur 64 MiB est retenue comme défaut produit, sans surface de configuration supplémentaire.
3. **Quota d'octets seul** : borne le backlog, mais laisse `active.wal` grandir indéfiniment en régime sain et ne réduit pas le temps de recovery. Insuffisant.
4. **Compaction sur fichier unique (`RESTART`/`TRUNCATE`)** : rejetée par ADR-001 §3.1, car elle bloque les écritures le temps de réécrire les données vivantes.
5. **Quota d'octets sur `MemoryStorageEngine`** : hors périmètre. Le sidecar utilise le stockage disque ; le port et le moteur mémoire restent inchangés.

## 4. Conséquences & Invariants de Sûreté

* **Disque borné approximativement** : l'espace utilisé par le spool du sidecar est majoré par la somme explicite :
  $$\text{Disque}_{\text{max}} \approx Q + S + M$$
  avec $Q$ le quota d'octets non committés ($Q \le 1\,\text{GiB}$), $S$ le segment actif partiellement committé ($S \le 64\,\text{MiB} + 1\,\text{enregistrement}$) et $M$ la taille de `checkpoint.meta`. L'espace disque total est donc borné par le quota non committé, plus la queue partiellement committée du segment actif, plus les métadonnées, et non par le quota seul. Il s'agit d'une approximation bornée, pas d'un quota strict du système de fichiers.
* **Pas de fausse garantie disque** : `-max-spool-bytes` borne les octets non committés, pas l'espace disque total. La documentation README l'indique explicitement.
* **Compatibilité bibliothèque** : `DefaultFileStorageOptions()` reste inchangé (`MaxSegmentSize = 0`, `MaxUncommittedBytes = 0`). Aucun appelant existant ne change de comportement.
* **Migration transparente** : après la première rotation, les fichiers sont des `segment-*.wal`. Un `active.wal` historique est lu et réutilisé, puis élagué. Les sauvegardes doivent inclure `segment-*.wal` et `checkpoint.meta`.
* **Backpressure inchangée** : `ErrSpoolFull` était déjà traduit en HTTP `503` avec `Retry-After: 1` dans les handlers. Le quota d'octets déclenche le même chemin, sans modification des handlers.
* **Compteur vérifié par senseurs** : les tests couvrent le rejet, la comptabilité exacte, le commit partiel, le commit obsolète, le recovery avec queue déchirée, le rollback sur échec d'écriture, le mode `<= 0 = illimité`, la surcharge d'en-tête, le cycle rotation/élagage/recovery segmenté et l'absence de débordement int64 près de `MaxInt64`.
