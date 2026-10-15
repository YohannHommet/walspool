# 🏛️ Registre des Décisions d'Architecture (ADR) — Walspool

Ce répertoire consigne l'ensemble des **Architecture Decision Records (ADR)** de Walspool.  
Chaque enregistrement capture une décision architecturale structurante, son contexte, ses justifications techniques, les options concurrentes écartées et ses conséquences sur la base de code.

Le format adopté suit le standard de **Michael Nygard** (Titre, Statut, Contexte, Décision, Conséquences).

---

## 📑 Index des Décisions

| Référence | Titre | Statut | Date | Portée |
| :--- | :--- | :---: | :---: | :--- |
| **[ADR-001](ADR-001-multi-segment-rotation.md)** | **Rotation Multi-Segments & Élagage Automatique Synchronisé sur le Checkpoint** | `Accepté` | Oct. 2026 | Cœur WAL Disque (`storage_file.go`) |
| **[ADR-002](ADR-002-standalone-cli-tui.md)** | **CLI Autonome `walspool` avec TUI Pur Go sans Dépendance & Défense contre Injections ANSI** | `Accepté` | Oct. 2026 | Interface CLI (`cmd/walspool/`) |
| **[ADR-003](ADR-003-v2-high-throughput-and-community-scope.md)** | **Blueprint Cible V2 : Résolution du Goulot d'Ingestion & Arbitrage du Périmètre Community** | `Accepté` | Oct. 2026 | Dual-Engine, Ingestion & Produit |

---

## 🎯 Principes Directeurs des Décisions Walspool

1. **Zéro Dépendance Externe (`go.mod`)** :  
   Walspool n'admet aucune bibliothèque tierce dans son cœur ni dans sa CLI. Tout composant doit être conçu à partir de la bibliothèque standard Go ou d'algorithmes propriétaires ultra-optimisés.
2. **Doctrine Boîte Noire Profonde (David Parnas, John Ousterhout)** :  
   Une interface publique minimale et stable dissimulant une implémentation profonde, hautement optimisée et insubmersible.
3. **Souveraineté Locale & Résilience Absolue** :  
   Walspool doit être le shock absorber le plus fiable au monde sur un nœud unique ou en conteneur edge.
4. **Frontière Commerciale Saine** :  
   L'édition Community (FSL-1.1-MIT) est complète, sans nagware ni limitations artificielles. La valeur des éditions commerciales réside dans le multi-nœuds, le cloud analytique à l'échelle et les intégrations transactionnelles d'entreprise.
