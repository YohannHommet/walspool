# Déroulé Type d'une Tâche : Issue #10 via Walspool Squad

Ce document illustre le déroulé complet de la résolution de l'Issue #10 par l'équipe multi-agents Walspool Squad.

---

## Étape 1 : Initialisation par l'Orchestrateur
```bash
# Vérification de la prochaine tâche sur l'Epic #27
$ python3 .agents/skills/walspool-squad/scripts/orchestrate.py next

# Sortie :
# Issue Number : #10
# Title : fix(sidecar): standardize default sidecar and client port to :9099

# Démarrage de la branche et de la session
$ python3 .agents/skills/walspool-squad/scripts/orchestrate.py start 10
# [Orchestrator] Creating and switching to branch: feat/issue-10-fix-sidecar-standardize-default-sidecar
# [Orchestrator] Session artifact directory ready: .walspool/squad/issue-10
```

---

## Étape 2 : Spécification (Agent 1 : Spec Analyst)
L'Agent 1 analyse le code existant dans `cmd/sidecar/main.go` et `cmd/walspool/client.go`.
Il constate l'asymétrie : le sidecar écoute sur `:9099`, mais le client CLI interroge `:9090` par défaut.
Il rédige `.walspool/squad/issue-10/01_spec.md` :
- Unification stricte sur `:9099`.
- Variable d'environnement de fallback `WALSPOOL_ADDR` supportée.
- Critère DoD : `walspool stream` et `walspool monitor` se connectent au sidecar sans argument d'adresse.

---

## Étape 3 : Plan d'Implémentation (Agent 2 : Architect Planner)
L'Agent 2 rédige `.walspool/squad/issue-10/02_plan.md` :
- Fichiers cibles :
  - `[MODIFY] cmd/walspool/client.go` (changer `DefaultAddr = "127.0.0.1:9090"` en `"127.0.0.1:9099"`).
  - `[MODIFY] cmd/walspool/client_test.go` (mettre à jour les tests unitaires).
- Stratégie TDD :
  1. Red : écrire un test vérifiant que `DefaultClientConfig().ServerAddr` vaut `"127.0.0.1:9099"`.
  2. Green : modifier la constante dans `client.go`.
  3. Refactor : vérifier les docstrings et exemples.

---

## Étape 4 : Porte de Validation Humaine
L'orchestrateur présente le plan synthétique :
> "Analyse et plan prêts pour l'Issue #10 (unification du port :9099 dans client.go). Donnez votre feu vert ("Go")."
L'utilisateur valide ("Go").

---

## Étape 5 : Implémentation TDD (Agent 3 : TDD Implementer)
1. **Red** : modification du test d'assertion d'adresse par défaut. Exécution :
   `go test -v ./cmd/walspool/client_test.go` -> Échec attendu.
2. **Green** : modification de la constante dans `client.go`. Exécution :
   `go test -v ./cmd/walspool/client_test.go` -> PASS.
3. **Refactor** : nettoyage des imports et vérification de la cohérence.

---

## Étape 6 : Capteurs & Vérification (Agent 4 : Sensor Verifier)
```bash
$ python3 .agents/skills/walspool-squad/scripts/orchestrate.py verify
# ==> [walspool] Running Go static analyzer (vet)...
# ==> [walspool] Running unit tests...
# ok github.com/YohannHommet/walspool
# ok github.com/YohannHommet/walspool/cmd/sidecar
# ok github.com/YohannHommet/walspool/cmd/walspool
# ==> [walspool] All verification checks passed cleanly.
# [Orchestrator] Sensor PASSED cleanly.
```

---

## Étape 7 : Validation Qualité (Agent 5 : Quality Validator)
```bash
$ python3 .agents/skills/walspool-squad/scripts/quality_checker.py main
# [walspool-quality-validator] PASS: 0 forbidden moves detected across diff.
```

---

## Étape 8 : Rétrospective & Clôture (Agent 6 : Retro Specialist)
1. Rétrospective : aucun manque de capteur détecté, le bogue venait d'une divergence historique entre deux sous-paquets.
2. Clôture automatisée :
```bash
$ python3 .agents/skills/walspool-squad/scripts/orchestrate.py complete 10
# [Orchestrator] Closing GitHub Issue #10...
# [Orchestrator] Updating task checkbox in Epic #27...
# [Orchestrator] Epic #27 checkbox successfully marked [x] for #10.
# [Orchestrator] Merging feat/issue-10-... into main...
# [Orchestrator] Pushing main to origin...
# [Orchestrator] Deleting feature branch...
# [Orchestrator] SUCCESS: Issue #10 fully closed!
```
