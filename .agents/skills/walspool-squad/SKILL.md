---
name: walspool-squad
description: Orchestrates a dedicated crew of 6 specialized AI agents (Spec Analyst, Architect Planner, TDD Craftsman, Sensor Verifier, Quality Validator, and Retro Specialist) to sequentially implement roadmap issues for Walspool tracked in GitHub Epic #27. Use when implementing Walspool roadmap issues.
---

# Walspool Squad : Équipe Multi-Agents d'Ingénierie Autonome

Le skill `walspool-squad` structure l'implémentation de la feuille de route de **Walspool** en orchestrant une équipe de 6 sous-agents spécialisés. Il implémente les patterns *Orchestrator-Workers* et *Evaluator-Optimizer* (Anthropic), la génération TDD (Codex), et l'ingénierie de harnais déterministe (DeepMind / Antigravity).

---

## Architecture de l'Équipe

```text
               [GitHub Epic #27]
                       │
             ┌─────────▼─────────┐
             │ Squad Lead (Orch) │
             └─────────┬─────────┘
                       │
  ┌────────────────────┼────────────────────┐
  │ 1. Spécification   │ 2. Architecture    │
  ▼                    ▼                    ▼
[Spec Analyst]  →  [Architect]  →  [Porte Validation Humaine]
                                            │ "Go"
  ┌─────────────────────────────────────────┘
  │
  │ 3. Implémentation TDD
  ▼
[TDD Implementer] ◄─── (Erreur capteur : max 3 itérations)
  │                    │
  │ 4. Vérification    │
  ▼                    │
[Sensor Verifier] ─────┘ (Échec)
  │ (Succès)
  │ 5. Validation Qualité
  ▼
[Quality Validator] ───► (Rejet Forbidden Moves) ───► [TDD Implementer]
  │ (Conforme)
  │ 6. Rétrospective & Clôture
  ▼
[Retro Specialist]
  │
  ▼
[Merge main & Push origin]
```

### Roster des Sous-Agents
1. **Spec & Requirements Analyst** (`walspool-spec-analyst`) : Analyse du besoin, invariants système, zéro allocation, conformité Community. Fiche : [agents_manifest.md](./references/agents_manifest.md#2-agent-1--spec--requirements-analyst-walspool-spec-analyst).
2. **Implementation Architect & Planner** (`walspool-architect-planner`) : Conception chirurgicale, alignement 64-bit, découpage TDD. Fiche : [agents_manifest.md](./references/agents_manifest.md#3-agent-2--implementation-architect--planner-walspool-architect-planner).
3. **TDD Software Craftsman** (`walspool-tdd-implementer`) : Boucle Red-Green-Refactor en pur Go stdlib (`CGO_ENABLED=0`). Fiche : [agents_manifest.md](./references/agents_manifest.md#4-agent-3--tdd-software-craftsman-walspool-tdd-implementer).
4. **Sensor & Test Verifier** (`walspool-sensor-verifier`) : Exécution de `./scripts/verify.sh` et benchmarks. Fiche : [agents_manifest.md](./references/agents_manifest.md#5-agent-4--sensor--test-verifier-walspool-sensor-verifier).
5. **Quality & Security Gate Validator** (`walspool-quality-validator`) : Détection des Forbidden Moves via [quality_checker.py](./scripts/quality_checker.py). Fiche : [agents_manifest.md](./references/agents_manifest.md#6-agent-5--quality--security-gate-validator-walspool-quality-validator).
6. **Harness Retro & Knowledge Engineer** (`walspool-retro-specialist`) : Rétrospective, mise à jour des règles du dépôt, clôture de l'issue et commit. Fiche : [agents_manifest.md](./references/agents_manifest.md#7-agent-6--harness-retro--knowledge-engineer-walspool-retro-specialist).

---

## Protocole d'Exécution Étape par Étape

Lorsque vous êtes sollicité pour implémenter une issue ou faire avancer la roadmap :

### 1. Diagnostic de Roadmap & Initialisation
Exécutez le script d'orchestration pour identifier la prochaine issue non terminée :
```bash
python3 .agents/skills/walspool-squad/scripts/orchestrate.py next
```
Initialisez la branche git dédiée et l'espace de session :
```bash
python3 .agents/skills/walspool-squad/scripts/orchestrate.py start <ISSUE_NUMBER>
```

### 2. Phase 1 : Spécification (Spec Analyst)
- Lisez le code existant relatif à l'issue.
- Formulez les critères d'acceptation et les contraintes de sympathie mécanique.
- Rédigez `.walspool/squad/issue-<ID>/01_spec.md`.

### 3. Phase 2 : Architecture (Architect Planner)
- Établissez le plan chirurgical des fichiers impactés ([MODIFY], [NEW]).
- Définissez le premier test unitaire RED à écrire.
- Rédigez `.walspool/squad/issue-<ID>/02_plan.md`.

### 4. Porte de Validation Humaine (Squad Lead -> Utilisateur)
- Présentez une synthèse concise (3-5 lignes) du besoin et du plan technique à l'utilisateur.
- **Attendez explicitement le feu vert de l'utilisateur ("Go") avant d'engager la moindre modification de code**.

### 5. Phase 3 : Implémentation TDD (TDD Implementer)
- **Red** : Rédigez le test unitaire provoquant l'échec ou affirmant le nouveau comportement. Lancez le test et vérifiez l'échec effectif.
- **Green** : Écrivez l'implémentation minimale requise pour faire passer le test.
- **Refactor** : Nettoyez et optimisez selon les standards idiomatiques Go.

### 6. Phase 4 : Capteurs de Vérification (Sensor Verifier)
Exécutez le capteur global :
```bash
python3 .agents/skills/walspool-squad/scripts/orchestrate.py verify
```
Si le capteur échoue, renvoyez l'erreur concise au TDD Implementer (boucle limitée à 3 tentatives).

### 7. Phase 5 : Porte de Validation Qualité (Quality Validator)
Exécutez le validateur de qualité :
```bash
python3 .agents/skills/walspool-squad/scripts/quality_checker.py main
```
Vérifiez l'absence absolue de tout em-dash, test sauté (`t.Skip`), commentaire TODO, ou dépendance `go.mod` superflue.

### 8. Phase 6 : Rétrospective & Fusion (Retro Specialist)
- Analysez le cycle : y a-t-il un axe d'amélioration pour le harnais (`AGENTS.md`) ?
- Clôturez l'issue GitHub, cochez la case dans l'Epic #27, et fusionnez la branche sur `main` :
```bash
python3 .agents/skills/walspool-squad/scripts/orchestrate.py complete <ISSUE_NUMBER>
```

---

## Outils & Scripts Inclus
- [orchestrate.py](./scripts/orchestrate.py) : Gestionnaire CLI de l'Epic #27, des branches et des transitions.
- [quality_checker.py](./scripts/quality_checker.py) : Scanner statique de conformité et de détection des Forbidden Moves.
- [agents_manifest.md](./references/agents_manifest.md) : Fiches complètes des 6 sous-agents.
- [prompts_templates.md](./references/prompts_templates.md) : Prompts prêts à l'emploi pour chaque sous-agent.
- [issue_10_dry_run.md](./examples/issue_10_dry_run.md) : Exemple pas à pas de l'Issue #10.

---

## Common Rationalizations

| Rationalization | Reality |
|---|---|
| "I can skip the human validation gate for small issues" | The Human Gate before TDD implementation is mandatory. Skipping it risks architectural rework and wasted tokens. |
| "I will write the implementation and then add tests" | Strict TDD (Red -> Green -> Refactor) is non-negotiable. Tests prove the bug/feature before code is modified. |
| "A full test suite pass is enough; I don't need the quality checker" | Tests only check logic; `quality_checker.py` catches forbidden moves, t.Skip, and architectural leaks. |

## Red Flags

- Modifying code before the user has explicitly confirmed "Go" at the Human Validation Gate
- Bypassing `orchestrate.py verify` or ignoring `./scripts/verify.sh`
- Introducing third-party dependencies into the Walspool core engine (`ports.go` / `engine.go`)
- Committing code with skipped tests (`t.Skip`) or unresolved `TODO` comments
- Iterating more than 3 times on a failed TDD cycle without stopping to consult the user

## Verification

Before declaring an issue complete:

- [ ] Issue specification and architectural plan approved by the user
- [ ] Failing test written first (Red) and observed failing
- [ ] Implementation passes all unit tests and benchmarks (`verify.sh`)
- [ ] `quality_checker.py` reports zero forbidden moves
- [ ] Issue closed and branch cleanly merged into `main` via `orchestrate.py complete`
