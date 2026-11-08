# Delegation Contract

Delegated work is checkpointed, not open-ended. This contract exists so the Lead
can detect a stalled teammate from the filesystem, not from a vague "still
running" status.

## 1. Checkpoints by role

| Role | First artifact | Substantive checkpoint | Hard stop |
|---|---|---|---|
| Architect | T+5 min: decision skeleton | T+12 min: chosen option, files/API, tests, risks | T+20 min |
| Planner | T+5 min: file manifest and test list | T+10 min: ordered TDD steps and risks | T+15 min |
| TDD implementer | T+10 min: first failing test plus RED output | T+25 min: code green, targeted tests | T+40 min |
| Reviewer | T+5 min: verdict skeleton | T+15 min: findings and sensor results | T+20 min |

The architect skeleton is deliberately cheap:

```markdown
# Decision
One paragraph.

## Chosen option
## Rejected options
## Files/API affected
## Tests
## Risks
## Open questions
```

A title-only file is not a checkpoint. It must contain the required sections and
at least one concrete decision, file path, or test name.

## 2. Mandatory prompt block

Prepend this block to every delegated prompt, filling the role-specific values:

```text
DELEGATION CONTRACT
- Artifact: <absolute path>
- First checkpoint (T+5m): non-empty skeleton with <required headings/content>
- Heartbeat: append one line to <artifact>.progress at least every 3 minutes:
  YYYY-MM-DDTHH:MM:SSZ <phase or blocker>
- Substantive checkpoint (T+<N>m): <required content>
- Hard stop: T+<N>m. The Lead will interrupt if the first checkpoint is missing
  at T+7m, or if the artifact has not materially changed by T+<N>m.
- If blocked: write "BLOCKED: <reason>" to the artifact and message the Lead
  immediately. Do not silently continue reading.
- Read scope: <paths>. Write scope: <paths>. No production edits outside it.
```

The heartbeat is required because "the agent is still running" is not evidence of
progress. File size and modification time are.

## 3. Lead watchdog

At spawn time, record `T0`, the expected artifact path, the required headings, and
the heartbeat path. Then:

1. Wait in 2-minute `wait_agent` slices. Do not busy-poll.
2. After each wait, run:

   ```bash
   ./scripts/artifact-check.sh <path> --max-age 300 --require '<heading>'
   ```

3. At T+5 min, if the skeleton is missing or empty:
   - Send one nudge: "Write the skeleton now; do not expand research first."
   - Start a 2-minute grace period.
4. At T+7 min, if the skeleton is still missing:
   - Interrupt the teammate.
   - Reassign the task to the Lead or a replacement.
   - Preserve the task id and revision; do not create a duplicate task.
5. At the substantive checkpoint, if the artifact is still a skeleton, send a
   progress warning and require a concrete plan to finish within the hard stop.
6. At the hard stop, interrupt and reassign or finish the artifact as Lead.

Never wait more than 10 minutes for a read-only artifact without a nudge or an
interrupt.

## 4. Reassignment protocol

When an agent misses T+7:

- Keep the same task id; use `team_task_update` with `reassign` to the Lead or a
  replacement.
- Preserve any skeleton and write `interrupted: no artifact at T+7m` in the task
  description.
- Give the replacement the skeleton path, the exact missing section, a narrower
  scope, and a new hard stop.
- Do not silently retry the same unbounded prompt.

## 5. Anti-gaming checks

A checkpoint passes only when all of these are true:

- the path exists and is a regular non-empty file;
- its modification age is within the allowed window;
- the required headings are present;
- at least one concrete decision/file/test is present, not only a title;
- the agent wrote only inside its declared write scope.

The `artifact-check.sh` script checks the first three mechanically; the Lead
checks the last two by reading the artifact.

## 6. Metrics

Record per delegation:

- time to first artifact;
- time to substantive artifact;
- interrupt count and reassignment count;
- review findings and rework cycles.

If a role misses its first checkpoint twice in a row, tighten the prompt or
shorten the scope. The contract is a sensor, not a punishment.
