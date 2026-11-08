#!/usr/bin/env bash
# Bounded feedback loop for the Walspool verification sensor.
#
# Runs ./scripts/verify.sh, captures a compact failure signature, and writes a
# machine-readable feedback artifact under .walspool/feedback/. Repeated
# identical failures escalate to a harness-patch instruction instead of letting
# an agent re-prompt or retry the same code change indefinitely.
set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

STATE_DIR="${WALSPOOL_FEEDBACK_DIR:-$PROJECT_ROOT/.walspool/feedback}"
HISTORY="$STATE_DIR/history.tsv"
LATEST="$STATE_DIR/latest.md"
VERIFY_LOG="$STATE_DIR/verify.log"
ESCALATION="$STATE_DIR/escalation.md"
MAX_ATTEMPTS=3
BENCH=0

usage() {
  cat <<'EOF'
Usage: scripts/feedback-loop.sh [--max-attempts N] [--bench] [--reset]

Runs the project verification sensor and records a bounded feedback artifact.
  --max-attempts N  Escalate after N identical failures (default: 3)
  --bench           Pass --bench to the verification sensor
  --reset           Clear the feedback state directory and exit

Environment:
  WALSPOOL_FEEDBACK_DIR  State directory (default: .walspool/feedback)
  WALSPOOL_VERIFY_CMD    Sensor command override (used by the self-test)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-attempts)
      if [[ $# -lt 2 || ! "$2" =~ ^[1-9][0-9]*$ ]]; then
        echo "feedback-loop: --max-attempts requires a positive integer" >&2
        exit 2
      fi
      MAX_ATTEMPTS="$2"
      shift 2
      ;;
    --bench)
      BENCH=1
      shift
      ;;
    --reset)
      rm -rf "$STATE_DIR"
      echo "[feedback] State directory cleared: $STATE_DIR"
      exit 0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "feedback-loop: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$STATE_DIR" "$STATE_DIR/tmp" "$STATE_DIR/cache"
export TMPDIR="${TMPDIR:-$STATE_DIR/tmp}"
export GOCACHE="${GOCACHE:-$STATE_DIR/cache}"
touch "$HISTORY"
if [[ $(wc -l <"$HISTORY") -gt 1500 ]]; then
  tail -n 1000 "$HISTORY" > "$HISTORY.tmp" && mv "$HISTORY.tmp" "$HISTORY"
fi

if [[ -n "${WALSPOOL_VERIFY_CMD:-}" ]]; then
  # The override is intentionally split so the self-test can pass a command
  # plus arguments without eval.
  read -r -a VERIFY_CMD <<< "$WALSPOOL_VERIFY_CMD"
  if [[ "${#VERIFY_CMD[@]}" -eq 0 ]]; then
    echo "feedback-loop: WALSPOOL_VERIFY_CMD cannot be empty or whitespace" >&2
    exit 2
  fi
else
  VERIFY_CMD=("$PROJECT_ROOT/scripts/verify.sh")
fi
if [[ "$BENCH" -eq 1 ]]; then
  VERIFY_CMD+=("--bench")
fi

start=$SECONDS
"${VERIFY_CMD[@]}" >"$VERIFY_LOG" 2>&1
status=$?
duration=$((SECONDS - start))
commit="$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cmd_string="${VERIFY_CMD[*]}"

if [[ "$status" -eq 0 ]]; then
  rm -f "$ESCALATION"
  {
    echo "# Harness Feedback"
    echo
    echo "- Status: PASS"
    echo "- Commit: \`$commit\`"
    echo "- Command: \`$cmd_string\`"
    echo "- Duration: ${duration}s"
    echo "- Log: \`$VERIFY_LOG\`"
    echo
    echo "The sensor is green. No feedback action is required."
  } >"$LATEST"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$now" "PASS" "$commit" "PASS" "1" "$duration" "$cmd_string" >>"$HISTORY"
  echo "[feedback] PASS in ${duration}s: $LATEST"
  exit 0
fi

# Keep the feedback bounded. Prefer compiler/test failure lines, then fall back
# to the tail of the log if the sensor failed without recognizable markers.
failures="$(grep -E '^(--- FAIL:|FAIL[[:space:]]|panic:|# |[[:space:]]*[^[:space:]].*\.go:[0-9]+(:[0-9]+)?: )' "$VERIFY_LOG" | head -n 25)" || failures=""
if [[ -z "$failures" ]]; then
  failures="$(tail -n 25 "$VERIFY_LOG")" || failures=""
fi
if [[ -z "$failures" ]]; then
  failures="(sensor produced no output)"
fi

# Normalize durations, hex addresses, and temporary paths before hashing so the
# same failure gets the same signature across attempts.
normalized="$(printf '%s\n' "$failures" \
  | sed -E "s/\([0-9]+(\.[0-9]+)?s\)/()/g; s/[[:space:]][0-9]+(\.[0-9]+)?s$//g; s/0x[0-9a-fA-F]+/0xADDR/g; s#$PROJECT_ROOT/tmp/[^[:space:]]*#REPO_TMP#g; s#$STATE_DIR/tmp/[^[:space:]]*#STATE_TMP#g; s#/tmp/[^[:space:]]*#/tmp/...#g; s/[[:space:]]+$//" \
  | sort -u)"
signature="$(printf '%s\n' "$normalized" | cksum | awk '{print $1 "-" $2}')"

# Count consecutive failures with the same signature (resetting on PASS or differing failure).
prior="$(awk -F'\t' -v sig="$signature" '
  $2 == "PASS" { count = 0; next }
  $4 == sig { count++ }
  $4 != sig { count = 0 }
  END { print count + 0 }
' "$HISTORY" 2>/dev/null || printf '0')"
attempt=$((prior + 1))

rm -f "$ESCALATION"
if [[ "$attempt" -ge "$MAX_ATTEMPTS" ]]; then
  phase="ESCALATE"
else
  phase="FAIL"
fi

{
  echo "# Harness Feedback"
  echo
  echo "- Status: $phase"
  echo "- Attempt: $attempt/$MAX_ATTEMPTS"
  echo "- Signature: \`$signature\`"
  echo "- Commit: \`$commit\`"
  echo "- Command: \`$cmd_string\`"
  echo "- Duration: ${duration}s"
  echo "- Log: \`$VERIFY_LOG\`"
  echo
  echo "## First failures"
  echo
  echo '```text'
  printf '%s\n' "$failures"
  echo '```'
  echo
  echo "## Next action"
  echo
  if [[ "$phase" == "ESCALATE" ]]; then
    echo "The same signature has repeated $attempt times. **Stop changing production code.**"
    echo "Patch the harness first:"
    echo
    echo "- Guidance gap: update \`AGENTS.md\` or the relevant leaf rule."
    echo "- Sensor gap: add a regression test or a \`scripts/verify.sh\` check."
    echo "- Execution gap: add a guard/hook or a forbidden-move check."
    echo
    echo "Then rerun \`./scripts/feedback-loop.sh\` with the harness change in place."
  else
    echo "Fix only the first failure, then rerun \`./scripts/feedback-loop.sh\`."
    echo "Do not rerun unchanged code and do not weaken or skip a test."
  fi
} >"$LATEST"

if [[ "$phase" == "ESCALATE" ]]; then
  {
    echo "# Harness Escalation"
    echo
    echo "The same failure signature repeated $attempt times (limit: $MAX_ATTEMPTS)."
    echo "Patch the harness first: do not make another production-code attempt."
    echo
    echo "## Failure signature"
    echo
    echo '```text'
    printf '%s\n' "$failures"
    echo '```'
    echo
    echo "## Diagnose the gap"
    echo
    echo "1. Guidance: was a rule or API contract missing from AGENTS.md?"
    echo "2. Sensor: did a broken behavior pass verify.sh because a test is missing?"
    echo "3. Execution: did an unsafe or forbidden action slip through?"
    echo
    echo "Patch the smallest harness layer that makes the failure impossible to repeat,"
    echo "then rerun the loop. Never weaken an assertion or skip a test."
  } >"$ESCALATION"
fi

printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$now" "$phase" "$commit" "$signature" "$attempt" "$duration" "$cmd_string" >>"$HISTORY"

echo "[feedback] $phase (attempt $attempt/$MAX_ATTEMPTS): $LATEST"
if [[ -n "$ESCALATION" && -f "$ESCALATION" ]]; then
  echo "[feedback] escalation: $ESCALATION"
fi
exit "$status"
