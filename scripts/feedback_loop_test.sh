#!/usr/bin/env bash
# Self-test for scripts/feedback-loop.sh. Uses a fake sensor and a temporary
# state directory, so it does not invoke Go or touch the real feedback state.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOOP="$ROOT/scripts/feedback-loop.sh"
mkdir -p "$ROOT/tmp"
TMP="$(mktemp -d "$ROOT/tmp/feedback-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

cat >"$TMP/fail.sh" <<'EOF'
#!/usr/bin/env bash
echo "--- FAIL: TestSensor (0.00s)"
echo "    sensor_test.go:42: expected 1, got 2"
echo "FAIL github.com/YohannHommet/walspool"
exit 1
EOF
chmod +x "$TMP/fail.sh"

cat >"$TMP/pass.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$TMP/pass.sh"

export WALSPOOL_FEEDBACK_DIR="$TMP/state"
export WALSPOOL_VERIFY_CMD="$TMP/fail.sh"

set +e
"$LOOP" --max-attempts 2 >"$TMP/first.out" 2>&1
first=$?
set -e
if [[ "$first" -ne 1 ]]; then
  echo "FAIL: first failure should exit 1, got $first" >&2
  cat "$TMP/first.out" >&2
  exit 1
fi
grep -q 'Status: FAIL' "$TMP/state/latest.md" || { echo "FAIL: expected FAIL status" >&2; exit 1; }
grep -q 'Attempt: 1/2' "$TMP/state/latest.md" || { echo "FAIL: expected attempt 1/2" >&2; exit 1; }
grep -q 'sensor_test.go:42' "$TMP/state/latest.md" || { echo "FAIL: expected compact failure line" >&2; exit 1; }
if [[ -f "$TMP/state/escalation.md" ]]; then
  echo "FAIL: escalation must not exist on attempt 1" >&2
  exit 1
fi

set +e
"$LOOP" --max-attempts 2 >"$TMP/second.out" 2>&1
second=$?
set -e
if [[ "$second" -ne 1 ]]; then
  echo "FAIL: second failure should exit 1, got $second" >&2
  cat "$TMP/second.out" >&2
  exit 1
fi
grep -q 'Status: ESCALATE' "$TMP/state/latest.md" || { echo "FAIL: expected ESCALATE status" >&2; exit 1; }
grep -q 'Attempt: 2/2' "$TMP/state/latest.md" || { echo "FAIL: expected attempt 2/2" >&2; exit 1; }
[[ -f "$TMP/state/escalation.md" ]] || { echo "FAIL: expected escalation artifact" >&2; exit 1; }
grep -q 'Patch the harness first' "$TMP/state/escalation.md" || { echo "FAIL: escalation guidance missing" >&2; exit 1; }

export WALSPOOL_VERIFY_CMD="$TMP/pass.sh"
"$LOOP" --max-attempts 2 >"$TMP/pass.out" 2>&1
grep -q 'Status: PASS' "$TMP/state/latest.md" || { echo "FAIL: expected PASS status" >&2; exit 1; }
if [[ -f "$TMP/state/escalation.md" ]]; then
  echo "FAIL: pass must clear the escalation artifact" >&2
  exit 1
fi

lines="$(wc -l <"$TMP/state/history.tsv" | tr -d ' ')"
if [[ "$lines" -ne 3 ]]; then
  echo "FAIL: expected 3 history rows (fail, escalate, pass), got $lines" >&2
  cat "$TMP/state/history.tsv" >&2
  exit 1
fi

echo "PASS: feedback-loop self-test (2 failures -> escalation, pass clears it)"
