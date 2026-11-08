#!/usr/bin/env bash
# Validate that a delegated task's checkpoint artifact exists, is non-empty,
# is recent enough, and contains the required headings.
set -uo pipefail

MAX_AGE=300
REQUIRED=()
TARGET=""

usage() {
  cat <<'EOF'
Usage: scripts/artifact-check.sh <path> [--max-age SECONDS] [--require HEADING]...

Checks:
  - path exists and is a regular file
  - file is non-empty
  - file modification age is <= max-age (default: 300s)
  - every required heading appears literally

Exit codes: 0 pass, 1 validation failure, 2 usage error.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-age)
      if [[ $# -lt 2 || ! "$2" =~ ^[0-9]+$ ]]; then
        echo "artifact-check: --max-age requires a non-negative integer" >&2
        exit 2
      fi
      MAX_AGE="$2"
      shift 2
      ;;
    --require)
      if [[ $# -lt 2 ]]; then
        echo "artifact-check: --require needs a heading" >&2
        exit 2
      fi
      REQUIRED+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      if [[ $# -gt 0 && -z "$TARGET" ]]; then
        TARGET="$1"
        shift
      fi
      ;;
    -*)
      echo "artifact-check: unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ -n "$TARGET" ]]; then
        echo "artifact-check: only one artifact path is allowed" >&2
        exit 2
      fi
      TARGET="$1"
      shift
      ;;
  esac
done

if [[ -z "$TARGET" ]]; then
  usage >&2
  exit 2
fi

if [[ ! -e "$TARGET" ]]; then
  echo "FAIL: artifact not found: $TARGET"
  exit 1
fi
if [[ ! -f "$TARGET" ]]; then
  echo "FAIL: artifact is not a regular file: $TARGET"
  exit 1
fi
if [[ ! -s "$TARGET" ]]; then
  echo "FAIL: artifact is empty: $TARGET"
  exit 1
fi

mtime=""
if mtime="$(stat -c %Y "$TARGET" 2>/dev/null)"; then
  :
elif mtime="$(stat -f %m "$TARGET" 2>/dev/null)"; then
  :
fi
if [[ ! "$mtime" =~ ^[0-9]+$ ]]; then
  echo "FAIL: cannot read artifact modification time: $TARGET"
  exit 1
fi

now="$(date +%s)"
age=$((now - mtime))
if [[ "$age" -lt 0 ]]; then
  age=0
fi
if [[ "$age" -gt "$MAX_AGE" ]]; then
  echo "FAIL: artifact is stale: age=${age}s > max=${MAX_AGE}s: $TARGET"
  exit 1
fi

for heading in "${REQUIRED[@]}"; do
  if ! grep -Fq -- "$heading" "$TARGET"; then
    echo "FAIL: artifact is missing required heading '$heading': $TARGET"
    exit 1
  fi
done

size="$(wc -c <"$TARGET" | tr -d ' ')"
echo "PASS: artifact $TARGET size=${size}B age=${age}s"
exit 0
