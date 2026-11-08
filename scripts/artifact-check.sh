#!/usr/bin/env bash
# Validate that a delegated task's checkpoint artifact exists, is non-empty,
# is recent enough, and contains the required headings.
set -uo pipefail

MAX_AGE=300
MAX_BYTES=10485760 # 10 MiB ceiling against DoS
REQUIRED=()
TARGET=""

usage() {
  cat <<'EOF'
Usage: scripts/artifact-check.sh <path> [--max-age SECONDS] [--max-bytes BYTES] [--require HEADING]...

Checks:
  - path exists, is not a symlink, and is a regular readable file
  - file is non-empty and does not exceed max-bytes (default: 10MiB)
  - file modification age is >= 0 and <= max-age (default: 300s)
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
    --max-bytes)
      if [[ $# -lt 2 || ! "$2" =~ ^[0-9]+$ ]]; then
        echo "artifact-check: --max-bytes requires a non-negative integer" >&2
        exit 2
      fi
      MAX_BYTES="$2"
      shift 2
      ;;
    --require)
      if [[ $# -lt 2 || -z "${2// /}" ]]; then
        echo "artifact-check: --require needs a non-empty heading" >&2
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
      while [[ $# -gt 0 ]]; do
        if [[ -n "$TARGET" ]]; then
          echo "artifact-check: only one artifact path is allowed" >&2
          exit 2
        fi
        TARGET="$1"
        shift
      done
      ;;
    -)
      if [[ -n "$TARGET" ]]; then
        echo "artifact-check: only one artifact path is allowed" >&2
        exit 2
      fi
      TARGET="-"
      shift
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

if [[ "$TARGET" == "-" ]]; then
  echo "FAIL: artifact path cannot be '-' (stdin): $TARGET"
  exit 1
fi

if [[ ! -e "$TARGET" ]]; then
  echo "FAIL: artifact not found: $TARGET"
  exit 1
fi

if [[ -L "$TARGET" ]]; then
  echo "FAIL: artifact must not be a symbolic link: $TARGET"
  exit 1
fi

if [[ ! -f "$TARGET" ]]; then
  echo "FAIL: artifact is not a regular file: $TARGET"
  exit 1
fi

if [[ ! -r "$TARGET" ]]; then
  echo "FAIL: artifact is not readable: $TARGET"
  exit 1
fi

if [[ ! -s "$TARGET" ]]; then
  echo "FAIL: artifact is empty: $TARGET"
  exit 1
fi

mtime=""
size=""
# O(1) inode inspection without streaming file bytes
if stat_out="$(stat -c '%Y %s' -- "$TARGET" 2>/dev/null)"; then
  mtime="${stat_out%% *}"
  size="${stat_out##* }"
elif stat_out="$(stat -f '%m %z' -- "$TARGET" 2>/dev/null)"; then
  mtime="${stat_out%% *}"
  size="${stat_out##* }"
fi

if [[ ! "$mtime" =~ ^[0-9]+$ || ! "$size" =~ ^[0-9]+$ ]]; then
  echo "FAIL: cannot read artifact metadata: $TARGET"
  exit 1
fi

if [[ "$size" -gt "$MAX_BYTES" ]]; then
  echo "FAIL: artifact exceeds size limit: size=${size}B > max=${MAX_BYTES}B: $TARGET"
  exit 1
fi

now="$(date +%s)"
# Anti-gaming: detect timestamps placed in the future (>30s drift tolerance)
if [[ "$mtime" -gt $((now + 30)) ]]; then
  echo "FAIL: artifact modification time is in the future: $TARGET"
  exit 1
fi

age=$((now - mtime))
if [[ "$age" -lt 0 ]]; then
  age=0
fi

if [[ "$age" -gt "$MAX_AGE" ]]; then
  echo "FAIL: artifact is stale: age=${age}s > max=${MAX_AGE}s: $TARGET"
  exit 1
fi

for heading in "${REQUIRED[@]}"; do
  if ! grep -Fq -- "$heading" < "$TARGET"; then
    echo "FAIL: artifact is missing required heading '$heading': $TARGET"
    exit 1
  fi
done

echo "PASS: artifact $TARGET size=${size}B age=${age}s"
exit 0
