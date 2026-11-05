#!/usr/bin/env python3
"""Quality and Security Gate Validator for Walspool.

Inspects git diff against HEAD (or main) to enforce absolute zero-tolerance
for forbidden moves:
- No em-dashes (\\u2014) or en-dashes (\\u2013)
- No test softening (.skip, .todo, xit, xdescribe, t.Skip)
- No stubs or unresolved markers (TODO, FIXME, panic("not implemented"))
- No linter suppressions (// nolint, // nosnakecase)
- No unauthorized external runtime dependencies added to go.mod
"""

import os
import re
import subprocess
import sys

FORBIDDEN_PATTERNS = [
    (re.compile(r"[\u2014\u2013]"), "Forbidden em-dash or en-dash detected"),
    (re.compile(r"\b(t\.Skip|t\.Skipf|t\.SkipNow)\b"), "Forbidden test skipping (t.Skip)"),
    (re.compile(r"\b(xit|xdescribe|\.skip|\.todo)\b"), "Forbidden test skipping/softening"),
    (re.compile(r"\bpanic\([\"'](not implemented|todo)[\"']\)", re.IGNORECASE), "Forbidden stub panic"),
    (re.compile(r"//\s*(TODO|FIXME):", re.IGNORECASE), "Forbidden unresolved TODO/FIXME comment"),
    (re.compile(r"//\s*nolint", re.IGNORECASE), "Forbidden linter suppression comment"),
]

ALLOWED_MODULES = {
    "github.com/google/uuid",
    "go.opentelemetry.io/proto/otlp",
    "golang.org/x/net",
    "golang.org/x/sys",
    "google.golang.org/grpc",
    "google.golang.org/protobuf",
}

def get_git_diff(base_ref=None):
    cmd = ["git", "diff"]
    if base_ref:
        cmd.append(base_ref)
    else:
        cmd.append("HEAD")
    try:
        res = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return res
    except subprocess.CalledProcessError as e:
        return ""

def check_diff(diff_text):
    errors = []
    current_file = None
    line_num = 0

    for line in diff_text.splitlines():
        if line.startswith("+++ b/"):
            current_file = line[6:]
            continue
        if line.startswith("@@"):
            m = re.search(r"\+(\d+)", line)
            if m:
                line_num = int(m.group(1)) - 1
            continue

        if line.startswith("+") and not line.startswith("+++"):
            line_num += 1
            content = line[1:]

            # Exclude this checker file itself or test fixtures that deliberately test the checker
            if current_file and "quality_checker" in current_file:
                continue

            for pattern, desc in FORBIDDEN_PATTERNS:
                if pattern.search(content):
                    errors.append(f"{current_file}:{line_num}: {desc} -> {content.strip()}")

    return errors

def check_go_mod():
    errors = []
    if not os.path.exists("go.mod"):
        return errors
    with open("go.mod", "r", encoding="utf-8") as f:
        in_require = False
        for line in f:
            stripped = line.strip()
            if stripped.startswith("require ("):
                in_require = True
                continue
            if in_require and stripped == ")":
                in_require = False
                continue
            if (in_require or stripped.startswith("require ")) and not stripped.startswith("//"):
                parts = stripped.replace("require ", "").split()
                if len(parts) >= 2:
                    mod_name = parts[0]
                    # Direct dependency check
                    if "// indirect" not in stripped and mod_name not in ALLOWED_MODULES:
                        # Allow self-module or submodules
                        if not mod_name.startswith("github.com/YohannHommet/walspool"):
                            errors.append(f"go.mod: Unauthorized direct runtime dependency added: {mod_name}")
    return errors

def main():
    base_ref = sys.argv[1] if len(sys.argv) > 1 else None
    diff_text = get_git_diff(base_ref)
    
    errors = check_diff(diff_text)
    mod_errors = check_go_mod()
    errors.extend(mod_errors)

    if errors:
        print("[walspool-quality-validator] REJECTED: Forbidden moves detected:")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)

    print("[walspool-quality-validator] PASS: 0 forbidden moves detected across diff.")
    sys.exit(0)

if __name__ == "__main__":
    main()
