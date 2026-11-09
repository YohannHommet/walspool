#!/usr/bin/env python3
"""Walspool Squad Orchestrator CLI.

Orchestrates the multi-agent crew across GitHub Epic #27:
- Queries Epic #27 to identify the roadmap sequence
- Manages feature branches and artifact directories
- Runs verification and quality gates
- Updates task checkboxes in Epic #27 and closes completed issues
- Manages clean Git merges into main
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

EPIC_ISSUE_NUMBER = 27
REPO_NAME = "YohannHommet/walspool"
SQUAD_DIR = ".walspool/squad"
REPO_ROOT = Path(__file__).resolve().parents[4]
HARNESS_DIR = os.environ.get(
    "WALSPOOL_HARNESS_DIR",
    str(REPO_ROOT.parent / ".harness" / "walspool"),
)

def run_cmd(cmd, check=True, text=True):
    """Executes a command and returns stdout."""
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=text)
    if check and res.returncode != 0:
        err_msg = res.stderr.strip() or res.stdout.strip()
        raise RuntimeError(f"Command failed ({res.returncode}): {' '.join(cmd)}\n{err_msg}")
    return res.stdout.strip()

def get_epic_body():
    """Fetches the full markdown body of Epic #27."""
    cmd = ["gh", "issue", "view", str(EPIC_ISSUE_NUMBER), "--repo", REPO_NAME, "--json", "body", "-q", ".body"]
    return run_cmd(cmd)

def parse_epic_tasks(body):
    """Parses tasks from Epic #27. Returns list of dicts."""
    tasks = []
    # Match lines like: - [ ] #10 `fix(...)`
    # or: - [x] #10 `fix(...)`
    pattern = re.compile(r"^\s*-\s*\[([ xX])\]\s*#(\d+)\s*(.*)$")
    for line in body.splitlines():
        m = pattern.match(line)
        if m:
            completed = (m.group(1).lower() == "x")
            issue_num = int(m.group(2))
            title = m.group(3).strip()
            tasks.append({
                "number": issue_num,
                "title": title,
                "completed": completed,
                "raw_line": line
            })
    return tasks

def cmd_status(args):
    """Displays progress status of Epic #27."""
    body = get_epic_body()
    tasks = parse_epic_tasks(body)
    total = len(tasks)
    completed = sum(1 for t in tasks if t["completed"])
    remaining = total - completed
    pct = (completed / total * 100) if total > 0 else 0

    print(f"=== Walspool Epic #{EPIC_ISSUE_NUMBER} Status ===")
    print(f"Total Issues: {total} | Completed: {completed} ({pct:.1f}%) | Remaining: {remaining}\n")

    next_task = None
    for t in tasks:
        mark = "[x]" if t["completed"] else "[ ]"
        print(f"  {mark} #{t['number']:2d} : {t['title']}")
        if not t["completed"] and next_task is None:
            next_task = t

    print("\n------------------------------------------------------------")
    if next_task:
        print(f"Next Issue in Line: #{next_task['number']} - {next_task['title']}")
    else:
        print("All Epic issues are completed! Ready for release tagging.")

def cmd_next(args):
    """Returns detailed briefing for the next uncompleted issue."""
    body = get_epic_body()
    tasks = parse_epic_tasks(body)
    next_task = None
    for t in tasks:
        if not t["completed"]:
            next_task = t
            break

    if not next_task:
        print("No pending issues found in Epic #27.")
        sys.exit(0)

    # Fetch issue details
    cmd = ["gh", "issue", "view", str(next_task["number"]), "--repo", REPO_NAME, "--json", "number,title,body,labels"]
    raw = run_cmd(cmd)
    data = json.loads(raw)
    labels = ", ".join([l["name"] for l in data.get("labels", [])])

    print(f"=== NEXT ISSUE TO IMPLEMENT ===")
    print(f"Issue Number : #{data['number']}")
    print(f"Title        : {data['title']}")
    print(f"Labels       : {labels}")
    print(f"\n--- Description ---\n")
    print(data["body"].strip())

def cmd_start(args):
    """Prepares feature branch and session directory for an issue."""
    issue_num = args.issue
    cmd = ["gh", "issue", "view", str(issue_num), "--repo", REPO_NAME, "--json", "number,title"]
    raw = run_cmd(cmd)
    data = json.loads(raw)

    title = data.get("title", f"issue-{issue_num}")
    # Sanitize title for git branch
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", title.lower()).strip("-")
    slug = slug[:40] # Keep branch name readable
    branch_name = f"feat/issue-{issue_num}-{slug}"

    # Ensure main is up to date
    current_branch = run_cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    print(f"[Orchestrator] Current branch: {current_branch}")

    # Check if branch already exists
    branches = run_cmd(["git", "branch", "--list", branch_name])
    if branches:
        print(f"[Orchestrator] Switching to existing branch: {branch_name}")
        run_cmd(["git", "checkout", branch_name])
    else:
        print(f"[Orchestrator] Creating and switching to branch: {branch_name}")
        run_cmd(["git", "checkout", "-b", branch_name])

    # Create session workspace and global harness directory
    session_dir = os.path.join(SQUAD_DIR, f"issue-{issue_num}")
    os.makedirs(session_dir, exist_ok=True)
    harness_feature_dir = os.path.join(HARNESS_DIR, f"issue-{issue_num}-{slug}")
    os.makedirs(harness_feature_dir, exist_ok=True)
    print(f"[Orchestrator] Session artifact directory ready: {session_dir}")
    print(f"[Orchestrator] Harness global artifact directory ready: {harness_feature_dir}")

def cmd_verify(args):
    """Executes the verification sensor."""
    env = os.environ.copy()
    env["CGO_ENABLED"] = "0"

    print("[Orchestrator] Running ./scripts/verify.sh sensor...")
    res = subprocess.run(["./scripts/verify.sh"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print(res.stdout.strip())
    if res.returncode != 0:
        if res.stderr.strip():
            print(res.stderr.strip())
        print(f"[Orchestrator] Sensor FAILED with code {res.returncode}")
        sys.exit(res.returncode)
    print("[Orchestrator] Sensor PASSED cleanly.")

def cmd_quality(args):
    """Executes the quality gate checker."""
    checker = os.path.join(os.path.dirname(__file__), "quality_checker.py")
    res = subprocess.run(["python3", checker, args.base_ref or "HEAD"], text=True)
    sys.exit(res.returncode)

def cmd_complete(args):
    """Finalizes an issue: closes GitHub issue, checks Epic, merges to main, and pushes."""
    issue_num = args.issue
    current_branch = run_cmd(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    if current_branch == "main":
        raise RuntimeError("Cannot complete from main branch directly. Must be on feature branch.")

    # 1. Verify sensors pass
    print(f"[Orchestrator] Verifying sensor pass on branch {current_branch}...")
    cmd_verify(args)

    # 2. Verify quality gate passes
    print("[Orchestrator] Verifying quality gate pass...")
    checker = os.path.join(os.path.dirname(__file__), "quality_checker.py")
    res = subprocess.run(["python3", checker, "main"], text=True)
    if res.returncode != 0:
        raise RuntimeError("Quality gate check failed against main. Fix violations before completing.")

    # 3. Close the GitHub issue
    print(f"[Orchestrator] Closing GitHub Issue #{issue_num}...")
    run_cmd(["gh", "issue", "close", str(issue_num), "--repo", REPO_NAME, "--comment", f"Resolved via multi-agent squad on branch `{current_branch}`."])

    # 4. Check off the issue in Epic #27
    print(f"[Orchestrator] Updating task checkbox in Epic #{EPIC_ISSUE_NUMBER}...")
    body = get_epic_body()
    pattern = re.compile(rf"(\s*-\s*\[)[ ](\]\s*#{issue_num}\b)")
    new_body, count = pattern.subn(r"\1x\2", body)
    if count > 0:
        tmp_path = f"/tmp/epic_{EPIC_ISSUE_NUMBER}_update.md"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(new_body)
        run_cmd(["gh", "issue", "edit", str(EPIC_ISSUE_NUMBER), "--repo", REPO_NAME, "--body-file", tmp_path])
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        print(f"[Orchestrator] Epic #{EPIC_ISSUE_NUMBER} checkbox successfully marked [x] for #{issue_num}.")
    else:
        print(f"[Orchestrator] Warning: checkbox for #{issue_num} was not found or already checked.")

    # 4b. Sync artifacts to global harness directory
    git_dir = run_cmd(["git", "rev-parse", "--git-dir"])
    is_worktree = "worktrees" in git_dir
    common_git_dir = run_cmd(["git", "rev-parse", "--git-common-dir"])
    main_repo_dir = os.path.dirname(os.path.abspath(common_git_dir))
    worktree_root = run_cmd(["git", "rev-parse", "--show-toplevel"])

    session_dir = os.path.join(main_repo_dir, SQUAD_DIR, f"issue-{issue_num}")
    if os.path.exists(session_dir) and os.path.exists(HARNESS_DIR):
        for item in os.listdir(HARNESS_DIR):
            if item.startswith(f"issue-{issue_num}"):
                dest = os.path.join(HARNESS_DIR, item)
                for f in os.listdir(session_dir):
                    src_f = os.path.join(session_dir, f)
                    if os.path.isfile(src_f):
                        import shutil
                        shutil.copy2(src_f, os.path.join(dest, f))
                print(f"[Orchestrator] Synced artifacts to {dest}")

    # 5. Merge branch into main and push
    if is_worktree:
        print(f"[Orchestrator] Merging {current_branch} into main (via main repo {main_repo_dir})...")
        run_cmd(["git", "-C", main_repo_dir, "merge", "--no-ff", current_branch, "-m", f"merge: issue #{issue_num} from {current_branch}"])
        print("[Orchestrator] Pushing main to origin...")
        run_cmd(["git", "-C", main_repo_dir, "push", "origin", "main"])

        # 6. Delete local feature branch
        print(f"[Orchestrator] Cleaning up worktree and deleting feature branch {current_branch}...")
        run_cmd(["git", "-C", main_repo_dir, "worktree", "remove", "--force", worktree_root])
        run_cmd(["git", "-C", main_repo_dir, "branch", "-d", current_branch])
    else:
        print(f"[Orchestrator] Merging {current_branch} into main...")
        run_cmd(["git", "checkout", "main"])
        run_cmd(["git", "merge", "--no-ff", current_branch, "-m", f"merge: issue #{issue_num} from {current_branch}"])
        
        print("[Orchestrator] Pushing main to origin...")
        run_cmd(["git", "push", "origin", "main"])

        # 6. Delete local feature branch
        print(f"[Orchestrator] Deleting feature branch {current_branch}...")
        run_cmd(["git", "branch", "-d", current_branch])

    print(f"\n[Orchestrator] SUCCESS: Issue #{issue_num} fully closed, checked in Epic #{EPIC_ISSUE_NUMBER}, and merged to main!")

def main():
    parser = argparse.ArgumentParser(description="Walspool Multi-Agent Squad Orchestrator")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # status
    p_status = subparsers.add_parser("status", help="Show Epic #27 progress and next issue")
    p_status.set_defaults(func=cmd_status)

    # next
    p_next = subparsers.add_parser("next", help="Get full briefing for next uncompleted issue")
    p_next.set_defaults(func=cmd_next)

    # start
    p_start = subparsers.add_parser("start", help="Start feature branch and workspace for an issue")
    p_start.add_argument("issue", type=int, help="Issue number to start")
    p_start.set_defaults(func=cmd_start)

    # verify
    p_verify = subparsers.add_parser("verify", help="Run verification sensor (verify.sh)")
    p_verify.set_defaults(func=cmd_verify)

    # quality
    p_quality = subparsers.add_parser("quality", help="Run quality gate checker")
    p_quality.add_argument("--base-ref", default=None, help="Base ref to diff against (default HEAD)")
    p_quality.set_defaults(func=cmd_quality)

    # complete
    p_complete = subparsers.add_parser("complete", help="Complete issue, check Epic #27, merge and push")
    p_complete.add_argument("issue", type=int, help="Issue number to complete")
    p_complete.set_defaults(func=cmd_complete)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
