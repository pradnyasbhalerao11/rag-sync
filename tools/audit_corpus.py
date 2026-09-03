#!/usr/bin/env python3
"""
Score a git repository on whether it makes a good corpus for this pipeline.

Any repo will run. Not every repo will exercise the parts that matter. A repo
where nothing is ever deleted or renamed will never surface the orphaned-chunk
bug, which is the whole point of the project.

Usage:
    python3 tools/audit_corpus.py --repo ../some-repo --pathspec '*.md'
"""

import argparse
import collections
import json
import subprocess
import sys
import io

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from extract_events import run_git_log, parse, to_events  # noqa: E402

NULL_SHA = "0" * 40


def head_stats(repo: str, pathspec: str):
    files = subprocess.run(
        ["git", "-C", repo, "ls-files", pathspec],
        capture_output=True, text=True,
    ).stdout.split()
    total_bytes = 0
    for f in files:
        out = subprocess.run(
            ["git", "-C", repo, "cat-file", "-s", f"HEAD:{f}"],
            capture_output=True, text=True,
        )
        if out.returncode == 0:
            total_bytes += int(out.stdout.strip())
    return len(files), total_bytes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--pathspec", default="*.md")
    args = ap.parse_args()

    events = list(to_events(parse(run_git_log(args.repo, args.pathspec, None))))

    ops = collections.Counter(op for _, op, _, _, _ in events)
    per_doc = collections.Counter(doc for doc, _, _, _, _ in events)
    hashes = collections.Counter(b for _, _, _, _, b in events if b and b != NULL_SHA)

    # a rename shows up as a delete and an upsert in the same commit
    by_commit = collections.defaultdict(lambda: {"delete": set(), "upsert": set()})
    for doc, op, sha, _, _ in events:
        by_commit[sha][op].add(doc)
    renames = sum(
        min(len(c["delete"]), len(c["upsert"]))
        for c in by_commit.values()
        if c["delete"] and c["upsert"]
    )

    deleted_docs = {doc for doc, op, _, _, _ in events if op == "delete"}
    head_files, head_bytes = head_stats(args.repo, args.pathspec)
    total_events = len(events)
    distinct = len(per_doc)

    if not distinct:
        sys.exit(f"no files matched pathspec {args.pathspec!r} in this repo")

    churn = total_events / distinct
    delete_rate = len(deleted_docs) / distinct
    dupe_rate = sum(c - 1 for c in hashes.values() if c > 1) / max(1, ops["upsert"])

    print(f"repo:            {args.repo}")
    print(f"pathspec:        {args.pathspec}")
    print()
    print(f"docs at HEAD:    {head_files}")
    print(f"corpus size:     {head_bytes / 1024:.0f} KB")
    print(f"paths in history:{distinct}")
    print(f"events:          {total_events}  (upsert {ops['upsert']}, delete {ops['delete']})")
    print(f"renames:         ~{renames}")
    print()
    print(f"churn:           {churn:.1f} events per document")
    print(f"delete rate:     {delete_rate:.0%} of documents were deleted at some point")
    print(f"duplicate blobs: {dupe_rate:.0%} of upserts reuse an existing content hash")
    print()

    checks = [
        ("enough documents", 80 <= head_files <= 3000, f"{head_files} at HEAD"),
        ("real churn", churn >= 3.0, f"{churn:.1f} events/doc"),
        ("deletes present", ops["delete"] >= 20, f"{ops['delete']} deletes"),
        ("renames present", renames >= 5, f"~{renames} renames"),
        ("fits free-tier index", head_bytes < 40 * 1024 * 1024, f"{head_bytes/1024:.0f} KB"),
    ]
    for name, ok, detail in checks:
        print(f"  [{'x' if ok else ' '}] {name:22} {detail}")

    passed = sum(1 for _, ok, _ in checks if ok)
    print()
    if passed == 5:
        print("VERDICT: good corpus. Use it.")
    elif passed >= 3:
        print("VERDICT: usable, but weak on the unchecked dimensions above.")
    else:
        print("VERDICT: poor fit. Pick a docs-focused repo with a longer history.")


if __name__ == "__main__":
    main()
