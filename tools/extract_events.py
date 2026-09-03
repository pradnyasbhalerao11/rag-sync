#!/usr/bin/env python3
"""
Turn a git repository's commit history into a stream of document change events.

Uses `git log --raw`, which gives us the pre- and post-image blob SHAs for free.
The post-image blob SHA IS the content hash — git already computed it, so we
never have to read file contents or hash anything ourselves.

Output: one JSON object per line (JSONL) on stdout or to --out.

Example:
    python3 extract_events.py --repo ../some-docs-repo --pathspec '*.md' \
        --out ../data/events.jsonl
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone

NULL_SHA = "0" * 40


def run_git_log(repo: str, pathspec: str, since: str | None) -> str:
    cmd = [
        "git", "-C", repo, "log",
        "--reverse",          # oldest first: replay history forwards
        "--no-merges",        # merge commits have no useful --raw output
        "-M",                 # detect renames explicitly
        "--raw",
        "--abbrev=40",        # full blob SHAs, not shortened
        "--pretty=format:commit\t%H\t%ct",
    ]
    if since:
        cmd += ["--since", since]
    cmd += ["--", pathspec]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"git log failed: {result.stderr.strip()}")
    return result.stdout


def parse(log_output: str):
    """Yield (commit_sha, unix_ts, status, paths, src_blob, dst_blob) tuples."""
    commit_sha = None
    unix_ts = None

    for line in log_output.splitlines():
        if not line.strip():
            continue

        if line.startswith("commit\t"):
            _, commit_sha, ts = line.split("\t", 2)
            unix_ts = int(ts)
            continue

        if not line.startswith(":"):
            continue

        # :100644 100644 <src_blob> <dst_blob> <status>\t<path>[\t<newpath>]
        meta, *paths = line.split("\t")
        fields = meta.split()
        if len(fields) < 5:
            continue
        src_blob, dst_blob, status = fields[2], fields[3], fields[4]
        yield commit_sha, unix_ts, status, paths, src_blob, dst_blob


def to_events(parsed):
    """Expand raw git changes into upsert/delete events. Renames become two."""
    for commit_sha, unix_ts, status, paths, src_blob, dst_blob in parsed:
        code = status[0]  # R100 -> R, C75 -> C

        if code in ("A", "M", "T"):
            yield (paths[0], "upsert", commit_sha, unix_ts, dst_blob)
        elif code == "D":
            yield (paths[0], "delete", commit_sha, unix_ts, None)
        elif code in ("R", "C"):
            old_path, new_path = paths[0], paths[1]
            if code == "R":
                # a rename is a delete of the old path plus an upsert of the new
                yield (old_path, "delete", commit_sha, unix_ts, None)
            yield (new_path, "upsert", commit_sha, unix_ts, dst_blob)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="path to a local git clone")
    ap.add_argument("--pathspec", default="*.md", help="which files count as documents")
    ap.add_argument("--since", default=None, help="e.g. '2 years ago'")
    ap.add_argument("--out", default="-", help="output file, or - for stdout")
    args = ap.parse_args()

    seq_by_doc: dict[str, int] = {}
    counts = {"upsert": 0, "delete": 0}
    docs = set()

    out = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")
    try:
        for doc_id, op, commit_sha, unix_ts, blob in to_events(
            parse(run_git_log(args.repo, args.pathspec, args.since))
        ):
            seq = seq_by_doc.get(doc_id, 0) + 1
            seq_by_doc[doc_id] = seq
            counts[op] += 1
            docs.add(doc_id)

            event = {
                # deterministic: replaying the same history gives the same ids,
                # which is what lets you test idempotency later
                "event_id": f"{commit_sha[:12]}:{doc_id}:{op}",
                "doc_id": doc_id,
                "op": op,
                "seq": seq,
                "source_version": commit_sha,
                "content_hash": blob if blob and blob != NULL_SHA else None,
                "committed_at": datetime.fromtimestamp(
                    unix_ts, tz=timezone.utc
                ).isoformat().replace("+00:00", "Z"),
            }
            out.write(json.dumps(event) + "\n")
    finally:
        if out is not sys.stdout:
            out.close()

    print(
        f"events={counts['upsert'] + counts['delete']} "
        f"upserts={counts['upsert']} deletes={counts['delete']} "
        f"distinct_docs={len(docs)}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
