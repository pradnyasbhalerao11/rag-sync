#!/usr/bin/env python3
"""
Export every document version referenced by an event stream into a
content-addressed blob store.

Why this exists
---------------
Events carry a content_hash, not the document text. That is the right shape: an
event is a notification that something changed, not a payload. In a real system
the consumer would resolve that reference against S3, a CMS, or a document
service. Here it resolves against a local directory.

Keeping content out of the event also keeps Kafka messages small. A 4,000-word
document is roughly 25KB; 3,500 of them would be ~90MB of topic traffic, and
individual documents can exceed Kafka's default 1MB message limit.

Layout
------
    data/blobs/4c/2ecab69c8d926671e1414faf1e291d0129c9df

Sharded by the first two hex characters so no single directory holds thousands
of files. Same convention git itself uses for loose objects.

Because the filename IS the content hash, the store is naturally deduplicated:
two documents with identical bytes occupy one file, and re-running this script
rewrites nothing that already exists.

Usage
-----
    python3 tools/export_blobs.py --repo ../vuejs-docs \
        --events data/events.jsonl --out data/blobs
"""

import argparse
import json
import os
import subprocess
import sys
import threading


def collect_hashes(events_path: str) -> list[str]:
    """Every distinct non-null content_hash in the event stream, in file order."""
    seen: dict[str, None] = {}  # dict preserves insertion order, set does not
    with open(events_path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            content_hash = json.loads(line).get("content_hash")
            if content_hash:
                seen.setdefault(content_hash, None)
    return list(seen)


def blob_path(out_dir: str, sha: str) -> str:
    return os.path.join(out_dir, sha[:2], sha[2:])


def export(repo: str, hashes: list[str], out_dir: str, force: bool) -> tuple[int, int, int]:
    """
    Stream every blob out of git in one subprocess using `git cat-file --batch`.

    The naive approach is one `git cat-file` per blob. On this corpus that is
    ~2,500 process spawns and takes minutes. --batch reads SHAs on stdin and
    writes objects to stdout, so it costs a single spawn.

    Protocol per object:
        <sha> <type> <size>\\n
        <size bytes of content>\\n
    """
    todo = hashes if force else [h for h in hashes if not os.path.exists(blob_path(out_dir, h))]
    skipped = len(hashes) - len(todo)

    if not todo:
        return 0, skipped, 0

    proc = subprocess.Popen(
        ["git", "-C", repo, "cat-file", "--batch"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    written = 0
    total_bytes = 0
    missing = 0

    # Feed stdin from a separate thread. Writing every SHA up front deadlocks
    # once the input exceeds the OS pipe buffer (~64KB, so a few hundred blobs):
    # we block writing while git blocks writing its output, and neither side
    # drains. Only reproduces on large corpora, which makes it a nasty one to
    # find later.
    def feed():
        try:
            proc.stdin.write(("\n".join(todo) + "\n").encode())
            proc.stdin.close()
        except BrokenPipeError:
            pass

    feeder = threading.Thread(target=feed, daemon=True)
    feeder.start()

    try:
        for _ in todo:
            header = proc.stdout.readline()
            if not header:
                break

            parts = header.decode().split()
            if len(parts) < 3 or parts[1] != "blob":
                # "<sha> missing" — the blob is not in this clone, which happens
                # with shallow or partial clones
                missing += 1
                continue

            sha, size = parts[0], int(parts[2])
            content = proc.stdout.read(size)
            proc.stdout.read(1)  # trailing newline the protocol adds

            path = blob_path(out_dir, sha)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # write to a temp file then rename, so a crash never leaves a
            # half-written blob sitting under a hash that claims to be complete
            tmp = path + ".tmp"
            with open(tmp, "wb") as fh:
                fh.write(content)
            os.replace(tmp, path)

            written += 1
            total_bytes += size
    finally:
        proc.wait()

    if missing:
        print(f"warning: {missing} blobs not found in the repo "
              f"(shallow or partial clone?)", file=sys.stderr)

    return written, skipped, total_bytes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="path to the local git clone")
    ap.add_argument("--events", default="data/events.jsonl")
    ap.add_argument("--out", default="data/blobs")
    ap.add_argument("--force", action="store_true",
                    help="rewrite blobs that already exist")
    args = ap.parse_args()

    hashes = collect_hashes(args.events)
    if not hashes:
        sys.exit("no content hashes found — did you run extract first?")

    written, skipped, total_bytes = export(args.repo, hashes, args.out, args.force)

    print(f"distinct_blobs={len(hashes)} written={written} already_present={skipped} "
          f"bytes={total_bytes / 1024:.0f}KB")


if __name__ == "__main__":
    main()
