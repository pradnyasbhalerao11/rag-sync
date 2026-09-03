# rag-sync

Keeps a RAG vector index in sync with a document corpus that keeps changing.

Most RAG systems index their documents once and never think about it again. Then
the documents change — edited, deleted, renamed — and the index quietly stops
matching reality. It still returns results. The results are just wrong, and
nothing tells you.

This is the write path for a RAG system: the part that notices changes and
applies them correctly.

---

## The failure modes this exists to prevent

**Orphaned chunks.** A 4,000-word document splits into 12 chunks. It gets
rewritten and now splits into 9. Upsert the 9 and chunks 10–12 from the old
version stay in the index forever, still retrievable, still cited.

**Deletes that don't propagate.** A document is removed from the source. Its
embeddings remain. If that document held a revoked policy or personal data, this
is a compliance problem rather than a quality one.

**Embedding model skew.** Upgrade the embedding model and old vectors sit in a
different space to new ones. Similarity scores between them become noise. You
need to know which model produced every vector and be able to re-embed without
downtime.

**Poison documents.** One malformed file throws in the consumer, the partition
stalls, and every document behind it stops indexing silently.

All four are ordinary backend correctness problems. Almost none of the AI
engineering content online addresses any of them.

---

## Status: week 1 of 6

This repo is being built in public, one increment per week.

- [x] **Week 1** — change events extracted from real git history, published to
      Kafka, consumed in order, counted
- [ ] **Week 2** — chunking, content-hash dedupe, skip unchanged chunks
- [ ] **Week 3** — embeddings via Microsoft Foundry, upsert into the index
- [ ] **Week 4** — delete propagation, orphan sweep, idempotent replay
- [ ] **Week 5** — dead-letter topic, reconciler, drift report
- [ ] **Week 6** — embedding model migration by topic replay, before/after
      numbers

Week 1 deliberately does no work. Before spending money on embedding calls, the
transport has to be provably correct: if a delete can overtake an upsert, you
delete a document that was about to be correctly indexed, and you would never
find that bug with embedding calls and index writes in the way.

---

## Where the test data comes from

Synthetic change events are too clean. Real documentation repositories are
already a recording of a folder changing over years — genuine edits, deletions
and renames, with timestamps.

`tools/extract_events.py` replays a repository's commit log as a change event
stream. It uses `git log --raw`, which returns the blob SHA of every file
version, so content hashes come free: git already computed them, and identical
content always produces an identical SHA.

Running it against the Vue.js documentation repo:

```
events=3726  upserts=3499  deletes=227
distinct_docs=347  ordering_violations=0
avg_edits_per_doc=10.7
```

347 distinct paths across history against 122 files at HEAD. Roughly two thirds
of the documents this corpus ever contained were later deleted or moved. That
gap is the entire reason this project exists.

`tools/audit_corpus.py` scores any repository on whether it will actually
exercise the pipeline — a corpus where nothing is ever deleted can never
surface the orphaned-chunk bug.

---

## Architecture

```
  Documents            Sync layer (this repo)              Index
 +----------+      +---------------------------+      +------------+
 | git repo |----->|  doc.changes topic        |      |  vectors + |
 |  (source |      |  keyed by document path   |----->|   chunks   |
 | of truth)|      |           |               |      |            |
 +----------+      |           v               |      +------------+
                   |  ingestion consumer       |             |
                   +---------------------------+             v
                                                       +------------+
                                                       |  RAG app   |
                                                       | (not here) |
                                                       +------------+
```

The retrieval side is deliberately out of scope. This is infrastructure —
nothing sits above it that a user logs into.

---

## Running it

Requires Docker, Java 17+, Maven, Python 3.10+.

```bash
conda create -n ragsync python=3.12 -y && conda activate ragsync
pip install -r tools/requirements.txt

git clone https://github.com/vuejs/docs ../vuejs-docs
python3 tools/audit_corpus.py --repo ../vuejs-docs   # should say "good corpus"

make up                              # Kafka in KRaft mode, plus UI on :8081
make topic                           # doc.changes, 6 partitions
make extract REPO=../vuejs-docs      # git history -> data/events.jsonl
make consumer                        # Spring Boot, blocks
make publish                         # in another shell
make stats
```

`make stats` returns:

```json
{
  "totalEvents": 3726,
  "eventsByOp": { "delete": 227, "upsert": 3499 },
  "distinctDocuments": 347,
  "eventsByPartition": { "0": 813, "1": 828, "2": 496, "3": 509, "4": 554, "5": 526 },
  "orderingViolations": 0,
  "deserializationFailures": 0
}
```

`orderingViolations: 0` across 347 documents is the result week 1 exists to
produce.
---

## Design notes

**Events are keyed by document path.** All events for one document therefore
land on one partition, and one partition is consumed by one thread, so a
document's events are always processed in commit order. This single producer
config line is what makes everything downstream possible.

**Ordering is asserted at runtime, not assumed.** `OrderingTracker` holds the
last sequence number seen per document and requires each new one to be strictly
greater. If the partitioning key changes, or a rebalance reprocesses records,
`orderingViolations` goes non-zero immediately rather than surfacing months
later as a wrong answer.

**Manual offset commits.** Auto-commit is off. The offset moves when the work is
done, not when the poll loop comes around again. Right now "done" means counted;
from week 3 it will mean written to the index, and that distinction is the
difference between at-least-once and at-most-once delivery.

**Renames expand into two events.** Git records a rename as one operation.
The extractor emits a delete of the old path and an upsert of the new one.
Handle only the upsert and the index ends up holding two copies of the same
document, one filed under a path that no longer exists.

**Deterministic event ids.** `{commit}:{path}:{op}`. Replaying the same history
produces identical ids, which is what makes week 4's idempotency test possible
without extra machinery.

---

## Layout

```
tools/
  extract_events.py   git history -> JSONL change events
  publish_events.py   JSONL -> Kafka, keyed by document path
  audit_corpus.py     score a repo on whether it's a useful corpus
ingest-consumer/      Spring Boot consumer, ordering verification, stats
docker-compose.yml    single-node Kafka (KRaft) + Kafka UI
Makefile              every command above
```