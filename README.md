# rag-sync

Keeps a RAG vector index in sync with a document corpus that keeps changing.

Most RAG systems index once and stop. Then documents get edited, deleted and
renamed, and the index quietly stops matching reality. It still returns results.
The results are just wrong, and nothing tells you.

This is the write path: the part that notices a change and applies it correctly.

---

## Failure modes it exists to prevent

- **Orphaned chunks** — a 12-chunk document is rewritten into 9; upsert the 9 and
  chunks 10–12 stay retrievable forever
- **Deletes that don't propagate** — source document removed, embeddings remain
- **Embedding model skew** — new vectors land in a different space from old ones
- **Poison documents** — one bad file stalls a Kafka partition silently

---

## Status

- [x] Event transport — git history → Kafka, ordered per document
- [x] Chunking and dedupe — parse, normalize, chunk, content-hash
- [ ] Embeddings and vector index
- [ ] Delete propagation and orphan sweep
- [ ] Dead-letter topic and reconciler
- [ ] Embedding model migration by replay

---

## Architecture

```
  git repo
     │  extract_events.py          export_blobs.py
     ▼                                    │
  doc.changes  (Kafka, keyed by doc path) │
     │                                    ▼
     ▼                              data/blobs/  (content-addressed)
  ┌──────────────────────────────────────────────┐
  │  ingest consumer                             │
  │                                              │
  │   parse       DocumentParsers ──┐            │
  │                                 ├─ .md → MarkdownParser
  │                                 └─ *  → passthrough
  │      ▼                                       │
  │   normalize   OpaqueUrlNormalizer            │
  │                  opaque URL → §link0§        │
  │                  original kept in metadata   │
  │      ▼                                       │
  │   chunk       Chunker                        │
  │                  headings → paragraphs → cap │
  │      ▼                                       │
  │   hash        embedHash   = text             │
  │               contentHash = text + links     │
  └──────────────────────────────────────────────┘
     │
     ▼
  vector index  →  RAG app (out of scope)
```

---

## From markdown-specific to pluggable

The first chunker knew exactly one rule: split on markdown `#` headings. Running
it against other corpora showed the limit — **28 of 138 Prometheus docs and 29 of
150 JUnit Java files have no headings at all**, so each became a single chunk and
fell through to a blind character cut.

The fix was not a smarter markdown chunker. It was a seam:

```
DocumentParsers.extractText(raw, path)
        ├── .md  → MarkdownParser   (strips frontmatter)
        └── else → passthrough
```

Adding PDF or HTML later means adding one `DocumentParser`, not editing the
chunker. The block-model abstraction is deliberately deferred until a second
format actually needs it.

---

## Parsing and normalization

Two stages run **before** chunking.

**Parse** — what is the text in this file? For markdown, strip the YAML
frontmatter; `title: X / outline: deep` is metadata, not prose.

**Normalize** — what should the embedding model see? The corpus contains
generated permalinks up to **2,655 characters**, entirely base64 on one line. The
chunker had nothing to split them on, so it hard-cut at the size cap and emitted
fragments like `XG4gICAgPlxuICAgICAge3sgaXRlbS5tc2c`. Those embed without error,
cost real money, and produce meaningless vectors that can still be the nearest
match to a user's query.

Detection requires all three conditions, deliberately:

1. it is a URL
2. its fragment or query is ≥ 200 characters
3. that payload is ≥ 95% encoding-alphabet characters, no spaces

"Looks like base64" alone would catch JWTs, hashes and encoding examples. A
base64 blob in a paragraph is untouched — it isn't a URL.

**Nothing is deleted.** The original URL travels with the chunk as metadata, so a
retrieved chunk still presents a working link. Only the embedding representation
changes.

---

## Two hashes

```
embedHash   = sha256(normalized text)               → need a new vector?
contentHash = sha256(normalized text + links)       → need to rewrite the row?
```

| what changed | embedHash | contentHash | cost |
|---|---|---|---|
| prose | changes | changes | embed + write |
| only a URL | same | changes | write row, **reuse vector** |

Neither hash includes chunk position. If it did, inserting a paragraph at the top
of a document would make every chunk below look changed, collapsing reuse on
exactly the edits where it matters most.

---

## Results — full replay of vuejs/docs

```json
{
  "totalEvents": 3726,
  "distinctDocuments": 347,
  "orderingViolations": 0,
  "chunksTotal": 47760,
  "chunksNew": 10676,
  "reuseRate": 0.776,
  "vectorsReusedAcrossLinks": 146,
  "blobMisses": 0
}
```

**`orderingViolations: 0`** across 347 documents — every document's edits, renames
and deletes applied in commit order.

**`reuseRate: 0.776`** — 37,084 of 47,760 chunks were byte-identical to one
already seen, so 78% of embedding work is avoidable.

Normalization cut chunks from 52,255 to 47,760 and embedding calls from 11,239 to
10,676, keeping ~470 base64 vectors out of the index entirely.

---

## Running it

```bash
conda activate ragsync
pip install -r tools/requirements.txt
git clone https://github.com/vuejs/docs ../vuejs-docs

make test                          # 30 unit tests, no infrastructure
make up && make topic
make extract REPO=../vuejs-docs    # git history → data/events.jsonl
make export  REPO=../vuejs-docs    # document bytes → data/blobs/
make consumer                      # Spring Boot, blocks
make publish                       # another shell
make stats
```

Re-running consumes nothing new — Kafka remembers the group's offset. For a
clean replay: `make down && make up`, bump `group-id`, restart the consumer.

---

## Layout

```
tools/               extract_events, export_blobs, publish_events, audit_corpus
ingest-consumer/
  parse/             DocumentParser, MarkdownParser, DocumentParsers
  normalize/         ContentNormalizer, OpaqueUrlNormalizer, NormalizedContent
  chunk/             Chunker, ChunkPipeline, ChunkHasher, HashedChunk, SeenChunks
  consumer/          Kafka listener
  stats/             OrderingTracker, ReplayStats
  web/               GET /stats
docker-compose.yml   single-node Kafka (KRaft) + UI
```

MIT