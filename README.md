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
- **Embedding model skew** — new vectors sit in a different space from old ones
- **Poison documents** — one bad file stalls a Kafka partition silently

---

## Status

- [x] Event transport — git history → Kafka, ordered per document
- [x] Chunking and dedupe — parse, normalize, chunk, content-hash
- [x] Embeddings and vector store — pluggable provider, pluggable store
- [ ] Delete propagation and orphan sweep
- [ ] Dead-letter topic and reconciler
- [ ] Embedding model migration by replay

---

## Architecture

```
  git repository
     │  extract_events.py           export_blobs.py
     ▼                                     │
  doc.changes  (Kafka, keyed by doc path)  │
     │                                     ▼
     ▼                               data/blobs/   (content-addressed)
  ┌───────────────────────────────────────────────┐
  │  ingest consumer                              │
  │                                               │
  │   parse       DocumentParsers                 │
  │                 .md → MarkdownParser          │
  │                 *   → passthrough             │
  │      ▼                                        │
  │   normalize   opaque URL → placeholder        │
  │                 original kept as metadata     │
  │      ▼                                        │
  │   chunk       headings → paragraphs → cap     │
  │      ▼                                        │
  │   hash        embedHash   = text              │
  │               contentHash = text + links      │
  │      ▼                                        │
  │   embed       EmbeddingProvider               │
  │                 ollama | azure                │
  │      ▼                                        │
  │   store       VectorStore                     │
  │                 pgvector | ...                │
  └───────────────────────────────────────────────┘
```

Nothing above the store is in scope. This is infrastructure — no user logs into
it.

---

## Pluggable by design

Two seams, each selected by one config line. `@ConditionalOnProperty` means
exactly one bean of each type exists, and the pipeline never learns which.

```yaml
app:
  embedding:
    provider: ollama        # ollama | azure
  store:
    provider: pgvector      # pgvector | ...
```

The same pattern handles document formats. The first chunker knew one rule —
split on markdown headings — and measuring showed the limit: on other corpora,
**20% of files have no headings at all** and became a single chunk. The fix was a
parser seam rather than a cleverer markdown chunker.

---

## Parsing and normalization

Two stages run **before** chunking.

**Parse** — what is the text in this file? For markdown, strip YAML frontmatter;
`title: X / outline: deep` is metadata, not prose.

**Normalize** — what should the embedding model see? Documentation routinely
embeds generated permalinks: one in the test corpus is **2,655 characters** of
base64 on a single line. The chunker had nothing to split it on, so it hard-cut
at the size cap and emitted meaningless fragments. Those embed without error,
cost real calls, and produce vectors that can still be the nearest match to a
user's query.

Detection requires all three conditions:

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
embedHash   = sha256(normalized text)           → need a new vector?
contentHash = sha256(normalized text + links)   → need to rewrite the row?
```

| what changed | embedHash | contentHash | cost |
|---|---|---|---|
| prose | changes | changes | embed + write |
| only a URL | same | changes | write row, **copy the vector** |

Neither hash includes chunk position. If it did, inserting a paragraph at the top
of a document would make every chunk below look changed, collapsing reuse on
exactly the edits where it matters most.

---

## Results

Full replay of a public documentation repository — 3,726 change events across
347 distinct document paths:

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

**`orderingViolations: 0`** — every document's edits, renames and deletes applied
in commit order.

**`reuseRate: 0.776`** — 37,084 of 47,760 chunks were byte-identical to one
already seen, so 78% of embedding work is avoidable.

Normalization alone cut chunks from 52,255 to 47,760 and embedding calls from
11,239 to 10,676, keeping ~470 base64 vectors out of the store entirely.

---

## Running it

Requires Docker, Java 17+, Maven, Python 3.10+, and [Ollama](https://ollama.com).

```bash
conda create -n ragsync python=3.12 -y && conda activate ragsync
pip install -r tools/requirements.txt

ollama pull nomic-embed-text
make ollama-check                    # expects 768 dimensions

make test                            # unit tests, no infrastructure
make up                              # kafka + postgres
make topic
make db-init                         # pgvector extension, schema, indexes

make audit   REPO=../docs-corpus     # is this corpus worth using?
make extract REPO=../docs-corpus     # git history → data/events.jsonl
make export  REPO=../docs-corpus     # document bytes → data/blobs/
make consumer                        # Spring Boot, blocks
make publish                         # another shell
make count && make stats
```

Three pieces of state lie to you independently on a re-run: the Kafka topic
(`make down`), the consumer group offset (bump `group-id`), and the in-memory
counters (restart the consumer).

---

## Choosing a corpus

Any git repository works — the extractor reads commit metadata, not file
contents, so format is irrelevant at that stage.

`make audit REPO=<path>` scores a candidate on licence, document count, churn,
delete rate and rename count. A corpus where nothing is ever deleted can never
surface the orphaned-chunk bug, and a repository with no licence file grants no
redistribution rights — fine to read locally, not fine to publish anything
derived from.

No corpus is included in this repository. `data/` is gitignored: the event
stream, blob store and vector rows are all regenerated by the commands above.

---

## Layout

```
tools/               extract_events, export_blobs, publish_events, audit_corpus
ingest-consumer/
  parse/             DocumentParser, MarkdownParser, DocumentParsers
  normalize/         ContentNormalizer, OpaqueUrlNormalizer, NormalizedContent
  chunk/             Chunker, ChunkPipeline, ChunkHasher, HashedChunk
  embed/             EmbeddingProvider, Ollama…, Azure…, EmbeddingMetrics
  store/             VectorStore, PgVectorStore, IndexedChunk, ChunkId
  consumer/          Kafka listener
  stats/             OrderingTracker, ReplayStats
  web/               GET /stats
  resources/db/      schema.sql
docker-compose.yml   Kafka (KRaft) + UI + Postgres/pgvector
```

MIT
