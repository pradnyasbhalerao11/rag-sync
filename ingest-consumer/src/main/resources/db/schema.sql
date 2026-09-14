-- rag-sync vector store
--
-- Run once:  make db-init
--
-- Three columns exist for weeks that have not happened yet. Retrofitting any of
-- them means rebuilding the table and re-embedding the corpus, so they go in
-- from the first write:
--
--   source_version    the orphan sweep deletes rows whose version is not the
--                     one just written, which stops a rewritten document from
--                     leaving stale chunks behind
--   embedding_model   a model migration finds every vector produced by a
--                     retired model with a filter on this column
--   embed_hash        lets a row be rewritten while its vector is copied
--                     rather than recomputed

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id        text PRIMARY KEY,
    doc_id          text        NOT NULL,
    source_version  text        NOT NULL,
    chunk_index     integer     NOT NULL,
    content_hash    text        NOT NULL,
    embed_hash      text        NOT NULL,
    embedding_model text        NOT NULL,
    text            text        NOT NULL,
    links           text        NOT NULL DEFAULT '',
    -- The width is fixed at table creation. Changing embedding model means
    -- changing this number, which means a migration and a full re-embed.
    -- nomic-embed-text is 768; text-embedding-3-small at 512 would need 512.
    embedding       vector(768) NOT NULL,
    indexed_at      timestamptz NOT NULL DEFAULT now()
);

-- The dedupe lookup runs once per upsert event: "which rows does this document
-- already have?" Without this index it is a sequential scan of the whole table,
-- 3,499 times.
CREATE INDEX IF NOT EXISTS chunks_doc_id_idx        ON chunks (doc_id);
CREATE INDEX IF NOT EXISTS chunks_doc_embed_idx     ON chunks (doc_id, embed_hash);
CREATE INDEX IF NOT EXISTS chunks_source_version_idx ON chunks (doc_id, source_version);
CREATE INDEX IF NOT EXISTS chunks_model_idx         ON chunks (embedding_model);

-- Approximate nearest-neighbour index for similarity search.
--
-- Deliberately created AFTER the bulk load in make db-index, not here: building
-- HNSW incrementally during 10,000 inserts is far slower than building it once
-- over a finished table. Week 3 writes; the read path that needs this comes
-- later.
--
--   CREATE INDEX chunks_embedding_idx ON chunks
--     USING hnsw (embedding vector_cosine_ops);
