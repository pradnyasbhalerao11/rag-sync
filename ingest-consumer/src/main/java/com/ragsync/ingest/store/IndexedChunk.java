package com.ragsync.ingest.store;

import java.util.Map;

/**
 * One row in the vector store.
 *
 * sourceVersion and embeddingModel are carried on every row for later weeks,
 * not for anything week 3 does with them:
 *
 *   sourceVersion   the orphan sweep deletes chunks whose version is not the
 *                   one just written, which is what stops a rewritten document
 *                   leaving stale chunks behind
 *   embeddingModel  a model migration finds every vector produced by the old
 *                   model with a filter on this column
 *
 * Retrofitting either would mean rebuilding the whole store and re-embedding,
 * so they go in from the first write.
 */
public record IndexedChunk(
        String chunkId,
        String docId,
        String sourceVersion,
        int chunkIndex,
        String contentHash,
        String embedHash,
        String embeddingModel,
        String text,
        Map<String, String> links,
        float[] vector) {
}
