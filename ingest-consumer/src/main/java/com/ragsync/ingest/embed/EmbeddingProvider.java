package com.ragsync.ingest.embed;

import java.util.List;

/**
 * Turns text into vectors.
 *
 * Batched rather than single-text on purpose. One call per chunk means ~10,700
 * round trips; at even 50ms each that is nine minutes of pure waiting. Batching
 * also changes the shape of every caller, so it has to exist in the first
 * version rather than be retrofitted.
 *
 * modelId() and dimensions() are not incidental. Every index row records which
 * model produced its vector, because vectors from different models live in
 * different spaces and comparing them is meaningless. Without that field, a
 * model migration has no way to find the rows it needs to replace.
 */
public interface EmbeddingProvider {

    /** @return one vector per input, in the same order */
    List<float[]> embed(List<String> texts);

    /** Stored on every row. Week 6's migration filters on this. */
    String modelId();

    int dimensions();
}
