package com.ragsync.ingest.store;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Where vectors live.
 *
 * An interface because the choice of store should be reversible. pgvector runs
 * locally today; Azure AI Search is a second implementation away. The consumer
 * never learns which is in use.
 *
 * What is NOT reversible is the data. A pgvector column is declared with a
 * fixed width — vector(768) — so changing embedding model changes the schema
 * and requires re-embedding everything. The provider is swappable; the rows
 * are not.
 */
public interface VectorStore {

    void upsert(List<IndexedChunk> chunks);

    /** Which chunk rows does this document already have? Drives dedupe. */
    Set<String> contentHashes(String docId);

    /**
     * An existing vector for this exact normalized text, if one exists.
     *
     * This is what makes the dual-hash scheme pay: when a chunk's contentHash
     * changed but its embedHash did not — a regenerated permalink, say — the
     * row needs rewriting but the vector can be copied rather than recomputed.
     */
    Optional<float[]> vectorFor(String docId, String embedHash);

    /** Week 4 wires this to delete events. */
    int deleteByDocument(String docId);

    long countChunks();
}
