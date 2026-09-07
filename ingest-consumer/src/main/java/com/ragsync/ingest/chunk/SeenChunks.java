package com.ragsync.ingest.chunk;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which chunk hashes have already been processed, per document.
 *
 * This is a stand-in for the vector index. In week 3 the question "have I
 * already embedded this chunk?" becomes a query against Azure AI Search, and
 * this class goes away. Keeping it in memory for now means week 2 can measure
 * the reuse rate without spending a cent on embeddings, and without the index
 * being in the way while the chunking logic is still settling.
 *
 * Scoped per document rather than globally on purpose. Two different documents
 * containing the same boilerplate paragraph legitimately need their own copy in
 * the index, because deleting one document must not orphan the other's chunk.
 */
@Component
public class SeenChunks {

    private final Map<String, Set<String>> byDocument = new ConcurrentHashMap<>();

    /**
     * @return true if this chunk hash is new for this document (and records it),
     *         false if it was already present
     */
    public boolean markSeen(String docId, String chunkHash) {
        return byDocument
                .computeIfAbsent(docId, k -> ConcurrentHashMap.newKeySet())
                .add(chunkHash);
    }

    /** Called on delete. Week 4 makes this a real index deletion. */
    public int forget(String docId) {
        Set<String> removed = byDocument.remove(docId);
        return removed == null ? 0 : removed.size();
    }

    public int trackedDocuments() {
        return byDocument.size();
    }

    public void reset() {
        byDocument.clear();
    }
}
