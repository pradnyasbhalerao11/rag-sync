package com.ragsync.ingest.chunk;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which chunks have already been processed, per document.
 *
 * Tracks both hashes separately because they answer different questions:
 *
 *   contentHashes  rows already written to the index
 *   embedHashes    vectors that already exist
 *
 * A chunk whose contentHash is known needs nothing at all. A chunk whose
 * contentHash is new but whose embedHash is known needs a row written but no
 * embedding call — that is the "only the URL changed" case.
 *
 * This is a stand-in for the vector index. In week 3 both questions become
 * queries against Azure AI Search and this class goes away: an in-memory set
 * alongside a real index is a second thing that can disagree, and a restart
 * would re-embed the whole corpus.
 *
 * Scoped per document on purpose. Two documents containing the same
 * boilerplate paragraph each need their own row, because deleting one must not
 * orphan the other's chunk.
 */
@Component
public class SeenChunks {

    private final Map<String, Set<String>> contentByDocument = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> embedByDocument = new ConcurrentHashMap<>();

    /** @return true if this exact row is new for this document */
    public boolean markContentSeen(String docId, String contentHash) {
        return contentByDocument
                .computeIfAbsent(docId, key -> ConcurrentHashMap.newKeySet())
                .add(contentHash);
    }

    /** @return true if no vector exists yet for this text */
    public boolean markEmbedSeen(String docId, String embedHash) {
        return embedByDocument
                .computeIfAbsent(docId, key -> ConcurrentHashMap.newKeySet())
                .add(embedHash);
    }

    /** Called on delete. Week 4 makes this a real index deletion. */
    public int forget(String docId) {
        Set<String> removed = contentByDocument.remove(docId);
        embedByDocument.remove(docId);
        return removed == null ? 0 : removed.size();
    }

    public int trackedDocuments() {
        return contentByDocument.size();
    }

    public void reset() {
        contentByDocument.clear();
        embedByDocument.clear();
    }
}
