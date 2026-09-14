package com.ragsync.ingest.store;

import com.ragsync.ingest.chunk.ChunkHasher;

/**
 * Builds a row key.
 *
 * Scoped per document on purpose: two documents containing the same boilerplate
 * paragraph each need their own row, because deleting one must not remove the
 * other's chunk.
 *
 * The document path is hashed rather than used directly. Postgres would accept
 * a path fine, but Azure AI Search keys allow only letters, digits, dash,
 * underscore and equals — "src/guide/essentials/computed.md" is rejected
 * outright. Hashing keeps the key valid in either store, so the choice stays
 * reversible.
 */
public final class ChunkId {

    private ChunkId() {
    }

    public static String of(String docId, String contentHash) {
        return ChunkHasher.sha256(docId).substring(0, 16) + "_" + contentHash;
    }
}
