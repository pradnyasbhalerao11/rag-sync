package com.ragsync.ingest.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkIdTest {

    /**
     * Postgres would accept a raw path, but Azure AI Search keys allow only
     * letters, digits, dash, underscore and equals. Hashing keeps the key valid
     * in either store, so the choice of store stays reversible.
     */
    @Test
    void keyIsSafeEvenForPathsWithSlashesAndDots() {
        String id = ChunkId.of("src/guide/essentials/computed.md", "a".repeat(64));
        assertTrue(id.matches("[A-Za-z0-9_\\-=]+"), "not a portable key: " + id);
    }

    @Test
    void sameInputsProduceSameKey() {
        assertEquals(ChunkId.of("a.md", "h1"), ChunkId.of("a.md", "h1"));
    }

    /**
     * Two documents sharing a boilerplate paragraph need distinct rows, or
     * deleting one would orphan the other's chunk.
     */
    @Test
    void sameChunkInDifferentDocumentsGetsDifferentKeys() {
        assertNotEquals(ChunkId.of("a.md", "shared"), ChunkId.of("b.md", "shared"));
    }
}
