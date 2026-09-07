package com.ragsync.ingest.chunk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeenChunksTest {

    @Test
    void firstSightIsNewAndSecondIsReused() {
        SeenChunks seen = new SeenChunks();
        assertTrue(seen.markSeen("docs/a.md", "hash1"));
        assertFalse(seen.markSeen("docs/a.md", "hash1"));
    }

    /**
     * Two documents sharing a boilerplate paragraph each need their own copy,
     * because deleting one must not orphan the other's chunk.
     */
    @Test
    void sameChunkInDifferentDocumentsIsNewForEach() {
        SeenChunks seen = new SeenChunks();
        assertTrue(seen.markSeen("docs/a.md", "shared"));
        assertTrue(seen.markSeen("docs/b.md", "shared"));
    }

    @Test
    void forgetDropsEveryChunkForThatDocumentOnly() {
        SeenChunks seen = new SeenChunks();
        seen.markSeen("docs/a.md", "h1");
        seen.markSeen("docs/a.md", "h2");
        seen.markSeen("docs/b.md", "h3");

        assertEquals(2, seen.forget("docs/a.md"));
        assertTrue(seen.markSeen("docs/a.md", "h1"), "should be new again after delete");
        assertFalse(seen.markSeen("docs/b.md", "h3"), "other documents untouched");
    }

    @Test
    void forgettingAnUnknownDocumentIsHarmless() {
        assertEquals(0, new SeenChunks().forget("docs/nope.md"));
    }
}
