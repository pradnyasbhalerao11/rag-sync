package com.ragsync.ingest.chunk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeenChunksTest {

    @Test
    void firstSightIsNewAndSecondIsReused() {
        SeenChunks seen = new SeenChunks();
        assertTrue(seen.markContentSeen("docs/a.md", "content1"));
        assertFalse(seen.markContentSeen("docs/a.md", "content1"));
    }

    /**
     * The case that motivated two hashes: the row is new because its link map
     * changed, but the vector already exists.
     */
    @Test
    void contentAndEmbedHashesAreTrackedIndependently() {
        SeenChunks seen = new SeenChunks();
        assertTrue(seen.markContentSeen("docs/a.md", "content1"));
        assertTrue(seen.markEmbedSeen("docs/a.md", "embed1"));

        assertTrue(seen.markContentSeen("docs/a.md", "content2"));
        assertFalse(seen.markEmbedSeen("docs/a.md", "embed1"),
                "same normalized text should not need a second embedding");
    }

    /**
     * Two documents sharing a boilerplate paragraph each need their own row,
     * because deleting one must not orphan the other's chunk.
     */
    @Test
    void sameChunkInDifferentDocumentsIsNewForEach() {
        SeenChunks seen = new SeenChunks();
        assertTrue(seen.markContentSeen("docs/a.md", "shared"));
        assertTrue(seen.markContentSeen("docs/b.md", "shared"));
    }

    @Test
    void forgetDropsBothMapsForThatDocumentOnly() {
        SeenChunks seen = new SeenChunks();
        seen.markContentSeen("docs/a.md", "c1");
        seen.markEmbedSeen("docs/a.md", "e1");
        seen.markContentSeen("docs/b.md", "c2");

        assertEquals(1, seen.forget("docs/a.md"));
        assertTrue(seen.markContentSeen("docs/a.md", "c1"), "new again after delete");
        assertTrue(seen.markEmbedSeen("docs/a.md", "e1"), "vector tracking cleared too");
        assertFalse(seen.markContentSeen("docs/b.md", "c2"), "other documents untouched");
    }

    @Test
    void forgettingAnUnknownDocumentIsHarmless() {
        assertEquals(0, new SeenChunks().forget("docs/nope.md"));
    }
}
