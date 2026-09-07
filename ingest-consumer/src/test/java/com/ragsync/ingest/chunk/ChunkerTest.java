package com.ragsync.ingest.chunk;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkerTest {

    private final Chunker chunker = new Chunker();

    @Test
    void identicalInputProducesIdenticalChunks() {
        String doc = "# A\n\nsome text\n\n# B\n\nmore text";
        assertEquals(chunker.chunk(doc), chunker.chunk(doc));
    }

    /** The whole thesis of week 2, as an assertion. */
    @Test
    void singleEditChangesExactlyOneChunk() {
        String before = "# A\n\nalpha\n\n# B\n\nbeta\n\n# C\n\ngamma";
        String after = "# A\n\nalpha\n\n# B\n\nbetaX\n\n# C\n\ngamma";

        List<Chunk> b = chunker.chunk(before);
        List<Chunk> a = chunker.chunk(after);

        assertEquals(b.size(), a.size());

        long differing = 0;
        for (int i = 0; i < b.size(); i++) {
            if (!b.get(i).text().equals(a.get(i).text())) {
                differing++;
            }
        }
        assertEquals(1, differing);
    }

    @Test
    void emptyAndBlankDocumentsProduceNoChunks() {
        assertTrue(chunker.chunk("").isEmpty());
        assertTrue(chunker.chunk("   \n\n  ").isEmpty());
        assertTrue(chunker.chunk(null).isEmpty());
    }

    @Test
    void noChunkExceedsMaxSize() {
        String longDoc = "# Title\n\n" + "word ".repeat(2000);
        for (Chunk chunk : chunker.chunk(longDoc)) {
            assertTrue(chunk.text().length() <= 1500,
                    "chunk " + chunk.index() + " was " + chunk.text().length());
        }
    }

    @Test
    void chunkIndicesAreSequentialFromZero() {
        List<Chunk> chunks = chunker.chunk("# A\n\nx\n\n# B\n\ny\n\n# C\n\nz");
        for (int i = 0; i < chunks.size(); i++) {
            assertEquals(i, chunks.get(i).index());
        }
    }

    @Test
    void headingsInsideCodeFencesDoNotSplit() {
        String doc = """
                # Config

                Set it up like this:

                ```bash
                # install the thing
                brew install thing
                # then run it
                thing --start
                ```

                That is all.
                """;

        List<Chunk> chunks = chunker.chunk(doc);

        assertEquals(1, chunks.size(), "code fence comments must not create sections");
        assertTrue(chunks.get(0).text().contains("brew install thing"));
    }

    @Test
    void documentWithoutLeadingHeadingStillChunks() {
        List<Chunk> chunks = chunker.chunk("intro paragraph\n\n# Later\n\nbody");
        assertEquals(2, chunks.size());
        assertTrue(chunks.get(0).text().startsWith("intro"));
    }

    @Test
    void documentWithNoHeadingsProducesOneChunk() {
        List<Chunk> chunks = chunker.chunk("just\n\nsome\n\nparagraphs");
        assertEquals(1, chunks.size());
    }

    @Test
    void oversizedSingleParagraphIsHardSplitButBounded() {
        String doc = "# T\n\n" + "x".repeat(4000);
        List<Chunk> chunks = chunker.chunk(doc);
        assertTrue(chunks.size() > 1);
        chunks.forEach(c -> assertTrue(c.text().length() <= 1500));
    }

    @Test
    void differentTextProducesDifferentHashes() {
        assertFalse(ChunkHasher.sha256("alpha").equals(ChunkHasher.sha256("beta")));
        assertEquals(ChunkHasher.sha256("alpha"), ChunkHasher.sha256("alpha"));
        assertEquals(64, ChunkHasher.sha256("alpha").length());
    }
}
