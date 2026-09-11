package com.ragsync.ingest.chunk;

import com.ragsync.ingest.normalize.OpaqueUrlNormalizer;
import com.ragsync.ingest.parse.DocumentParsers;
import com.ragsync.ingest.parse.MarkdownParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkPipelineTest {

    private final ChunkPipeline pipeline = new ChunkPipeline(
            new DocumentParsers(List.of(new MarkdownParser())),
            new OpaqueUrlNormalizer(),
            new Chunker());

    private static String base64(int length) {
        StringBuilder builder = new StringBuilder();
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (int i = 0; i < length; i++) {
            builder.append(alphabet.charAt(i % alphabet.length()));
        }
        return builder.toString();
    }

    /**
     * Regression for the corpus case: a 2,655-character encoded playground URL
     * used to hard-split into meaningless base64 fragments.
     */
    @Test
    void hugeEncodedUrlDoesNotBecomeSeveralChunks() {
        String doc = "# Demo\n\nTry it:\n\n[Playground](https://sfc.vuejs.org/#"
                + base64(2655) + ")\n\nThat is all.";

        List<HashedChunk> chunks = pipeline.process(doc, "a.md");

        assertEquals(1, chunks.size());
        assertFalse(chunks.get(0).text().contains(base64(200)),
                "no base64 should reach the embedding representation");
    }

    @Test
    void originalUrlIsRecoverableFromTheChunk() {
        String url = "https://sfc.vuejs.org/#" + base64(1500);
        List<HashedChunk> chunks = pipeline.process("# T\n\n[Play](" + url + ")", "a.md");

        assertEquals(1, chunks.size());
        assertTrue(chunks.get(0).displayText().contains(url));
    }

    /**
     * The whole point of splitting identity in two: a regenerated permalink
     * must not cost an embedding call.
     */
    @Test
    void urlOnlyChangeMovesContentHashButNotEmbedHash() {
        String before = "# T\n\n[Play](https://x.example/#" + base64(500) + ")";
        String after = "# T\n\n[Play](https://x.example/#" + base64(600) + ")";

        HashedChunk a = pipeline.process(before, "a.md").get(0);
        HashedChunk b = pipeline.process(after, "a.md").get(0);

        assertEquals(a.embedHash(), b.embedHash(), "same prose, same vector");
        assertNotEquals(a.contentHash(), b.contentHash(), "different link, new row");
    }

    @Test
    void proseChangeMovesBothHashes() {
        HashedChunk a = pipeline.process("# T\n\nalpha", "a.md").get(0);
        HashedChunk b = pipeline.process("# T\n\nbeta", "a.md").get(0);

        assertNotEquals(a.embedHash(), b.embedHash());
        assertNotEquals(a.contentHash(), b.contentHash());
    }

    @Test
    void pipelineIsDeterministic() {
        String doc = "# A\n\n[x](https://a.example/#" + base64(400) + ")\n\n## B\n\nbody";
        List<HashedChunk> first = pipeline.process(doc, "a.md");
        List<HashedChunk> second = pipeline.process(doc, "a.md");

        assertEquals(first.size(), second.size());
        for (int i = 0; i < first.size(); i++) {
            assertEquals(first.get(i).contentHash(), second.get(i).contentHash());
            assertEquals(first.get(i).embedHash(), second.get(i).embedHash());
        }
    }

    @Test
    void onlyLinksAppearingInAChunkAreAttachedToIt() {
        String doc = "# A\n\n[one](https://a.example/#" + base64(400) + ")\n\n"
                + "# B\n\n[two](https://b.example/#" + base64(400) + ")";

        List<HashedChunk> chunks = pipeline.process(doc, "a.md");
        assertEquals(2, chunks.size());
        assertEquals(1, chunks.get(0).links().size());
        assertEquals(1, chunks.get(1).links().size());
    }

    /**
     * With no links the two hashes still differ, because contentHash mixes in a
     * separator and an empty link block. That is fine — what matters is that
     * each is stable, not that they coincide.
     */
    @Test
    void hashesAreStableForDocumentsWithoutUrls() {
        HashedChunk first = pipeline.process("# Title\n\nJust prose.", "a.md").get(0);
        HashedChunk second = pipeline.process("# Title\n\nJust prose.", "a.md").get(0);

        assertEquals(first.embedHash(), second.embedHash());
        assertEquals(first.contentHash(), second.contentHash());
        assertTrue(first.links().isEmpty());
    }
}
