package com.ragsync.ingest.chunk;

import java.util.Map;

/**
 * A chunk with both of its identities computed.
 *
 * Two hashes, because two different questions need answering and they do not
 * always have the same answer:
 *
 *   embedHash    = sha256(normalized text)
 *                  "do I need to call the embedding API?"
 *
 *   contentHash  = sha256(normalized text + serialized links)
 *                  "do I need to rewrite this index row?"
 *
 * The cases this separation buys:
 *
 *   prose edited          both change   -> embed and write
 *   only a URL changed    embed same    -> write the row, REUSE the vector
 *   two chunks whose text normalizes identically but hold different URLs
 *                         embed same,   -> two distinct rows sharing one
 *                         content differs   vector, one embedding call
 *
 * Neither hash includes the chunk index. Position is deliberately excluded
 * from identity: if it were included, inserting a paragraph near the top of a
 * document would shift every later index and make every chunk below it look
 * changed, collapsing the reuse rate on exactly the edits where it matters
 * most.
 */
public record HashedChunk(
        int index,
        String text,
        String embedHash,
        String contentHash,
        Map<String, String> links) {

    /** The chunk as a reader should see it, with real URLs restored. */
    public String displayText() {
        String restored = text;
        for (Map.Entry<String, String> entry : links.entrySet()) {
            restored = restored.replace(entry.getKey(), entry.getValue());
        }
        return restored;
    }
}
