package com.ragsync.ingest.normalize;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Text prepared for chunking, plus whatever was lifted out of it.
 *
 * @param text  what the chunker, hasher and embedding model see
 * @param links placeholder -> original URL. MUST be a LinkedHashMap: the
 *              iteration order feeds into contentHash, and a HashMap would
 *              produce a different hash on different JVM runs, silently
 *              invalidating index rows at random.
 */
public record NormalizedContent(String text, Map<String, String> links) {

    public static NormalizedContent unchanged(String text) {
        return new NormalizedContent(text, new LinkedHashMap<>());
    }

    /**
     * Deterministic serialization, used both for contentHash and for the
     * index row. Insertion order, newline separated, no JSON library involved
     * so there is no serializer-version risk to the hash.
     */
    public String serializeLinks() {
        return links.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("\n"));
    }

    /** Put the original URLs back. Used when presenting a retrieved chunk. */
    public static String restore(String text, Map<String, String> links) {
        String restored = text;
        for (Map.Entry<String, String> entry : links.entrySet()) {
            restored = restored.replace(entry.getKey(), entry.getValue());
        }
        return restored;
    }

    /** Only the placeholders that actually appear in this chunk's text. */
    public Map<String, String> linksIn(String chunkText) {
        Map<String, String> subset = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : links.entrySet()) {
            if (chunkText.contains(entry.getKey())) {
                subset.put(entry.getKey(), entry.getValue());
            }
        }
        return subset;
    }
}
