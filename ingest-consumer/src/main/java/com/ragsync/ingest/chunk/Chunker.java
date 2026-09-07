package com.ragsync.ingest.chunk;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a markdown document into chunks small enough to embed.
 *
 * The hard requirement is DETERMINISM: identical input must produce
 * byte-identical output every time. Content-hash deduplication is worthless
 * otherwise, because a chunker that varies its output makes every chunk look
 * new on every run, and the entire point of week 2 evaporates.
 *
 * That rules out randomness, timestamps, HashMap/HashSet iteration order, and
 * anything locale-dependent. Everything here is a straight pass over the lines.
 *
 * Strategy: split on markdown headings, since a heading is a natural semantic
 * boundary and the resulting chunk carries its own title, which retrieves
 * better than an arbitrary slice. Sections that are still too large are split
 * further on blank lines, never mid-paragraph.
 */
@Component
public class Chunker {

    private final int targetSize;
    private final int maxSize;

    public Chunker() {
        this(1000, 1500);
    }

    public Chunker(@Value("${app.chunk.target-size:1000}") int targetSize,
                   @Value("${app.chunk.max-size:1500}") int maxSize) {
        this.targetSize = targetSize;
        this.maxSize = maxSize;
    }

    public List<Chunk> chunk(String markdown) {
        List<Chunk> chunks = new ArrayList<>();
        if (markdown == null || markdown.isBlank()) {
            return chunks;
        }

        int index = 0;
        for (String section : splitOnHeadings(markdown)) {
            for (String piece : splitIfTooLarge(section)) {
                String text = piece.strip();
                if (!text.isEmpty()) {
                    chunks.add(new Chunk(index++, text));
                }
            }
        }
        return chunks;
    }

    /**
     * Split on lines starting with '#'. The heading line belongs to the section
     * it introduces, not the one before it.
     *
     * Fenced code blocks are tracked and skipped, because markdown code samples
     * routinely contain lines starting with '#' — shell comments, Python
     * comments, CSS ids. Splitting on those shreds the code block and produces
     * a chunk that begins mid-function.
     */
    private List<String> splitOnHeadings(String markdown) {
        List<String> sections = new ArrayList<>();
        List<String> current = new ArrayList<>();
        boolean insideFence = false;

        for (String line : markdown.split("\n", -1)) {
            String stripped = line.strip();

            if (stripped.startsWith("```") || stripped.startsWith("~~~")) {
                insideFence = !insideFence;
            }

            boolean isHeading = !insideFence && stripped.startsWith("#");
            if (isHeading && hasContent(current)) {
                sections.add(String.join("\n", current));
                current = new ArrayList<>();
            }
            current.add(line);
        }

        if (hasContent(current)) {
            sections.add(String.join("\n", current));
        }
        return sections;
    }

    /**
     * If a section exceeds maxSize, split it on blank lines and greedily refill
     * up to targetSize. A chunk cut through the middle of a sentence embeds
     * badly and reads badly when retrieved, so paragraph boundaries are
     * respected wherever possible.
     *
     * A single paragraph longer than maxSize cannot be split that way. Those are
     * hard-split at a character boundary: rare, ugly, but bounded — and it keeps
     * the guarantee that no chunk ever exceeds maxSize, which matters because
     * the embedding API will reject oversized input in week 3.
     */
    private List<String> splitIfTooLarge(String section) {
        if (section.length() <= maxSize) {
            return List.of(section);
        }

        List<String> pieces = new ArrayList<>();
        StringBuilder buffer = new StringBuilder();

        for (String paragraph : section.split("\n\n")) {
            if (paragraph.length() > maxSize) {
                if (!buffer.isEmpty()) {
                    pieces.add(buffer.toString());
                    buffer = new StringBuilder();
                }
                for (int i = 0; i < paragraph.length(); i += maxSize) {
                    pieces.add(paragraph.substring(i, Math.min(i + maxSize, paragraph.length())));
                }
                continue;
            }

            if (!buffer.isEmpty() && buffer.length() + 2 + paragraph.length() > targetSize) {
                pieces.add(buffer.toString());
                buffer = new StringBuilder(paragraph);
            } else {
                if (!buffer.isEmpty()) {
                    buffer.append("\n\n");
                }
                buffer.append(paragraph);
            }
        }

        if (!buffer.isEmpty()) {
            pieces.add(buffer.toString());
        }
        return pieces;
    }

    private boolean hasContent(List<String> lines) {
        return lines.stream().anyMatch(l -> !l.isBlank());
    }
}
