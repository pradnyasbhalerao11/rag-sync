package com.ragsync.ingest.parse;

import org.springframework.stereotype.Component;

/**
 * Markdown needs no extraction: the file content is already the text, and the
 * chunker understands markdown structure directly.
 *
 * Frontmatter is stripped, because a YAML block at the top of a file is
 * metadata rather than prose. Embedding "title: TransitionGroup / outline:
 * deep" wastes a call and pollutes retrieval.
 */
@Component
public class MarkdownParser implements DocumentParser {

    @Override
    public boolean supports(String filePath) {
        return filePath != null && filePath.endsWith(".md");
    }

    @Override
    public String extractText(String raw, String filePath) {
        return stripFrontmatter(raw);
    }

    private String stripFrontmatter(String raw) {
        if (raw == null || !raw.startsWith("---")) {
            return raw;
        }
        int firstBreak = raw.indexOf('\n');
        if (firstBreak < 0) {
            return raw;
        }
        int closing = raw.indexOf("\n---", firstBreak);
        if (closing < 0) {
            return raw;
        }
        int afterClosing = raw.indexOf('\n', closing + 1);
        return afterClosing < 0 ? "" : raw.substring(afterClosing + 1);
    }
}
