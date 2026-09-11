package com.ragsync.ingest.parse;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarkdownParserTest {

    private final MarkdownParser parser = new MarkdownParser();

    @Test
    void supportsMarkdownOnly() {
        assertTrue(parser.supports("docs/guide.md"));
        assertFalse(parser.supports("src/Main.java"));
        assertFalse(parser.supports(null));
    }

    @Test
    void frontmatterIsStripped() {
        String raw = """
                ---
                title: TransitionGroup
                outline: deep
                ---
                # TransitionGroup

                Body text.
                """;
        String text = parser.extractText(raw, "a.md");
        assertFalse(text.contains("outline: deep"));
        assertTrue(text.startsWith("# TransitionGroup"));
    }

    @Test
    void documentWithoutFrontmatterIsUnchanged() {
        String raw = "# Title\n\nBody.";
        assertEquals(raw, parser.extractText(raw, "a.md"));
    }

    /** A horizontal rule is not frontmatter, and must not eat the document. */
    @Test
    void leadingHorizontalRuleDoesNotSwallowContent() {
        String raw = "---\n\n# Title\n\nBody.";
        String text = parser.extractText(raw, "a.md");
        assertTrue(text.contains("Body."));
    }
}
