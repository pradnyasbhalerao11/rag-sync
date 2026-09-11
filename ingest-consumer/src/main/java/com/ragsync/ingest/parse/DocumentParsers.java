package com.ragsync.ingest.parse;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Picks a parser by file path, falling back to passing the text through
 * unchanged.
 *
 * The fallback is what stops an unknown extension from failing outright. A
 * .txt or .rst file is still text; it just gets no format-specific handling.
 */
@Component
public class DocumentParsers {

    private final List<DocumentParser> parsers;

    public DocumentParsers(List<DocumentParser> parsers) {
        this.parsers = parsers;
    }

    public String extractText(String raw, String filePath) {
        for (DocumentParser parser : parsers) {
            if (parser.supports(filePath)) {
                return parser.extractText(raw, filePath);
            }
        }
        return raw;
    }
}
