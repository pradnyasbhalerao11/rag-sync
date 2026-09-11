package com.ragsync.ingest.parse;

/**
 * Extracts plain text from a raw source file.
 *
 * This is an extension seam, not a feature. Markdown needs no extraction — the
 * raw bytes already are the text — so MarkdownParser is effectively an identity
 * function today. It exists so that adding PDF, HTML or DOCX later means adding
 * one implementation rather than threading a format check through the consumer.
 *
 * Deliberately NOT a full document/block model. A Document/DocumentBlock/
 * BlockType abstraction is the right shape once a second format actually needs
 * it; building it before then is nine classes doing the job of two.
 */
public interface DocumentParser {

    boolean supports(String filePath);

    /** @return plain text suitable for normalization and chunking */
    String extractText(String raw, String filePath);
}
