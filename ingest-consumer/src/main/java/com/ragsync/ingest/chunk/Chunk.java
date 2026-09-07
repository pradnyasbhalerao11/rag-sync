package com.ragsync.ingest.chunk;

/**
 * One searchable piece of a document.
 *
 * @param index position within the document, 0-based. Useful for ordering and
 *              debugging, but deliberately NOT part of a chunk's identity: an
 *              edit near the top of a document shifts every later index, and if
 *              index were part of the hash, every chunk would look changed
 *              after any insertion.
 * @param text  the chunk's exact text. This is what gets embedded in week 3,
 *              and what gets hashed to decide whether the chunk is new.
 */
public record Chunk(int index, String text) {
}
