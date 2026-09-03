package com.ragsync.ingest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One change to one document, as emitted by tools/extract_events.py.
 *
 * Field names are mapped explicitly rather than via a global naming strategy so
 * the wire contract is visible in the type itself.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DocChangeEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("doc_id") String docId,
        @JsonProperty("op") String op,
        @JsonProperty("seq") long seq,
        @JsonProperty("source_version") String sourceVersion,
        @JsonProperty("content_hash") String contentHash,
        @JsonProperty("committed_at") String committedAt) {

    public boolean isDelete() {
        return "delete".equals(op);
    }
}
