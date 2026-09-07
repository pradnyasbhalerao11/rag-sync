package com.ragsync.ingest.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One change to one document, as emitted by tools/extract_events.py.
 *
 * Field names are mapped explicitly rather than via a global naming strategy so
 * the wire contract is visible in the type itself. @JsonAlias on seq accepts
 * both "seq" and "doc_seq", because producer and consumer evolve separately and
 * a silent 0 here would quietly break ordering verification.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DocChangeEvent(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("doc_id") String docId,
        @JsonProperty("op") String op,
        @JsonProperty("seq") @JsonAlias("doc_seq") long seq,
        @JsonProperty("source_version") String sourceVersion,
        @JsonProperty("content_hash") String contentHash,
        @JsonProperty("committed_at") String committedAt) {

    public boolean isDelete() {
        return "delete".equals(op);
    }
}
