package com.ragsync.ingest.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Verifies the ordering guarantee this whole design rests on: every event for a
 * given document must arrive with a strictly increasing seq.
 *
 * This holds only because the producer keys by doc_id, so all events for one
 * document land on one partition, and one partition is consumed by one thread.
 * If someone later changes the partitioning key or reprocesses out of band,
 * this counter goes non-zero and you find out immediately instead of six weeks
 * later via a wrong answer in production.
 */
public class OrderingTracker {

    private final Map<String, Long> lastSeqByDoc = new ConcurrentHashMap<>();
    private final AtomicLong violations = new AtomicLong();

    /** @return true if this event was in order */
    public boolean observe(String docId, long seq) {
        Long previous = lastSeqByDoc.put(docId, seq);
        if (previous != null && seq <= previous) {
            violations.incrementAndGet();
            return false;
        }
        return true;
    }

    public long violations() {
        return violations.get();
    }

    public int trackedDocuments() {
        return lastSeqByDoc.size();
    }

    public void reset() {
        lastSeqByDoc.clear();
        violations.set(0);
    }
}
