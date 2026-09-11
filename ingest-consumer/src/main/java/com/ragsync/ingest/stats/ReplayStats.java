package com.ragsync.ingest.stats;

import com.ragsync.ingest.model.DocChangeEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Everything week 1 and week 2 measure.
 *
 * One singleton bean, three consumer threads calling into it simultaneously, so
 * every field is a concurrent type. A plain HashMap would corrupt under
 * concurrent writes and a plain long++ would lose increments — the counters
 * would be quietly wrong rather than visibly broken, which is worse.
 */
@Component
public class ReplayStats {

    // week 1: transport
    private final Map<String, AtomicLong> countsByOp = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> countsByPartition = new ConcurrentHashMap<>();
    private final Set<String> distinctDocs = ConcurrentHashMap.newKeySet();
    private final OrderingTracker ordering = new OrderingTracker();
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong deserializationFailures = new AtomicLong();

    // week 2: chunking
    private final AtomicLong chunksTotal = new AtomicLong();
    private final AtomicLong chunksNew = new AtomicLong();
    private final AtomicLong chunksReused = new AtomicLong();
    private final AtomicLong blobMisses = new AtomicLong();
    private final AtomicLong documentsChunked = new AtomicLong();
    private final AtomicLong rowsWritten = new AtomicLong();
    private final AtomicLong vectorsReused = new AtomicLong();

    private volatile long firstEventAtMillis = 0L;
    private volatile long lastEventAtMillis = 0L;

    public void record(DocChangeEvent event, int partition) {
        long now = System.currentTimeMillis();
        if (firstEventAtMillis == 0L) {
            firstEventAtMillis = now;
        }
        lastEventAtMillis = now;

        total.incrementAndGet();
        countsByOp.computeIfAbsent(event.op(), k -> new AtomicLong()).incrementAndGet();
        countsByPartition.computeIfAbsent(partition, k -> new AtomicLong()).incrementAndGet();
        distinctDocs.add(event.docId());
        ordering.observe(event.docId(), event.seq());
    }

    public void recordDeserializationFailure() {
        deserializationFailures.incrementAndGet();
    }

    public void recordBlobMiss() {
        blobMisses.incrementAndGet();
    }

    /**
     * rowsToWrite  index rows that need writing (content changed)
     * vectorReuse  of those, how many skipped the embedding API because an
     *              identical normalized text already had a vector
     */
    public void recordRows(int rowsToWrite, int vectorReuse) {
        rowsWritten.addAndGet(rowsToWrite);
        vectorsReused.addAndGet(vectorReuse);
    }

    public void recordChunks(int newCount, int reusedCount) {
        documentsChunked.incrementAndGet();
        chunksNew.addAndGet(newCount);
        chunksReused.addAndGet(reusedCount);
        chunksTotal.addAndGet(newCount + reusedCount);
    }

    public Snapshot snapshot() {
        long elapsedMillis = Math.max(1, lastEventAtMillis - firstEventAtMillis);

        Map<String, Long> ops = new TreeMap<>();
        countsByOp.forEach((k, v) -> ops.put(k, v.get()));

        Map<Integer, Long> partitions = new TreeMap<>();
        countsByPartition.forEach((k, v) -> partitions.put(k, v.get()));

        long chunks = chunksTotal.get();
        double reuseRate = chunks == 0 ? 0.0 : (double) chunksReused.get() / chunks;

        return new Snapshot(
                total.get(),
                ops,
                distinctDocs.size(),
                partitions,
                ordering.violations(),
                deserializationFailures.get(),
                documentsChunked.get(),
                chunks,
                chunksNew.get(),
                chunksReused.get(),
                rowsWritten.get(),
                vectorsReused.get(),
                Math.round(reuseRate * 1000) / 1000.0,
                blobMisses.get(),
                elapsedMillis);
    }

    public void reset() {
        countsByOp.clear();
        countsByPartition.clear();
        distinctDocs.clear();
        ordering.reset();
        total.set(0);
        deserializationFailures.set(0);
        chunksTotal.set(0);
        chunksNew.set(0);
        chunksReused.set(0);
        rowsWritten.set(0);
        vectorsReused.set(0);
        blobMisses.set(0);
        documentsChunked.set(0);
        firstEventAtMillis = 0L;
        lastEventAtMillis = 0L;
    }

    public record Snapshot(
            long totalEvents,
            Map<String, Long> eventsByOp,
            int distinctDocuments,
            Map<Integer, Long> eventsByPartition,
            long orderingViolations,
            long deserializationFailures,
            long documentsChunked,
            long chunksTotal,
            long chunksNew,
            long chunksReused,
            long rowsWritten,
            long vectorsReusedAcrossLinks,
            double reuseRate,
            long blobMisses,
            long elapsedMillis) {
    }
}
