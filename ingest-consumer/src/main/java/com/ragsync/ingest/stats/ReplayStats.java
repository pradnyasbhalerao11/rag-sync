package com.ragsync.ingest.stats;

import com.ragsync.ingest.model.DocChangeEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The numbers that make week 1 "done". Everything here is a baseline you will
 * compare against once real work is added to the pipeline.
 */
@Component
public class ReplayStats {

    private final Map<String, AtomicLong> countsByOp = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicLong> countsByPartition = new ConcurrentHashMap<>();
    private final Set<String> distinctDocs = ConcurrentHashMap.newKeySet();
    private final OrderingTracker ordering = new OrderingTracker();
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong deserializationFailures = new AtomicLong();

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

    public Snapshot snapshot() {
        long elapsedMillis = Math.max(1, lastEventAtMillis - firstEventAtMillis);
        double perSecond = total.get() * 1000.0 / elapsedMillis;

        Map<String, Long> ops = new TreeMap<>();
        countsByOp.forEach((k, v) -> ops.put(k, v.get()));

        Map<Integer, Long> partitions = new TreeMap<>();
        countsByPartition.forEach((k, v) -> partitions.put(k, v.get()));

        return new Snapshot(
                total.get(),
                ops,
                distinctDocs.size(),
                partitions,
                ordering.violations(),
                deserializationFailures.get(),
                elapsedMillis,
                Math.round(perSecond * 10) / 10.0);
    }

    public void reset() {
        countsByOp.clear();
        countsByPartition.clear();
        distinctDocs.clear();
        ordering.reset();
        total.set(0);
        deserializationFailures.set(0);
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
            long elapsedMillis,
            double eventsPerSecond) {
    }
}
