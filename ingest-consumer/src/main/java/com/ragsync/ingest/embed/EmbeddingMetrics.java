package com.ragsync.ingest.embed;

import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Latency and volume for embedding calls.
 *
 * Written before the provider, deliberately: measuring after the fact means
 * rerunning the whole corpus for numbers you could have collected the first
 * time.
 *
 * Latencies go into a fixed-size reservoir rather than a growing list so memory
 * stays flat across a long replay. p50/p95 from 4,096 samples is accurate
 * enough to report and cheap enough to always collect.
 */
@Component
public class EmbeddingMetrics {

    private static final int RESERVOIR = 4096;

    private final AtomicLong calls = new AtomicLong();
    private final AtomicLong textsEmbedded = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong vectorsCopied = new AtomicLong();
    private final LongAdder totalMillis = new LongAdder();

    private final long[] latencies = new long[RESERVOIR];
    private final AtomicLong writes = new AtomicLong();

    public void recordCall(int textCount, long millis) {
        calls.incrementAndGet();
        textsEmbedded.addAndGet(textCount);
        totalMillis.add(millis);
        latencies[(int) (writes.getAndIncrement() % RESERVOIR)] = millis;
    }

    /** A vector fetched from the store instead of recomputed — the dual-hash win. */
    public void recordVectorCopied() {
        vectorsCopied.incrementAndGet();
    }

    public void recordRetry() {
        retries.incrementAndGet();
    }

    public void recordFailure() {
        failures.incrementAndGet();
    }

    public Snapshot snapshot() {
        long callCount = calls.get();
        int sampled = (int) Math.min(writes.get(), RESERVOIR);
        long[] sorted = Arrays.copyOf(latencies, sampled);
        Arrays.sort(sorted);

        return new Snapshot(
                callCount,
                textsEmbedded.get(),
                vectorsCopied.get(),
                retries.get(),
                failures.get(),
                callCount == 0 ? 0 : totalMillis.sum() / callCount,
                percentile(sorted, 0.50),
                percentile(sorted, 0.95),
                totalMillis.sum());
    }

    private long percentile(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }

    public void reset() {
        calls.set(0);
        textsEmbedded.set(0);
        vectorsCopied.set(0);
        retries.set(0);
        failures.set(0);
        totalMillis.reset();
        writes.set(0);
        Arrays.fill(latencies, 0L);
    }

    public record Snapshot(
            long embeddingCalls,
            long textsEmbedded,
            long vectorsCopiedFromStore,
            long retries,
            long failures,
            long meanCallMillis,
            long p50CallMillis,
            long p95CallMillis,
            long totalEmbeddingMillis) {
    }
}
