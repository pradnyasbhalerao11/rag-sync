package com.ragsync.ingest.embed;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EmbeddingMetricsTest {

    @Test
    void tracksCallsAndTexts() {
        EmbeddingMetrics metrics = new EmbeddingMetrics();
        metrics.recordCall(16, 100);
        metrics.recordCall(16, 200);
        metrics.recordCall(4, 300);

        EmbeddingMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(3, snapshot.embeddingCalls());
        assertEquals(36, snapshot.textsEmbedded());
        assertEquals(200, snapshot.meanCallMillis());
    }

    @Test
    void computesPercentiles() {
        EmbeddingMetrics metrics = new EmbeddingMetrics();
        for (int i = 1; i <= 100; i++) {
            metrics.recordCall(1, i);
        }
        EmbeddingMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(50, snapshot.p50CallMillis());
        assertEquals(95, snapshot.p95CallMillis());
    }

    /** Memory must stay flat across a long replay. */
    @Test
    void latencyReservoirIsBounded() {
        EmbeddingMetrics metrics = new EmbeddingMetrics();
        for (int i = 0; i < 50_000; i++) {
            metrics.recordCall(1, 42);
        }
        EmbeddingMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(50_000, snapshot.embeddingCalls());
        assertEquals(42, snapshot.p50CallMillis());
    }

    @Test
    void countsVectorsCopiedRatherThanEmbedded() {
        EmbeddingMetrics metrics = new EmbeddingMetrics();
        metrics.recordVectorCopied();
        metrics.recordVectorCopied();
        assertEquals(2, metrics.snapshot().vectorsCopiedFromStore());
    }

    @Test
    void resetClearsEverything() {
        EmbeddingMetrics metrics = new EmbeddingMetrics();
        metrics.recordCall(10, 100);
        metrics.recordVectorCopied();
        metrics.reset();

        EmbeddingMetrics.Snapshot snapshot = metrics.snapshot();
        assertEquals(0, snapshot.embeddingCalls());
        assertEquals(0, snapshot.vectorsCopiedFromStore());
    }
}
