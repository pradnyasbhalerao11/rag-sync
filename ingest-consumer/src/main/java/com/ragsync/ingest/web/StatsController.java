package com.ragsync.ingest.web;

import com.ragsync.ingest.embed.EmbeddingMetrics;
import com.ragsync.ingest.embed.EmbeddingProvider;
import com.ragsync.ingest.stats.ReplayStats;
import com.ragsync.ingest.store.VectorStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsController {

    private final ReplayStats stats;
    private final EmbeddingMetrics embeddingMetrics;
    private final EmbeddingProvider embeddings;
    private final VectorStore store;

    public StatsController(ReplayStats stats,
                           EmbeddingMetrics embeddingMetrics,
                           EmbeddingProvider embeddings,
                           VectorStore store) {
        this.stats = stats;
        this.embeddingMetrics = embeddingMetrics;
        this.embeddings = embeddings;
        this.store = store;
    }

    @GetMapping("/stats")
    public Stats stats() {
        return new Stats(
                stats.snapshot(),
                embeddingMetrics.snapshot(),
                new StoreInfo(embeddings.modelId(), embeddings.dimensions(), store.countChunks()));
    }

    @PostMapping("/stats/reset")
    public ResponseEntity<Void> reset() {
        stats.reset();
        embeddingMetrics.reset();
        return ResponseEntity.noContent().build();
    }

    public record Stats(ReplayStats.Snapshot pipeline,
                        EmbeddingMetrics.Snapshot embedding,
                        StoreInfo store) {
    }

    public record StoreInfo(String model, int dimensions, long rowsInStore) {
    }
}
