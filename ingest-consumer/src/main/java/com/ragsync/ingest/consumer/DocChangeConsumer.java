package com.ragsync.ingest.consumer;

import com.ragsync.ingest.blob.BlobStore;
import com.ragsync.ingest.chunk.ChunkPipeline;
import com.ragsync.ingest.chunk.HashedChunk;
import com.ragsync.ingest.embed.EmbeddingMetrics;
import com.ragsync.ingest.embed.EmbeddingProvider;
import com.ragsync.ingest.model.DocChangeEvent;
import com.ragsync.ingest.stats.ReplayStats;
import com.ragsync.ingest.store.ChunkId;
import com.ragsync.ingest.store.IndexedChunk;
import com.ragsync.ingest.store.VectorStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Chunks that are genuinely new get embedded and written to the vector store.
 *
 * The dedupe built in week 2 finally avoids something expensive here: a reused
 * chunk skips both an embedding call and a write.
 *
 * SeenChunks is gone. The store is now the source of truth for what has been
 * embedded — an in-memory set alongside a real store is a second thing that can
 * disagree, and a restart would re-embed the entire corpus.
 */
@Component
public class DocChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(DocChangeConsumer.class);

    private final ReplayStats stats;
    private final BlobStore blobStore;
    private final ChunkPipeline pipeline;
    private final EmbeddingProvider embeddings;
    private final EmbeddingMetrics embeddingMetrics;
    private final VectorStore store;

    public DocChangeConsumer(ReplayStats stats,
                             BlobStore blobStore,
                             ChunkPipeline pipeline,
                             EmbeddingProvider embeddings,
                             EmbeddingMetrics embeddingMetrics,
                             VectorStore store) {
        this.stats = stats;
        this.blobStore = blobStore;
        this.pipeline = pipeline;
        this.embeddings = embeddings;
        this.embeddingMetrics = embeddingMetrics;
        this.store = store;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(ConsumerRecord<String, DocChangeEvent> record, Acknowledgment ack) {
        DocChangeEvent event = record.value();

        if (event == null) {
            stats.recordDeserializationFailure();
            ack.acknowledge();
            return;
        }

        stats.record(event, record.partition());

        try {
            if (event.isDelete()) {
                // Week 4 wires this to store.deleteByDocument(). Leaving it
                // unwired is deliberate: a deleted document staying searchable
                // is exactly the failure this project exists to fix, and it is
                // worth seeing it broken before fixing it.
                log.debug("delete {} — index deletion lands in week 4", event.docId());
            } else {
                handleUpsert(event);
            }
        } catch (RuntimeException e) {
            // Week 5 routes this to a dead-letter topic. For now: never let one
            // bad document stall its partition forever.
            log.warn("failed to process {} {}: {}", event.op(), event.docId(), e.toString());
        }

        // Commit only after the record is fully handled. "Handled" now means
        // embedded and written, so a crash mid-document replays it rather than
        // losing it.
        ack.acknowledge();
    }

    private void handleUpsert(DocChangeEvent event) {
        String raw = blobStore.read(event.contentHash());
        if (raw == null) {
            stats.recordBlobMiss();
            return;
        }

        List<HashedChunk> chunks = pipeline.process(raw, event.docId());
        if (chunks.isEmpty()) {
            stats.recordChunks(0, 0);
            return;
        }

        // One query per document rather than an in-memory set that drifts from
        // reality and is wiped by a restart.
        Set<String> alreadyStored = store.contentHashes(event.docId());

        List<HashedChunk> toEmbed = new ArrayList<>();
        List<IndexedChunk> rows = new ArrayList<>();
        int unchanged = 0;
        int vectorsCopied = 0;

        for (HashedChunk chunk : chunks) {
            if (alreadyStored.contains(chunk.contentHash())) {
                unchanged++;
                continue;
            }

            // Content changed but the normalized text did not — a regenerated
            // permalink, say. Rewrite the row, reuse the vector, no API call.
            Optional<float[]> existing = store.vectorFor(event.docId(), chunk.embedHash());
            if (existing.isPresent()) {
                rows.add(row(event, chunk, existing.get()));
                vectorsCopied++;
                embeddingMetrics.recordVectorCopied();
            } else {
                toEmbed.add(chunk);
            }
        }

        if (!toEmbed.isEmpty()) {
            List<String> texts = toEmbed.stream().map(HashedChunk::text).toList();
            List<float[]> vectors = embeddings.embed(texts);
            for (int i = 0; i < toEmbed.size(); i++) {
                rows.add(row(event, toEmbed.get(i), vectors.get(i)));
            }
        }

        if (!rows.isEmpty()) {
            store.upsert(rows);
        }

        stats.recordChunks(toEmbed.size(), unchanged + vectorsCopied);
        stats.recordRows(rows.size(), vectorsCopied);

        if (log.isDebugEnabled()) {
            log.debug("{} -> {} chunks ({} embedded, {} vector-copied, {} unchanged)",
                    event.docId(), chunks.size(), toEmbed.size(), vectorsCopied, unchanged);
        }
    }

    private IndexedChunk row(DocChangeEvent event, HashedChunk chunk, float[] vector) {
        return new IndexedChunk(
                ChunkId.of(event.docId(), chunk.contentHash()),
                event.docId(),
                event.sourceVersion(),
                chunk.index(),
                chunk.contentHash(),
                chunk.embedHash(),
                embeddings.modelId(),
                chunk.displayText(),
                chunk.links(),
                vector);
    }
}
