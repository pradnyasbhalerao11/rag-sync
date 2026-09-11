package com.ragsync.ingest.consumer;

import com.ragsync.ingest.blob.BlobStore;
import com.ragsync.ingest.chunk.ChunkPipeline;
import com.ragsync.ingest.chunk.HashedChunk;
import com.ragsync.ingest.chunk.SeenChunks;
import com.ragsync.ingest.model.DocChangeEvent;
import com.ragsync.ingest.stats.ReplayStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Still no embeddings and no index — this week only decides what WOULD be
 * embedded, and counts it.
 *
 * Building the skip logic before anything expensive depends on it means the
 * slow version of this pipeline never gets written.
 */
@Component
public class DocChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(DocChangeConsumer.class);

    private final ReplayStats stats;
    private final BlobStore blobStore;
    private final ChunkPipeline pipeline;
    private final SeenChunks seenChunks;

    public DocChangeConsumer(ReplayStats stats,
                             BlobStore blobStore,
                             ChunkPipeline pipeline,
                             SeenChunks seenChunks) {
        this.stats = stats;
        this.blobStore = blobStore;
        this.pipeline = pipeline;
        this.seenChunks = seenChunks;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(ConsumerRecord<String, DocChangeEvent> record, Acknowledgment ack) {
        DocChangeEvent event = record.value();

        if (event == null) {
            // ErrorHandlingDeserializer hands us a null on a parse failure
            stats.recordDeserializationFailure();
            ack.acknowledge();
            return;
        }

        stats.record(event, record.partition());

        try {
            if (event.isDelete()) {
                seenChunks.forget(event.docId());
            } else {
                handleUpsert(event);
            }
        } catch (RuntimeException e) {
            // Week 5 routes this to a dead-letter topic. For now: never let one
            // bad document stall its partition forever.
            log.warn("failed to process {} {}: {}", event.op(), event.docId(), e.toString());
        }

        // Commit only after the record is fully handled.
        ack.acknowledge();
    }

    private void handleUpsert(DocChangeEvent event) {
        String raw = blobStore.read(event.contentHash());
        if (raw == null) {
            stats.recordBlobMiss();
            return;
        }

        List<HashedChunk> chunks = pipeline.process(raw, event.docId());

        int rowsToWrite = 0;
        int embeddingsNeeded = 0;
        int unchanged = 0;
        int vectorsReused = 0;

        for (HashedChunk chunk : chunks) {
            if (!seenChunks.markContentSeen(event.docId(), chunk.contentHash())) {
                unchanged++;                       // identical row already present
                continue;
            }
            rowsToWrite++;

            if (seenChunks.markEmbedSeen(event.docId(), chunk.embedHash())) {
                embeddingsNeeded++;                // week 3: the API call goes here
            } else {
                vectorsReused++;                   // same text, different links
            }
        }

        stats.recordChunks(embeddingsNeeded, unchanged + vectorsReused);
        stats.recordRows(rowsToWrite, vectorsReused);

        if (log.isDebugEnabled()) {
            log.debug("{} -> {} chunks ({} embed, {} vector-reuse, {} unchanged)",
                    event.docId(), chunks.size(), embeddingsNeeded, vectorsReused, unchanged);
        }
    }
}
