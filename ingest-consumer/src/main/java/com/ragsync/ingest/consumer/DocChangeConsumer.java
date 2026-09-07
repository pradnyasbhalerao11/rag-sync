package com.ragsync.ingest.consumer;

import com.ragsync.ingest.blob.BlobStore;
import com.ragsync.ingest.chunk.Chunk;
import com.ragsync.ingest.chunk.ChunkHasher;
import com.ragsync.ingest.chunk.Chunker;
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
 * Week 2: resolve each upsert to its document text, chunk it, and decide which
 * chunks are actually new.
 *
 * Still no embeddings and no index. The point of doing it in this order is that
 * the skip logic exists before anything expensive depends on it, so the slow
 * version of this pipeline never gets written.
 */
@Component
public class DocChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(DocChangeConsumer.class);

    private final ReplayStats stats;
    private final BlobStore blobStore;
    private final Chunker chunker;
    private final SeenChunks seenChunks;

    public DocChangeConsumer(ReplayStats stats,
                             BlobStore blobStore,
                             Chunker chunker,
                             SeenChunks seenChunks) {
        this.stats = stats;
        this.blobStore = blobStore;
        this.chunker = chunker;
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
                handleDelete(event);
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

    private void handleDelete(DocChangeEvent event) {
        int forgotten = seenChunks.forget(event.docId());
        if (log.isDebugEnabled()) {
            log.debug("delete {} dropped {} chunk hashes", event.docId(), forgotten);
        }
    }

    private void handleUpsert(DocChangeEvent event) {
        String markdown = blobStore.read(event.contentHash());
        if (markdown == null) {
            stats.recordBlobMiss();
            return;
        }

        List<Chunk> chunks = chunker.chunk(markdown);

        int newCount = 0;
        int reusedCount = 0;
        for (Chunk chunk : chunks) {
            String chunkHash = ChunkHasher.sha256(chunk.text());
            if (seenChunks.markSeen(event.docId(), chunkHash)) {
                newCount++;
                // week 3: this is where the embedding call goes
            } else {
                reusedCount++;
            }
        }

        stats.recordChunks(newCount, reusedCount);

        if (log.isDebugEnabled()) {
            log.debug("{} -> {} chunks ({} new, {} reused)",
                    event.docId(), chunks.size(), newCount, reusedCount);
        }
    }
}
