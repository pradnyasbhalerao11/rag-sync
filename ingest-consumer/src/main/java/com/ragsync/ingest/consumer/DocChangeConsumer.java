package com.ragsync.ingest.consumer;

import com.ragsync.ingest.model.DocChangeEvent;
import com.ragsync.ingest.stats.ReplayStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Counts what arrives and checks that it arrived
 * in the right order. Chunking, embedding and indexing land in weeks 2 and 3.
 *
 * Resist the urge to add them now. The point of this week is to prove the
 * transport is correct before anything expensive depends on it.
 */
@Component
public class DocChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(DocChangeConsumer.class);

    private final ReplayStats stats;

    public DocChangeConsumer(ReplayStats stats) {
        this.stats = stats;
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

        if (log.isDebugEnabled()) {
            log.debug("p{} off={} {} {} seq={}",
                    record.partition(), record.offset(), event.op(), event.docId(), event.seq());
        }

        // Commit only after the record is fully handled. Right now "handled"
        // means counted; later it will mean written to the index.
        ack.acknowledge();
    }
}
