package com.ragsync.ingest.blob;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves a content hash to the document text it names.
 *
 * Events carry a reference, not a payload. That is the right shape: an event
 * says "document X changed and its content is hash H", and the consumer fetches
 * the content separately. In a production system this class would talk to S3, a
 * CMS, or a document service. Here it reads a local content-addressed directory
 * written by tools/export_blobs.py.
 *
 * Keeping content out of the event also keeps Kafka messages small — a 4,000
 * word document is roughly 25KB, and individual documents can exceed Kafka's
 * default 1MB message limit.
 *
 * Layout mirrors git's loose object store:
 *     data/blobs/4c/2ecab69c8d926671e1414faf1e291d0129c9df
 */
@Component
public class BlobStore {

    private final Path root;

    public BlobStore(@Value("${app.blob-dir:data/blobs}") String blobDir) {
        this.root = Path.of(blobDir);
    }

    /**
     * @return the document text, or null if the blob is not in the store.
     *         Null rather than an exception because a missing blob is an
     *         expected condition — a shallow clone, or export_blobs not yet
     *         run — and the consumer needs to count it rather than crash.
     */
    public String read(String contentHash) {
        if (contentHash == null || contentHash.length() < 3) {
            return null;
        }
        Path path = root.resolve(contentHash.substring(0, 2))
                        .resolve(contentHash.substring(2));
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    public Path root() {
        return root;
    }
}
