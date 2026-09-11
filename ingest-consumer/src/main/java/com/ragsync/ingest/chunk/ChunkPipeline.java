package com.ragsync.ingest.chunk;

import com.ragsync.ingest.normalize.ContentNormalizer;
import com.ragsync.ingest.normalize.NormalizedContent;
import com.ragsync.ingest.parse.DocumentParsers;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * parse -> normalize -> chunk -> hash
 *
 * The one place the ordering is expressed, so the consumer does not have to
 * know it. Each stage answers one question:
 *
 *   parse      what is the text in this file?
 *   normalize  what should the embedding representation be?
 *   chunk      how do meaningful units fit under the size limit?
 *   hash       has this exact chunk changed?
 *
 * Every stage must be deterministic. Identical input has to produce
 * byte-identical chunks and hashes, or content-addressed dedupe is worthless:
 * a pipeline that varies its output makes every chunk look new on every run.
 */
@Component
public class ChunkPipeline {

    private final DocumentParsers parsers;
    private final ContentNormalizer normalizer;
    private final Chunker chunker;

    public ChunkPipeline(DocumentParsers parsers,
                         ContentNormalizer normalizer,
                         Chunker chunker) {
        this.parsers = parsers;
        this.normalizer = normalizer;
        this.chunker = chunker;
    }

    public List<HashedChunk> process(String raw, String filePath) {
        String text = parsers.extractText(raw, filePath);
        NormalizedContent normalized = normalizer.normalize(text);

        List<HashedChunk> out = new ArrayList<>();
        for (Chunk chunk : chunker.chunk(normalized.text())) {
            Map<String, String> chunkLinks = normalized.linksIn(chunk.text());
            String serialized = serialize(chunkLinks);

            out.add(new HashedChunk(
                    chunk.index(),
                    chunk.text(),
                    ChunkHasher.sha256(chunk.text()),
                    ChunkHasher.sha256(chunk.text() + "\u0000" + serialized),
                    chunkLinks));
        }
        return out;
    }

    private String serialize(Map<String, String> links) {
        if (links.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : links.entrySet()) {
            if (!builder.isEmpty()) {
                builder.append('\n');
            }
            builder.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return builder.toString();
    }
}
