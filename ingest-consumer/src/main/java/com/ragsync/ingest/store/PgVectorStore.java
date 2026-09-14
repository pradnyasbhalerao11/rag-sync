package com.ragsync.ingest.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Postgres with the pgvector extension.
 *
 * Chosen over a hosted search service for a project that is still iterating:
 * one more container next to Kafka, no credentials, no quota, no network, and
 * the whole store is inspectable with psql. Weeks 4 and 6 become ordinary SQL —
 * the orphan sweep is a DELETE with a WHERE clause, and finding vectors from a
 * retired model is a SELECT.
 *
 * Batched writes. One statement per chunk would dominate the wall clock the way
 * unbatched embedding calls would.
 */
@Component
@ConditionalOnProperty(name = "app.store.provider", havingValue = "pgvector",
        matchIfMissing = true)
public class PgVectorStore implements VectorStore {

    private static final Logger log = LoggerFactory.getLogger(PgVectorStore.class);

    private final JdbcTemplate jdbc;
    private final int batchSize;

    public PgVectorStore(JdbcTemplate jdbc,
                         @Value("${app.store.batch-size:200}") int batchSize) {
        this.jdbc = jdbc;
        this.batchSize = batchSize;
        log.info("vector store: pgvector, batch {}", batchSize);
    }

    @Override
    public void upsert(List<IndexedChunk> chunks) {
        for (int start = 0; start < chunks.size(); start += batchSize) {
            upsertBatch(chunks.subList(start, Math.min(start + batchSize, chunks.size())));
        }
    }

    private void upsertBatch(List<IndexedChunk> chunks) {
        // ON CONFLICT DO UPDATE rather than INSERT: writing the same chunk twice
        // has to be a no-op update, not an error, or replaying the same history
        // would fail instead of being idempotent.
        String sql = """
                INSERT INTO chunks (chunk_id, doc_id, source_version, chunk_index,
                                    content_hash, embed_hash, embedding_model,
                                    text, links, embedding, indexed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::vector, now())
                ON CONFLICT (chunk_id) DO UPDATE SET
                    doc_id = EXCLUDED.doc_id,
                    source_version = EXCLUDED.source_version,
                    chunk_index = EXCLUDED.chunk_index,
                    content_hash = EXCLUDED.content_hash,
                    embed_hash = EXCLUDED.embed_hash,
                    embedding_model = EXCLUDED.embedding_model,
                    text = EXCLUDED.text,
                    links = EXCLUDED.links,
                    embedding = EXCLUDED.embedding,
                    indexed_at = now()
                """;

        jdbc.batchUpdate(sql, chunks, chunks.size(), (PreparedStatement ps, IndexedChunk c) -> {
            ps.setString(1, c.chunkId());
            ps.setString(2, c.docId());
            ps.setString(3, c.sourceVersion());
            ps.setInt(4, c.chunkIndex());
            ps.setString(5, c.contentHash());
            ps.setString(6, c.embedHash());
            ps.setString(7, c.embeddingModel());
            ps.setString(8, c.text());
            ps.setString(9, serializeLinks(c.links()));
            ps.setString(10, toVectorLiteral(c.vector()));
        });
    }

    @Override
    public Set<String> contentHashes(String docId) {
        List<String> rows = jdbc.queryForList(
                "SELECT content_hash FROM chunks WHERE doc_id = ?", String.class, docId);
        return new HashSet<>(rows);
    }

    @Override
    public Optional<float[]> vectorFor(String docId, String embedHash) {
        List<String> rows = jdbc.queryForList(
                "SELECT embedding::text FROM chunks WHERE doc_id = ? AND embed_hash = ? LIMIT 1",
                String.class, docId, embedHash);
        return rows.isEmpty() ? Optional.empty() : Optional.of(parseVector(rows.get(0)));
    }

    @Override
    public int deleteByDocument(String docId) {
        return jdbc.update("DELETE FROM chunks WHERE doc_id = ?", docId);
    }

    @Override
    public long countChunks() {
        Long count = jdbc.queryForObject("SELECT count(*) FROM chunks", Long.class);
        return count == null ? 0 : count;
    }

    /** pgvector accepts a vector as the text form '[1.0,2.0,3.0]'. */
    static String toVectorLiteral(float[] vector) {
        StringBuilder builder = new StringBuilder(vector.length * 8 + 2);
        builder.append('[');
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(vector[i]);
        }
        return builder.append(']').toString();
    }

    static float[] parseVector(String literal) {
        String body = literal.substring(1, literal.length() - 1);
        if (body.isEmpty()) {
            return new float[0];
        }
        String[] parts = body.split(",");
        float[] vector = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vector[i] = Float.parseFloat(parts[i].trim());
        }
        return vector;
    }

    static String serializeLinks(Map<String, String> links) {
        if (links == null || links.isEmpty()) {
            return "";
        }
        List<String> entries = new ArrayList<>(links.size());
        links.forEach((key, value) -> entries.add(key + "=" + value));
        return String.join("\n", entries);
    }
}
