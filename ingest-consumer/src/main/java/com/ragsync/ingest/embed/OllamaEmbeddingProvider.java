package com.ragsync.ingest.embed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Embeddings from a local Ollama server.
 *
 * Default provider. Local means free, private, offline, and — the part that
 * matters most for a project still tuning its chunker — a re-embed of the whole
 * corpus costs minutes rather than money and rate limits.
 *
 * It also pins the model. A local model file never changes; a hosted model can
 * be updated behind the same name, silently producing vectors that no longer
 * match the ones already in the store.
 *
 * Ollama's /api/embed takes an array and returns one vector per input. Older
 * builds only had /api/embeddings, which is single-text; if that is what you
 * have, upgrade rather than looping, because looping is the nine-minutes-of-
 * waiting problem this class exists to avoid.
 */
@Component
@ConditionalOnProperty(name = "app.embedding.provider", havingValue = "ollama",
        matchIfMissing = true)
public class OllamaEmbeddingProvider implements EmbeddingProvider {

    private static final Logger log = LoggerFactory.getLogger(OllamaEmbeddingProvider.class);

    private final RestClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String model;
    private final int dimensions;
    private final int batchSize;
    private final int maxAttempts;
    private final EmbeddingMetrics metrics;

    public OllamaEmbeddingProvider(
            @Value("${app.embedding.ollama.base-url:http://localhost:11434}") String baseUrl,
            @Value("${app.embedding.ollama.model:nomic-embed-text}") String model,
            @Value("${app.embedding.dimensions:768}") int dimensions,
            @Value("${app.embedding.batch-size:16}") int batchSize,
            @Value("${app.embedding.max-attempts:4}") int maxAttempts,
            EmbeddingMetrics metrics) {

        this.http = RestClient.builder()
                .baseUrl(baseUrl.replaceAll("/+$", ""))
                .defaultHeader("Content-Type", "application/json")
                .build();
        this.model = model;
        this.dimensions = dimensions;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.metrics = metrics;
        log.info("embeddings: ollama {} ({} dims), batch {}", model, dimensions, batchSize);
    }

    @Override
    public String modelId() {
        return model;
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public List<float[]> embed(List<String> texts) {
        List<float[]> out = new ArrayList<>(texts.size());
        for (int start = 0; start < texts.size(); start += batchSize) {
            int end = Math.min(start + batchSize, texts.size());
            out.addAll(embedBatch(texts.subList(start, end)));
        }
        return out;
    }

    private List<float[]> embedBatch(List<String> batch) {
        long started = System.nanoTime();
        String response = callWithRetry(Map.of("model", model, "input", batch));
        metrics.recordCall(batch.size(), (System.nanoTime() - started) / 1_000_000);

        try {
            JsonNode embeddings = mapper.readTree(response).path("embeddings");
            if (!embeddings.isArray() || embeddings.size() != batch.size()) {
                throw new EmbeddingException("expected " + batch.size()
                        + " embeddings, got " + embeddings.size());
            }

            List<float[]> vectors = new ArrayList<>(batch.size());
            for (JsonNode row : embeddings) {
                float[] vector = new float[row.size()];
                for (int i = 0; i < row.size(); i++) {
                    vector[i] = (float) row.get(i).asDouble();
                }
                if (vector.length != dimensions) {
                    throw new EmbeddingException("model returned " + vector.length
                            + " dimensions, configuration says " + dimensions
                            + " — the store's column width will not match");
                }
                vectors.add(vector);
            }
            return vectors;
        } catch (EmbeddingException e) {
            throw e;
        } catch (Exception e) {
            throw new EmbeddingException("could not parse ollama response", e);
        }
    }

    private String callWithRetry(Map<String, Object> body) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return http.post().uri("/api/embed").body(body)
                        .retrieve().body(String.class);
            } catch (RuntimeException e) {
                last = e;
                if (attempt == maxAttempts) {
                    break;
                }
                metrics.recordRetry();
                long waitMillis = (long) Math.pow(2, attempt - 1) * 500L;
                log.warn("embed attempt {}/{} failed, retrying in {}ms: {}",
                        attempt, maxAttempts, waitMillis, e.getMessage());
                try {
                    Thread.sleep(waitMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new EmbeddingException("interrupted during backoff", interrupted);
                }
            }
        }
        metrics.recordFailure();
        throw new EmbeddingException("embedding failed after " + maxAttempts + " attempts", last);
    }
}
