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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Embeddings from Microsoft Foundry (Azure OpenAI).
 *
 * Not the default. It exists so the provider choice is demonstrably reversible
 * — flip app.embedding.provider to "azure" and nothing else in the pipeline
 * moves.
 *
 * Two differences from the local provider worth knowing:
 *
 *   - backoff matters more. A free or low-quota deployment WILL return 429, and
 *     a client that does not retry turns a rate limit into a load that fails
 *     halfway through the corpus.
 *
 *   - the API returns an "index" per result precisely because ordering is not
 *     guaranteed, so results are placed by index rather than appended.
 *
 * Switching providers does NOT migrate existing vectors. Different models
 * produce different spaces, and different dimension counts need a different
 * pgvector column width. That is week 6's job.
 */
@Component
@ConditionalOnProperty(name = "app.embedding.provider", havingValue = "azure")
public class AzureEmbeddingProvider implements EmbeddingProvider {

    private static final Logger log = LoggerFactory.getLogger(AzureEmbeddingProvider.class);

    private final RestClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String path;
    private final String model;
    private final int dimensions;
    private final int batchSize;
    private final int maxAttempts;
    private final EmbeddingMetrics metrics;

    public AzureEmbeddingProvider(
            @Value("${app.embedding.azure.endpoint:}") String endpoint,
            @Value("${app.embedding.azure.api-key:}") String apiKey,
            @Value("${app.embedding.azure.deployment:text-embedding-3-small}") String deployment,
            @Value("${app.embedding.azure.api-version:2024-02-01}") String apiVersion,
            @Value("${app.embedding.dimensions:512}") int dimensions,
            @Value("${app.embedding.batch-size:16}") int batchSize,
            @Value("${app.embedding.max-attempts:5}") int maxAttempts,
            EmbeddingMetrics metrics) {

        this.http = RestClient.builder()
                .baseUrl(endpoint.replaceAll("/+$", ""))
                .defaultHeader("api-key", apiKey)
                .defaultHeader("Content-Type", "application/json")
                .build();
        this.path = "/openai/deployments/" + deployment + "/embeddings?api-version=" + apiVersion;
        this.model = deployment;
        this.dimensions = dimensions;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.metrics = metrics;
        log.info("embeddings: azure {} ({} dims), batch {}", deployment, dimensions, batchSize);
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
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("input", batch);
        body.put("dimensions", dimensions);

        long started = System.nanoTime();
        String response = callWithRetry(body);
        metrics.recordCall(batch.size(), (System.nanoTime() - started) / 1_000_000);

        try {
            JsonNode data = mapper.readTree(response).path("data");
            float[][] byIndex = new float[batch.size()][];

            for (JsonNode item : data) {
                int index = item.path("index").asInt();
                JsonNode values = item.path("embedding");
                float[] vector = new float[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    vector[i] = (float) values.get(i).asDouble();
                }
                byIndex[index] = vector;
            }

            List<float[]> vectors = new ArrayList<>(batch.size());
            for (int i = 0; i < batch.size(); i++) {
                if (byIndex[i] == null) {
                    throw new EmbeddingException("no embedding returned for input " + i);
                }
                vectors.add(byIndex[i]);
            }
            return vectors;
        } catch (EmbeddingException e) {
            throw e;
        } catch (Exception e) {
            throw new EmbeddingException("could not parse azure response", e);
        }
    }

    private String callWithRetry(Map<String, Object> body) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return http.post().uri(path).body(body).retrieve().body(String.class);
            } catch (RuntimeException e) {
                last = e;
                if (attempt == maxAttempts) {
                    break;
                }
                metrics.recordRetry();
                // 1s, 2s, 4s, 8s — enough for a per-minute token quota to refill
                long waitMillis = (long) Math.pow(2, attempt - 1) * 1000L;
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
