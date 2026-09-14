package com.ragsync.ingest.store;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The serialization helpers, which are the only part of the store that can be
 * tested without a database. The SQL itself needs a real Postgres.
 */
class PgVectorStoreTest {

    @Test
    void vectorRoundTripsThroughItsTextLiteral() {
        float[] original = {0.5f, -0.25f, 0.125f};
        String literal = PgVectorStore.toVectorLiteral(original);

        assertEquals("[0.5,-0.25,0.125]", literal);
        assertArrayEquals(original, PgVectorStore.parseVector(literal), 1e-6f);
    }

    @Test
    void emptyVectorIsHandled() {
        assertEquals("[]", PgVectorStore.toVectorLiteral(new float[0]));
        assertEquals(0, PgVectorStore.parseVector("[]").length);
    }

    @Test
    void largeVectorRoundTrips() {
        float[] original = new float[768];
        for (int i = 0; i < original.length; i++) {
            original[i] = (float) Math.sin(i) * 0.1f;
        }
        assertArrayEquals(original,
                PgVectorStore.parseVector(PgVectorStore.toVectorLiteral(original)), 1e-5f);
    }

    @Test
    void linksSerializeInInsertionOrder() {
        Map<String, String> links = new LinkedHashMap<>();
        links.put("\u00A7link0\u00A7", "https://a.example");
        links.put("\u00A7link1\u00A7", "https://b.example");

        assertEquals("\u00A7link0\u00A7=https://a.example\n\u00A7link1\u00A7=https://b.example",
                PgVectorStore.serializeLinks(links));
    }

    @Test
    void noLinksSerializesToEmptyString() {
        assertEquals("", PgVectorStore.serializeLinks(Map.of()));
        assertEquals("", PgVectorStore.serializeLinks(null));
    }
}
