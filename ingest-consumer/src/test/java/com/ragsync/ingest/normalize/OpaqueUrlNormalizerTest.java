package com.ragsync.ingest.normalize;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpaqueUrlNormalizerTest {

    private final OpaqueUrlNormalizer normalizer = new OpaqueUrlNormalizer();

    private static String base64(int length) {
        StringBuilder builder = new StringBuilder();
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (int i = 0; i < length; i++) {
            builder.append(alphabet.charAt(i % alphabet.length()));
        }
        return builder.toString();
    }

    @Test
    void ordinaryUrlsAreUntouched() {
        String text = "See [the guide](https://vuejs.org/guide/essentials/computed.html) for details.";
        NormalizedContent result = normalizer.normalize(text);
        assertEquals(text, result.text());
        assertTrue(result.links().isEmpty());
    }

    @Test
    void shortFragmentsAreUntouched() {
        String text = "Jump to https://example.com/docs#installation now.";
        assertEquals(text, normalizer.normalize(text).text());
    }

    @Test
    void largeEncodedFragmentIsReplaced() {
        String url = "https://sfc.vuejs.org/#" + base64(1200);
        NormalizedContent result = normalizer.normalize("Try it: " + url);

        assertFalse(result.text().contains(base64(1200)));
        assertEquals(1, result.links().size());
        assertTrue(result.links().containsValue(url));
    }

    @Test
    void originalUrlRemainsRecoverable() {
        String url = "https://play.vuejs.org/#" + base64(900);
        NormalizedContent result = normalizer.normalize("Demo: " + url);

        String restored = NormalizedContent.restore(result.text(), result.links());
        assertEquals("Demo: " + url, restored);
    }

    /**
     * Base64 in prose is not a URL, so the "is it a URL" precondition rejects
     * it before length or alphabet are considered.
     */
    @Test
    void standaloneBase64InProseIsUntouched() {
        String text = "Encode it first: " + base64(2000);
        assertEquals(text, normalizer.normalize(text).text());
    }

    @Test
    void jwtLikeStringIsUntouched() {
        String text = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9."
                + base64(300) + "." + base64(200);
        assertEquals(text, normalizer.normalize(text).text());
    }

    /**
     * Long does not mean opaque. A readable query string stays, because its
     * character distribution includes separators and words.
     */
    @Test
    void longButReadableQueryStringIsUntouched() {
        StringBuilder query = new StringBuilder("https://example.com/search?");
        for (int i = 0; i < 40; i++) {
            query.append("filter").append(i).append("=some readable value&");
        }
        String text = query.toString();
        assertEquals(text, normalizer.normalize(text).text());
    }

    @Test
    void normalizationIsDeterministic() {
        String text = "A " + "https://a.example/#" + base64(400)
                + " and B " + "https://b.example/#" + base64(400);
        NormalizedContent first = normalizer.normalize(text);
        NormalizedContent second = normalizer.normalize(text);

        assertEquals(first.text(), second.text());
        assertEquals(first.serializeLinks(), second.serializeLinks());
    }

    @Test
    void placeholdersAreNumberedInDocumentOrder() {
        String first = "https://a.example/#" + base64(400);
        String second = "https://b.example/#" + base64(400);
        NormalizedContent result = normalizer.normalize(first + "\n\n" + second);

        assertEquals(2, result.links().size());
        assertEquals(first, result.links().get("\u00A7link0\u00A7"));
        assertEquals(second, result.links().get("\u00A7link1\u00A7"));
    }

    @Test
    void markdownLinkKeepsItsLabel() {
        String url = "https://sfc.vuejs.org/#" + base64(1500);
        NormalizedContent result = normalizer.normalize("[Full Example](" + url + ")");
        assertTrue(result.text().startsWith("[Full Example]("));
        assertTrue(result.text().endsWith(")"));
    }

    @Test
    void emptyAndNullInputAreSafe() {
        assertEquals("", normalizer.normalize("").text());
        assertEquals("", normalizer.normalize(null).text());
    }
}
