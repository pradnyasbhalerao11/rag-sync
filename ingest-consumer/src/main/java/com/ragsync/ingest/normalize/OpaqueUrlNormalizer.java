package com.ragsync.ingest.normalize;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Replaces URLs whose fragment or query is a large encoded payload.
 *
 * Why this exists
 * ---------------
 * Documentation routinely embeds generated links — playground permalinks, data
 * URIs, encoded examples. One such URL in the corpus is 2,655 characters, all
 * of it base64. The chunker has nothing to split it on, so it hard-cuts at the
 * size cap and emits fragments like "XG4gICAgPlxuICAgICAge3sgaXRlbS5tc2c".
 *
 * Those fragments embed without error and cost real money, but the vectors are
 * meaningless — an embedding places text in a space organized by meaning, and
 * base64 has none. Worse, because they are all similarly meaningless they
 * cluster, so an unrelated query can land nearest to that cluster and feed
 * base64 to an LLM as context.
 *
 * Detection is deliberately conservative
 * --------------------------------------
 * "Looks like base64" is one signal, not proof. Legitimate documentation
 * contains base64 on purpose (encoding examples), and JWTs, hashes, object ids
 * and version strings all look encoded. Three conditions must ALL hold:
 *
 *   1. it is a URL
 *   2. its fragment or query is at least fragmentThreshold characters
 *   3. that payload is almost entirely encoding-alphabet characters with no
 *      spaces
 *
 * A base64 string sitting in a paragraph is untouched — it is not a URL. A JWT
 * in a code sample is untouched. A normal long documentation link is untouched,
 * because its path is readable text, not an encoded payload.
 *
 * Nothing is deleted
 * ------------------
 * The original URL is kept in the returned link map and travels with the chunk
 * into the index, so a retrieved chunk can still present a working link. Only
 * the embedding representation changes.
 */
@Component
public class OpaqueUrlNormalizer implements ContentNormalizer {

    // stops at whitespace or a closing paren, so markdown links work
    private static final Pattern URL = Pattern.compile("https?://[^\\s)\\]]+");
    private static final String ENCODING_ALPHABET_EXTRAS = "+/=-_.~%";

    private final int fragmentThreshold;
    private final double encodedRatio;

    public OpaqueUrlNormalizer() {
        this(200, 0.95);
    }

    public OpaqueUrlNormalizer(
            @Value("${app.normalize.opaque-fragment-threshold:200}") int fragmentThreshold,
            @Value("${app.normalize.encoded-ratio:0.95}") double encodedRatio) {
        this.fragmentThreshold = fragmentThreshold;
        this.encodedRatio = encodedRatio;
    }

    @Override
    public NormalizedContent normalize(String text) {
        if (text == null || text.isEmpty()) {
            return NormalizedContent.unchanged(text == null ? "" : text);
        }

        // LinkedHashMap: placeholder numbering must be stable, because it
        // feeds contentHash
        Map<String, String> links = new LinkedHashMap<>();
        Matcher matcher = URL.matcher(text);
        StringBuilder out = new StringBuilder();

        while (matcher.find()) {
            String url = matcher.group();
            if (isOpaque(url)) {
                String placeholder = "\u00A7link" + links.size() + "\u00A7";
                links.put(placeholder, url);
                matcher.appendReplacement(out, Matcher.quoteReplacement(placeholder));
            }
        }
        matcher.appendTail(out);

        return new NormalizedContent(out.toString(), links);
    }

    boolean isOpaque(String url) {
        String payload = fragmentOrQuery(url);
        return payload != null
                && payload.length() >= fragmentThreshold
                && looksEncoded(payload);
    }

    private String fragmentOrQuery(String url) {
        int hash = url.indexOf('#');
        if (hash >= 0 && hash < url.length() - 1) {
            return url.substring(hash + 1);
        }
        int question = url.indexOf('?');
        if (question >= 0 && question < url.length() - 1) {
            return url.substring(question + 1);
        }
        return null;
    }

    private boolean looksEncoded(String payload) {
        if (payload.indexOf(' ') >= 0) {
            return false;
        }
        long encodingChars = payload.chars()
                .filter(c -> Character.isLetterOrDigit(c)
                        || ENCODING_ALPHABET_EXTRAS.indexOf(c) >= 0)
                .count();
        return (double) encodingChars / payload.length() >= encodedRatio;
    }
}
