package com.ragsync.ingest.chunk;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * SHA-256 of a chunk's exact text.
 *
 * This hash IS the chunk's identity. Two chunks with the same text have the
 * same hash regardless of which document they came from or where in the
 * document they sit, which is what lets an edit to paragraph nine leave the
 * other eleven chunks untouched.
 *
 * SHA-256 rather than String.hashCode(): 32 bits collides in practice at this
 * volume, and a collision here means silently skipping a chunk that actually
 * changed — a wrong answer with no error attached to it.
 */
public final class ChunkHasher {

    private ChunkHasher() {
    }

    public static String sha256(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required by the JLS to be present on every JVM
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
