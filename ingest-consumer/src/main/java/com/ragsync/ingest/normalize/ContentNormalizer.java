package com.ragsync.ingest.normalize;

public interface ContentNormalizer {

    NormalizedContent normalize(String text);
}
