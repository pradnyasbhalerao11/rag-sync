package com.ragsync.ingest.web;

import com.ragsync.ingest.stats.ReplayStats;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsController {

    private final ReplayStats stats;

    public StatsController(ReplayStats stats) {
        this.stats = stats;
    }

    @GetMapping("/stats")
    public ReplayStats.Snapshot stats() {
        return stats.snapshot();
    }

    @PostMapping("/stats/reset")
    public ResponseEntity<Void> reset() {
        stats.reset();
        return ResponseEntity.noContent().build();
    }
}
