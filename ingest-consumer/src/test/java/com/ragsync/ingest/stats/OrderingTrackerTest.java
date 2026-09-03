package com.ragsync.ingest.stats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderingTrackerTest {

    @Test
    void acceptsIncreasingSequencePerDocument() {
        OrderingTracker tracker = new OrderingTracker();

        assertTrue(tracker.observe("docs/a.md", 1));
        assertTrue(tracker.observe("docs/a.md", 2));
        assertTrue(tracker.observe("docs/a.md", 3));

        assertEquals(0, tracker.violations());
    }

    @Test
    void documentsAreTrackedIndependently() {
        OrderingTracker tracker = new OrderingTracker();

        tracker.observe("docs/a.md", 1);
        tracker.observe("docs/b.md", 1);
        tracker.observe("docs/a.md", 2);
        tracker.observe("docs/b.md", 2);

        assertEquals(0, tracker.violations());
        assertEquals(2, tracker.trackedDocuments());
    }

    @Test
    void flagsOutOfOrderEvent() {
        OrderingTracker tracker = new OrderingTracker();

        tracker.observe("docs/a.md", 1);
        tracker.observe("docs/a.md", 5);
        assertFalse(tracker.observe("docs/a.md", 4));

        assertEquals(1, tracker.violations());
    }

    @Test
    void flagsDuplicateSequence() {
        OrderingTracker tracker = new OrderingTracker();

        tracker.observe("docs/a.md", 7);
        assertFalse(tracker.observe("docs/a.md", 7));

        assertEquals(1, tracker.violations());
    }
}
