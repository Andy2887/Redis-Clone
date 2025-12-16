import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import StorageManager.StreamStorage;
import StorageManager.StreamEntry;
import StorageManager.StreamIdHelper;
import StorageManager.BlockedClient;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for StreamStorage class.
 * Tests stream operations including XADD, XRANGE, XREAD, and blocking operations.
 */
@DisplayName("StreamStorage Tests")
class StreamStorageTest {

    private StreamStorage storage;

    @BeforeEach
    void setUp() {
        storage = new StreamStorage();
    }

    // ========== Basic XADD Tests ==========

    @Test
    @DisplayName("XADD adds entry to new stream")
    void testXaddNewStream() {
        Map<String, String> fields = new HashMap<>();
        fields.put("temperature", "36");
        fields.put("humidity", "95");

        String entryId = storage.addEntry("stream1", "1000-0", fields);

        assertEquals("1000-0", entryId);
        assertTrue(storage.exists("stream1"));
        assertEquals(1, storage.length("stream1"));
    }

    @Test
    @DisplayName("XADD adds multiple entries to stream")
    void testXaddMultipleEntries() {
        Map<String, String> fields1 = new HashMap<>();
        fields1.put("field1", "value1");
        
        Map<String, String> fields2 = new HashMap<>();
        fields2.put("field2", "value2");

        storage.addEntry("stream1", "1000-0", fields1);
        storage.addEntry("stream1", "1000-1", fields2);

        assertEquals(2, storage.length("stream1"));
    }

    @Test
    @DisplayName("XADD with auto-generated sequence number")
    void testXaddAutoSequence() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        String entryId1 = storage.addEntry("stream1", "1000-*", fields);
        String entryId2 = storage.addEntry("stream1", "1000-*", fields);

        assertEquals("1000-0", entryId1);
        assertEquals("1000-1", entryId2);
    }

    @Test
    @DisplayName("XADD with fully auto-generated ID")
    void testXaddFullyAutoId() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        String entryId = storage.addEntry("stream1", "*", fields);

        assertNotNull(entryId);
        assertTrue(entryId.contains("-"));
        String[] parts = entryId.split("-");
        assertEquals(2, parts.length);
        assertTrue(Long.parseLong(parts[0]) > 0);
    }

    @Test
    @DisplayName("XADD rejects ID 0-0")
    void testXaddRejectsZeroZeroId() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        assertThrows(IllegalArgumentException.class, () -> {
            storage.addEntry("stream1", "0-0", fields);
        });
    }

    @Test
    @DisplayName("XADD rejects ID smaller than existing")
    void testXaddRejectsSmallerOrEqualId() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-5", fields);

        // Same ID should be rejected
        assertThrows(IllegalArgumentException.class, () -> {
            storage.addEntry("stream1", "1000-5", fields);
        });

        // Smaller ID should be rejected
        assertThrows(IllegalArgumentException.class, () -> {
            storage.addEntry("stream1", "1000-4", fields);
        });

        // Smaller milliseconds should be rejected
        assertThrows(IllegalArgumentException.class, () -> {
            storage.addEntry("stream1", "999-9", fields);
        });
    }

    @Test
    @DisplayName("XADD stores field-value pairs correctly")
    void testXaddStoresFields() {
        Map<String, String> fields = new HashMap<>();
        fields.put("temperature", "36");
        fields.put("humidity", "95");
        fields.put("location", "office");

        storage.addEntry("stream1", "1000-0", fields);

        List<StreamEntry> entries = storage.getAllEntries("stream1");
        assertEquals(1, entries.size());
        
        StreamEntry entry = entries.get(0);
        assertEquals("1000-0", entry.id);
        assertEquals("36", entry.fields.get("temperature"));
        assertEquals("95", entry.fields.get("humidity"));
        assertEquals("office", entry.fields.get("location"));
    }

    // ========== XRANGE Tests ==========

    @Test
    @DisplayName("XRANGE returns all entries with - +")
    void testXrangeAllEntries() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream1", "1000-1", fields);
        storage.addEntry("stream1", "2000-0", fields);

        List<StreamEntry> result = storage.getRange("stream1", "-", "+");

        assertEquals(3, result.size());
        assertEquals("1000-0", result.get(0).id);
        assertEquals("1000-1", result.get(1).id);
        assertEquals("2000-0", result.get(2).id);
    }

    @Test
    @DisplayName("XRANGE returns entries in range")
    void testXrangeWithinRange() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream1", "1500-0", fields);
        storage.addEntry("stream1", "2000-0", fields);
        storage.addEntry("stream1", "2500-0", fields);

        List<StreamEntry> result = storage.getRange("stream1", "1500-0", "2000-0");

        assertEquals(2, result.size());
        assertEquals("1500-0", result.get(0).id);
        assertEquals("2000-0", result.get(1).id);
    }

    @Test
    @DisplayName("XRANGE on non-existent stream returns empty list")
    void testXrangeNonExistentStream() {
        List<StreamEntry> result = storage.getRange("nonexistent", "-", "+");

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("XRANGE with start after all entries returns empty list")
    void testXrangeNoMatchingEntries() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream1", "2000-0", fields);

        List<StreamEntry> result = storage.getRange("stream1", "3000-0", "+");

        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("XRANGE includes boundary entries")
    void testXrangeIncludesBoundaries() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream1", "1000-1", fields);
        storage.addEntry("stream1", "1000-2", fields);

        List<StreamEntry> result = storage.getRange("stream1", "1000-0", "1000-2");

        assertEquals(3, result.size());
    }

    // ========== XREAD Tests (getEntriesAfter) ==========

    @Test
    @DisplayName("XREAD returns entries after specified ID")
    void testXreadEntriesAfter() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream1", "1000-1", fields);
        storage.addEntry("stream1", "2000-0", fields);

        List<StreamEntry> result = storage.getEntriesAfter("stream1", "1000-0");

        assertEquals(2, result.size());
        assertEquals("1000-1", result.get(0).id);
        assertEquals("2000-0", result.get(1).id);
    }

    @Test
    @DisplayName("XREAD from start returns all entries")
    void testXreadFromStart() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream1", "2000-0", fields);

        List<StreamEntry> result = storage.getEntriesAfter("stream1", "0-0");

        assertEquals(2, result.size());
    }

    @Test
    @DisplayName("XREAD on non-existent stream returns empty list")
    void testXreadNonExistentStream() {
        List<StreamEntry> result = storage.getEntriesAfter("nonexistent", "0-0");

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("XREAD with ID after all entries returns empty list")
    void testXreadNoNewEntries() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream1", "2000-0", fields);

        List<StreamEntry> result = storage.getEntriesAfter("stream1", "2000-0");

        assertTrue(result.isEmpty());
    }

    // ========== Stream Utility Tests ==========

    @Test
    @DisplayName("exists returns true for stream with entries")
    void testExistsWithEntries() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);

        assertTrue(storage.exists("stream1"));
    }

    @Test
    @DisplayName("exists returns false for non-existent stream")
    void testExistsNonExistent() {
        assertFalse(storage.exists("nonexistent"));
    }

    @Test
    @DisplayName("length returns correct count")
    void testLength() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        assertEquals(0, storage.length("stream1"));

        storage.addEntry("stream1", "1000-0", fields);
        assertEquals(1, storage.length("stream1"));

        storage.addEntry("stream1", "2000-0", fields);
        assertEquals(2, storage.length("stream1"));
    }

    @Test
    @DisplayName("getLastEntryId returns correct ID")
    void testGetLastEntryId() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        assertNull(storage.getLastEntryId("stream1"));

        storage.addEntry("stream1", "1000-0", fields);
        assertEquals("1000-0", storage.getLastEntryId("stream1"));

        storage.addEntry("stream1", "2000-5", fields);
        assertEquals("2000-5", storage.getLastEntryId("stream1"));
    }

    @Test
    @DisplayName("getFirstEntryId returns correct ID")
    void testGetFirstEntryId() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        assertNull(storage.getFirstEntryId("stream1"));

        storage.addEntry("stream1", "1000-0", fields);
        assertEquals("1000-0", storage.getFirstEntryId("stream1"));

        storage.addEntry("stream1", "2000-0", fields);
        assertEquals("1000-0", storage.getFirstEntryId("stream1"));
    }

    @Test
    @DisplayName("removeStream removes the stream")
    void testRemoveStream() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        assertTrue(storage.exists("stream1"));

        boolean removed = storage.removeStream("stream1");
        assertTrue(removed);
        assertFalse(storage.exists("stream1"));
    }

    @Test
    @DisplayName("clear removes all streams")
    void testClear() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream2", "1000-0", fields);

        storage.clear();

        assertEquals(0, storage.getStreamCount());
        assertFalse(storage.exists("stream1"));
        assertFalse(storage.exists("stream2"));
    }

    @Test
    @DisplayName("getAllStreamKeys returns all keys")
    void testGetAllStreamKeys() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream2", "1000-0", fields);
        storage.addEntry("stream3", "1000-0", fields);

        List<String> keys = storage.getAllStreamKeys();

        assertEquals(3, keys.size());
        assertTrue(keys.contains("stream1"));
        assertTrue(keys.contains("stream2"));
        assertTrue(keys.contains("stream3"));
    }

    // ========== StreamIdHelper Tests ==========

    @Test
    @DisplayName("parseStreamId parses valid ID correctly")
    void testParseStreamIdValid() {
        StreamIdHelper.ParsedStreamId parsed = StreamIdHelper.parseStreamId("1526919030474-55");

        assertNotNull(parsed);
        assertEquals(1526919030474L, parsed.milliseconds);
        assertEquals(55L, parsed.sequence);
    }

    @Test
    @DisplayName("parseStreamId returns null for invalid format")
    void testParseStreamIdInvalid() {
        assertNull(StreamIdHelper.parseStreamId("invalid"));
        assertNull(StreamIdHelper.parseStreamId("1000"));
        assertNull(StreamIdHelper.parseStreamId("1000-abc"));
        assertNull(StreamIdHelper.parseStreamId(""));
        assertNull(StreamIdHelper.parseStreamId(null));
    }

    @Test
    @DisplayName("compareStreamIds compares correctly")
    void testCompareStreamIds() {
        assertTrue(StreamIdHelper.compareStreamIds("1000-0", "2000-0") < 0);
        assertTrue(StreamIdHelper.compareStreamIds("2000-0", "1000-0") > 0);
        assertTrue(StreamIdHelper.compareStreamIds("1000-5", "1000-10") < 0);
        assertEquals(0, StreamIdHelper.compareStreamIds("1000-5", "1000-5"));
    }

    @Test
    @DisplayName("isEntryInRange checks range correctly")
    void testIsEntryInRange() {
        assertTrue(StreamIdHelper.isEntryInRange("1500-0", "1000-0", "2000-0"));
        assertTrue(StreamIdHelper.isEntryInRange("1000-0", "1000-0", "2000-0")); // Start boundary
        assertTrue(StreamIdHelper.isEntryInRange("2000-0", "1000-0", "2000-0")); // End boundary
        assertFalse(StreamIdHelper.isEntryInRange("500-0", "1000-0", "2000-0"));
        assertFalse(StreamIdHelper.isEntryInRange("2500-0", "1000-0", "2000-0"));
    }

    @Test
    @DisplayName("isEntryInRange handles special markers")
    void testIsEntryInRangeSpecialMarkers() {
        assertTrue(StreamIdHelper.isEntryInRange("1000-0", "-", "+"));
        assertTrue(StreamIdHelper.isEntryInRange("0-1", "-", "+"));
        assertTrue(StreamIdHelper.isEntryInRange("1000-0", "-", "2000-0"));
        assertTrue(StreamIdHelper.isEntryInRange("1000-0", "0-0", "+"));
    }

    @Test
    @DisplayName("normalizeStreamId adds default sequence")
    void testNormalizeStreamId() {
        assertEquals("1000-0", StreamIdHelper.normalizeStreamId("1000"));
        assertEquals("1000-5", StreamIdHelper.normalizeStreamId("1000-5"));
        assertEquals("0-0", StreamIdHelper.normalizeStreamId(""));
        assertEquals("0-0", StreamIdHelper.normalizeStreamId(null));
    }

    // ========== Concurrency Tests ==========

    @Test
    @DisplayName("Concurrent XADD operations are thread-safe")
    void testConcurrentXadd() throws InterruptedException {
        int threadCount = 10;
        int entriesPerThread = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < entriesPerThread; i++) {
                        Map<String, String> fields = new HashMap<>();
                        fields.put("thread", String.valueOf(threadId));
                        fields.put("entry", String.valueOf(i));

                        // Use auto-generated ID to avoid conflicts
                        storage.addEntry("stream1", "*", fields);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(threadCount * entriesPerThread, storage.length("stream1"));
    }

    @Test
    @DisplayName("Concurrent reads and writes are thread-safe")
    void testConcurrentReadsAndWrites() throws InterruptedException {
        // Pre-populate stream
        for (int i = 0; i < 100; i++) {
            Map<String, String> fields = new HashMap<>();
            fields.put("field", "value" + i);
            storage.addEntry("stream1", (1000 + i) + "-0", fields);
        }

        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < 50; i++) {
                        if (threadId % 2 == 0) {
                            // Reader thread
                            storage.getRange("stream1", "-", "+");
                            storage.getEntriesAfter("stream1", "1050-0");
                        } else {
                            // Writer thread
                            Map<String, String> fields = new HashMap<>();
                            fields.put("thread", String.valueOf(threadId));
                            storage.addEntry("stream1", "*", fields);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // Should have initial 100 + (5 writer threads * 50 entries) = 350 entries
        assertEquals(350, storage.length("stream1"));
    }

    // ========== Edge Cases ==========

    @Test
    @DisplayName("XADD with empty fields map")
    void testXaddEmptyFields() {
        Map<String, String> fields = new HashMap<>();

        String entryId = storage.addEntry("stream1", "1000-0", fields);

        assertEquals("1000-0", entryId);
        List<StreamEntry> entries = storage.getAllEntries("stream1");
        assertTrue(entries.get(0).fields.isEmpty());
    }

    @Test
    @DisplayName("XADD with large field values")
    void testXaddLargeFields() {
        Map<String, String> fields = new HashMap<>();
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeValue.append("x");
        }
        fields.put("largeField", largeValue.toString());

        String entryId = storage.addEntry("stream1", "1000-0", fields);

        assertEquals("1000-0", entryId);
        List<StreamEntry> entries = storage.getAllEntries("stream1");
        assertEquals(10000, entries.get(0).fields.get("largeField").length());
    }

    @Test
    @DisplayName("Multiple independent streams")
    void testMultipleStreams() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream1", "1000-0", fields);
        storage.addEntry("stream2", "2000-0", fields);
        storage.addEntry("stream3", "3000-0", fields);

        assertEquals(1, storage.length("stream1"));
        assertEquals(1, storage.length("stream2"));
        assertEquals(1, storage.length("stream3"));
        assertEquals(3, storage.getStreamCount());
    }

    @Test
    @DisplayName("Stream with special characters in key")
    void testSpecialCharactersInKey() {
        Map<String, String> fields = new HashMap<>();
        fields.put("field", "value");

        storage.addEntry("stream:with:colons", "1000-0", fields);
        storage.addEntry("stream.with.dots", "1000-0", fields);
        storage.addEntry("stream-with-dashes", "1000-0", fields);

        assertTrue(storage.exists("stream:with:colons"));
        assertTrue(storage.exists("stream.with.dots"));
        assertTrue(storage.exists("stream-with-dashes"));
    }
}
