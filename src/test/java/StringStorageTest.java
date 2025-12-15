import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import StorageManager.StringStorage;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for StringStorage class.
 * Tests basic operations, expiry management, and thread safety.
 */
@DisplayName("StringStorage Tests")
class StringStorageTest {

    private StringStorage storage;

    @BeforeEach
    void setUp() {
        storage = new StringStorage();
    }

    // ========== Basic SET/GET Tests ==========

    @Test
    @DisplayName("SET and GET basic operation")
    void testBasicSetAndGet() {
        storage.set("key1", "value1", null);
        assertEquals("value1", storage.get("key1"));
    }

    @Test
    @DisplayName("GET non-existent key returns null")
    void testGetNonExistentKey() {
        assertNull(storage.get("nonexistent"));
    }

    @Test
    @DisplayName("SET overwrites existing value")
    void testSetOverwrite() {
        storage.set("key1", "value1", null);
        storage.set("key1", "value2", null);
        assertEquals("value2", storage.get("key1"));
    }

    @Test
    @DisplayName("SET multiple keys")
    void testSetMultipleKeys() {
        storage.set("key1", "value1", null);
        storage.set("key2", "value2", null);
        storage.set("key3", "value3", null);

        assertEquals("value1", storage.get("key1"));
        assertEquals("value2", storage.get("key2"));
        assertEquals("value3", storage.get("key3"));
    }

    // ========== Expiry Tests ==========

    @Test
    @DisplayName("SET with expiry - key expires correctly")
    void testSetWithExpiry() throws InterruptedException {
        long expiryTime = System.currentTimeMillis() + 100; // 100ms from now
        storage.set("key1", "value1", expiryTime);

        // Key should exist immediately
        assertEquals("value1", storage.get("key1"));

        // Wait for expiry
        Thread.sleep(150);

        // Key should be expired and return null
        assertNull(storage.get("key1"));
    }

    @Test
    @DisplayName("SET with expiry - key accessible before expiry")
    void testKeyAccessibleBeforeExpiry() throws InterruptedException {
        long expiryTime = System.currentTimeMillis() + 200; // 200ms from now
        storage.set("key1", "value1", expiryTime);

        // Wait a bit but not past expiry
        Thread.sleep(50);

        // Key should still be accessible
        assertEquals("value1", storage.get("key1"));
    }

    @Test
    @DisplayName("SET removes expiry when set to null")
    void testSetRemovesExpiry() throws InterruptedException {
        long expiryTime = System.currentTimeMillis() + 100; // 100ms from now
        storage.set("key1", "value1", expiryTime);

        // Update the key without expiry
        storage.set("key1", "value2", null);

        // Wait past original expiry time
        Thread.sleep(150);

        // Key should still exist (no expiry)
        assertEquals("value2", storage.get("key1"));
    }

    @Test
    @DisplayName("SET updates expiry time")
    void testSetUpdatesExpiry() throws InterruptedException {
        long expiryTime1 = System.currentTimeMillis() + 100; // 100ms from now
        storage.set("key1", "value1", expiryTime1);

        // Update with new expiry time (much longer)
        long expiryTime2 = System.currentTimeMillis() + 500; // 500ms from now
        storage.set("key1", "value2", expiryTime2);

        // Wait past first expiry
        Thread.sleep(150);

        // Key should still exist with new expiry
        assertEquals("value2", storage.get("key1"));
    }

    @Test
    @DisplayName("SET with past expiry time - key immediately expired")
    void testSetWithPastExpiryTime() {
        long pastTime = System.currentTimeMillis() - 1000; // 1 second ago
        storage.set("key1", "value1", pastTime);

        // Key should be immediately expired
        assertNull(storage.get("key1"));
    }

    @Test
    @DisplayName("SET with exact current time expiry")
    void testSetWithExactCurrentTime() throws InterruptedException {
        long currentTime = System.currentTimeMillis();
        storage.set("key1", "value1", currentTime);

        // Small delay to ensure time passes
        Thread.sleep(10);

        // Key should be expired
        assertNull(storage.get("key1"));
    }

    // ========== EXISTS Tests ==========

    @Test
    @DisplayName("EXISTS returns true for existing key")
    void testExistsForExistingKey() {
        storage.set("key1", "value1", null);
        assertTrue(storage.exists("key1"));
    }

    @Test
    @DisplayName("EXISTS returns false for non-existent key")
    void testExistsForNonExistentKey() {
        assertFalse(storage.exists("nonexistent"));
    }

    @Test
    @DisplayName("EXISTS returns false for expired key")
    void testExistsForExpiredKey() throws InterruptedException {
        long expiryTime = System.currentTimeMillis() + 50;
        storage.set("key1", "value1", expiryTime);

        Thread.sleep(100);

        assertFalse(storage.exists("key1"));
    }

    // ========== REMOVE Tests ==========

    @Test
    @DisplayName("REMOVE existing key")
    void testRemoveExistingKey() {
        storage.set("key1", "value1", null);
        assertTrue(storage.remove("key1"));
        assertNull(storage.get("key1"));
    }

    @Test
    @DisplayName("REMOVE non-existent key returns false")
    void testRemoveNonExistentKey() {
        assertFalse(storage.remove("nonexistent"));
    }

    @Test
    @DisplayName("REMOVE also removes expiry")
    void testRemoveAlsoRemovesExpiry() {
        long expiryTime = System.currentTimeMillis() + 10000;
        storage.set("key1", "value1", expiryTime);
        storage.remove("key1");

        assertNull(storage.getExpiryTime("key1"));
    }

    // ========== SIZE Tests ==========

    @Test
    @DisplayName("SIZE returns correct count")
    void testSize() {
        assertEquals(0, storage.size());

        storage.set("key1", "value1", null);
        assertEquals(1, storage.size());

        storage.set("key2", "value2", null);
        assertEquals(2, storage.size());

        storage.remove("key1");
        assertEquals(1, storage.size());
    }

    @Test
    @DisplayName("SIZE excludes expired keys")
    void testSizeExcludesExpiredKeys() throws InterruptedException {
        storage.set("key1", "value1", null);
        storage.set("key2", "value2", System.currentTimeMillis() + 50);

        assertEquals(2, storage.size());

        Thread.sleep(100);

        // key2 should be expired and not counted
        assertEquals(1, storage.size());
    }

    // ========== GET ALL KEYS Tests ==========

    @Test
    @DisplayName("getAllKeys returns all non-expired keys")
    void testGetAllKeys() {
        storage.set("key1", "value1", null);
        storage.set("key2", "value2", null);
        storage.set("key3", "value3", null);

        List<String> keys = storage.getAllKeys();
        assertEquals(3, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
        assertTrue(keys.contains("key3"));
    }

    @Test
    @DisplayName("getAllKeys excludes expired keys")
    void testGetAllKeysExcludesExpired() throws InterruptedException {
        storage.set("key1", "value1", null);
        storage.set("key2", "value2", System.currentTimeMillis() + 50);
        storage.set("key3", "value3", null);

        Thread.sleep(100);

        List<String> keys = storage.getAllKeys();
        assertEquals(2, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key3"));
        assertFalse(keys.contains("key2"));
    }

    @Test
    @DisplayName("getAllKeys returns empty list when no keys")
    void testGetAllKeysEmpty() {
        List<String> keys = storage.getAllKeys();
        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    // ========== EXPIRY MANAGEMENT Tests ==========

    @Test
    @DisplayName("getExpiryTime returns correct expiry")
    void testGetExpiryTime() {
        long expiryTime = System.currentTimeMillis() + 10000;
        storage.set("key1", "value1", expiryTime);

        assertEquals(expiryTime, storage.getExpiryTime("key1"));
    }

    @Test
    @DisplayName("getExpiryTime returns null when no expiry")
    void testGetExpiryTimeNoExpiry() {
        storage.set("key1", "value1", null);
        assertNull(storage.getExpiryTime("key1"));
    }

    @Test
    @DisplayName("setExpiry sets expiry on existing key")
    void testSetExpiry() throws InterruptedException {
        storage.set("key1", "value1", null);

        long expiryTime = System.currentTimeMillis() + 50;
        assertTrue(storage.setExpiry("key1", expiryTime));

        Thread.sleep(100);
        assertNull(storage.get("key1"));
    }

    @Test
    @DisplayName("setExpiry returns false for non-existent key")
    void testSetExpiryNonExistent() {
        long expiryTime = System.currentTimeMillis() + 10000;
        assertFalse(storage.setExpiry("nonexistent", expiryTime));
    }

    @Test
    @DisplayName("removeExpiry makes key persistent")
    void testRemoveExpiry() throws InterruptedException {
        long expiryTime = System.currentTimeMillis() + 50;
        storage.set("key1", "value1", expiryTime);

        assertTrue(storage.removeExpiry("key1"));

        Thread.sleep(100);

        // Key should still exist
        assertEquals("value1", storage.get("key1"));
    }

    @Test
    @DisplayName("removeExpiry returns false for non-existent key")
    void testRemoveExpiryNonExistent() {
        assertFalse(storage.removeExpiry("nonexistent"));
    }

    @Test
    @DisplayName("cleanupExpiredKeys removes all expired keys")
    void testCleanupExpiredKeys() throws InterruptedException {
        storage.set("key1", "value1", null);
        storage.set("key2", "value2", System.currentTimeMillis() + 50);
        storage.set("key3", "value3", System.currentTimeMillis() + 50);
        storage.set("key4", "value4", null);

        Thread.sleep(100);

        storage.cleanupExpiredKeys();

        assertEquals(2, storage.size());
        assertTrue(storage.exists("key1"));
        assertTrue(storage.exists("key4"));
        assertFalse(storage.exists("key2"));
        assertFalse(storage.exists("key3"));
    }

    // ========== EDGE CASES ==========

    @Test
    @DisplayName("SET with empty string value")
    void testSetEmptyString() {
        storage.set("key1", "", null);
        assertEquals("", storage.get("key1"));
    }

    @Test
    @DisplayName("SET with empty string key")
    void testSetEmptyKey() {
        storage.set("", "value1", null);
        assertEquals("value1", storage.get(""));
    }

    @Test
    @DisplayName("SET with very long value")
    void testSetLongValue() {
        String longValue = "x".repeat(10000);
        storage.set("key1", longValue, null);
        assertEquals(longValue, storage.get("key1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"key", "key-with-dash", "key_with_underscore", "key:with:colon", "key.with.dot"})
    @DisplayName("SET with various key formats")
    void testSetWithVariousKeyFormats(String key) {
        storage.set(key, "value", null);
        assertEquals("value", storage.get(key));
    }

    // ========== CONCURRENCY Tests ==========

    @Test
    @DisplayName("Concurrent SET operations on same key")
    void testConcurrentSetSameKey() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                storage.set("key1", String.valueOf(value), null);
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Key should exist and have one of the values
        assertNotNull(storage.get("key1"));
        assertEquals(1, storage.size());
    }

    @Test
    @DisplayName("Concurrent SET operations on different keys")
    void testConcurrentSetDifferentKeys() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int keyNum = i;
            executor.submit(() -> {
                storage.set("key" + keyNum, "value" + keyNum, null);
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // All keys should exist
        assertEquals(threadCount, storage.size());

        // Verify all values
        for (int i = 0; i < threadCount; i++) {
            assertEquals("value" + i, storage.get("key" + i));
        }
    }

    @Test
    @DisplayName("Concurrent GET and SET operations")
    void testConcurrentGetAndSet() throws InterruptedException {
        storage.set("key1", "initial", null);

        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successfulReads = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                if (value % 2 == 0) {
                    storage.set("key1", String.valueOf(value), null);
                } else {
                    if (storage.get("key1") != null) {
                        successfulReads.incrementAndGet();
                    }
                }
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // All reads should have succeeded (key always exists)
        assertTrue(successfulReads.get() > 0);
        // Key should still exist
        assertNotNull(storage.get("key1"));
    }

    @Test
    @DisplayName("Concurrent expiry and access")
    void testConcurrentExpiryAndAccess() throws InterruptedException {
        long expiryTime = System.currentTimeMillis() + 100;
        storage.set("key1", "value1", expiryTime);

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger nullReads = new AtomicInteger(0);
        AtomicInteger nonNullReads = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    Thread.sleep((long) (Math.random() * 150));
                    String value = storage.get("key1");
                    if (value == null) {
                        nullReads.incrementAndGet();
                    } else {
                        nonNullReads.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Some reads should have gotten null (after expiry), some should have gotten the value
        assertTrue(nullReads.get() > 0, "Some reads should have gotten null after expiry");
        assertTrue(nonNullReads.get() > 0, "Some reads should have gotten the value before expiry");
    }

    @Test
    @DisplayName("Concurrent getAllKeys calls")
    void testConcurrentGetAllKeys() throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            storage.set("key" + i, "value" + i, null);
        }

        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                List<String> keys = storage.getAllKeys();
                assertEquals(50, keys.size());
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
    }

    // ========== STRESS TESTS ==========

    @Test
    @DisplayName("Stress test - many keys with varying expiry times")
    void testStressManyKeysWithExpiry() throws InterruptedException {
        int keyCount = 1000;
        long baseTime = System.currentTimeMillis();

        // Set 1000 keys with varying expiry times
        for (int i = 0; i < keyCount; i++) {
            Long expiry = (i % 3 == 0) ? null : baseTime + (i % 100) + 50;
            storage.set("key" + i, "value" + i, expiry);
        }

        // Wait for some to expire
        Thread.sleep(150);

        // Cleanup and verify
        storage.cleanupExpiredKeys();

        // Should have keys that had no expiry or long expiry
        int remainingKeys = storage.size();
        assertTrue(remainingKeys > 0 && remainingKeys < keyCount);
    }
}
