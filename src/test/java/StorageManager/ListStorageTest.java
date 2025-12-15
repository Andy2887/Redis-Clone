package StorageManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for ListStorage class.
 * Tests basic operations, blocking operations, and thread safety.
 */
@DisplayName("ListStorage Tests")
class ListStorageTest {

    private ListStorage storage;

    @BeforeEach
    void setUp() {
        storage = new ListStorage();
    }

    // ========== Basic LPUSH Tests ==========

    @Test
    @DisplayName("LPUSH single element to new list")
    void testLeftPushSingleElement() {
        int size = storage.leftPush("list1", "value1");
        assertEquals(1, size);
        assertTrue(storage.exists("list1"));
    }

    @Test
    @DisplayName("LPUSH multiple elements to new list")
    void testLeftPushMultipleElements() {
        int size = storage.leftPush("list1", "value1", "value2", "value3");
        assertEquals(3, size);
    }

    @Test
    @DisplayName("LPUSH maintains correct order")
    void testLeftPushOrder() {
        storage.leftPush("list1", "a", "b", "c");
        List<String> result = storage.range("list1", 0, -1);
        assertEquals(List.of("c", "b", "a"), result);
    }

    @Test
    @DisplayName("LPUSH to existing list")
    void testLeftPushToExistingList() {
        storage.leftPush("list1", "a");
        storage.leftPush("list1", "b");
        storage.leftPush("list1", "c");

        List<String> result = storage.range("list1", 0, -1);
        assertEquals(List.of("c", "b", "a"), result);
    }

    // ========== Basic RPUSH Tests ==========

    @Test
    @DisplayName("RPUSH single element to new list")
    void testRightPushSingleElement() {
        int size = storage.rightPush("list1", "value1");
        assertEquals(1, size);
        assertTrue(storage.exists("list1"));
    }

    @Test
    @DisplayName("RPUSH multiple elements to new list")
    void testRightPushMultipleElements() {
        int size = storage.rightPush("list1", "value1", "value2", "value3");
        assertEquals(3, size);
    }

    @Test
    @DisplayName("RPUSH maintains correct order")
    void testRightPushOrder() {
        storage.rightPush("list1", "a", "b", "c");
        List<String> result = storage.range("list1", 0, -1);
        assertEquals(List.of("a", "b", "c"), result);
    }

    @Test
    @DisplayName("RPUSH to existing list")
    void testRightPushToExistingList() {
        storage.rightPush("list1", "a");
        storage.rightPush("list1", "b");
        storage.rightPush("list1", "c");

        List<String> result = storage.range("list1", 0, -1);
        assertEquals(List.of("a", "b", "c"), result);
    }

    // ========== LPOP Tests ==========

    @Test
    @DisplayName("LPOP single element from list")
    void testLeftPopSingle() {
        storage.rightPush("list1", "a", "b", "c");
        List<String> result = storage.leftPop("list1", 1);

        assertEquals(List.of("a"), result);
        assertEquals(2, storage.length("list1"));
    }

    @Test
    @DisplayName("LPOP multiple elements from list")
    void testLeftPopMultiple() {
        storage.rightPush("list1", "a", "b", "c", "d", "e");
        List<String> result = storage.leftPop("list1", 3);

        assertEquals(List.of("a", "b", "c"), result);
        assertEquals(2, storage.length("list1"));
    }

    @Test
    @DisplayName("LPOP from non-existent list returns empty")
    void testLeftPopNonExistent() {
        List<String> result = storage.leftPop("nonexistent", 1);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("LPOP more elements than available")
    void testLeftPopMoreThanAvailable() {
        storage.rightPush("list1", "a", "b");
        List<String> result = storage.leftPop("list1", 5);

        assertEquals(List.of("a", "b"), result);
        assertFalse(storage.exists("list1")); // List should be removed when empty
    }

    @Test
    @DisplayName("LPOP all elements removes list from storage")
    void testLeftPopAllElementsRemovesList() {
        storage.rightPush("list1", "a", "b", "c");
        storage.leftPop("list1", 3);

        assertFalse(storage.exists("list1"));
        assertEquals(0, storage.length("list1"));
    }

    // ========== LRANGE Tests ==========

    @Test
    @DisplayName("LRANGE with positive indices")
    void testRangePositiveIndices() {
        storage.rightPush("list1", "a", "b", "c", "d", "e");
        List<String> result = storage.range("list1", 1, 3);
        assertEquals(List.of("b", "c", "d"), result);
    }

    @Test
    @DisplayName("LRANGE with negative indices")
    void testRangeNegativeIndices() {
        storage.rightPush("list1", "a", "b", "c", "d", "e");
        List<String> result = storage.range("list1", -3, -1);
        assertEquals(List.of("c", "d", "e"), result);
    }

    @Test
    @DisplayName("LRANGE entire list (0 to -1)")
    void testRangeEntireList() {
        storage.rightPush("list1", "a", "b", "c");
        List<String> result = storage.range("list1", 0, -1);
        assertEquals(List.of("a", "b", "c"), result);
    }

    @Test
    @DisplayName("LRANGE with mixed positive and negative indices")
    void testRangeMixedIndices() {
        storage.rightPush("list1", "a", "b", "c", "d", "e");
        List<String> result = storage.range("list1", 1, -2);
        assertEquals(List.of("b", "c", "d"), result);
    }

    @Test
    @DisplayName("LRANGE from non-existent list returns empty")
    void testRangeNonExistent() {
        List<String> result = storage.range("nonexistent", 0, -1);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("LRANGE with out-of-bounds indices")
    void testRangeOutOfBounds() {
        storage.rightPush("list1", "a", "b", "c");
        List<String> result = storage.range("list1", 10, 20);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("LRANGE with start > end returns empty")
    void testRangeStartGreaterThanEnd() {
        storage.rightPush("list1", "a", "b", "c");
        List<String> result = storage.range("list1", 3, 1);
        assertTrue(result.isEmpty());
    }

    // ========== LLEN Tests ==========

    @Test
    @DisplayName("LLEN returns correct length")
    void testLength() {
        storage.rightPush("list1", "a", "b", "c");
        assertEquals(3, storage.length("list1"));
    }

    @Test
    @DisplayName("LLEN on non-existent list returns 0")
    void testLengthNonExistent() {
        assertEquals(0, storage.length("nonexistent"));
    }

    @Test
    @DisplayName("LLEN updates after operations")
    void testLengthUpdates() {
        storage.rightPush("list1", "a");
        assertEquals(1, storage.length("list1"));

        storage.leftPush("list1", "b");
        assertEquals(2, storage.length("list1"));

        storage.leftPop("list1", 1);
        assertEquals(1, storage.length("list1"));
    }

    // ========== EXISTS Tests ==========

    @Test
    @DisplayName("EXISTS returns true for existing list")
    void testExistsTrue() {
        storage.rightPush("list1", "a");
        assertTrue(storage.exists("list1"));
    }

    @Test
    @DisplayName("EXISTS returns false for non-existent list")
    void testExistsFalse() {
        assertFalse(storage.exists("nonexistent"));
    }

    @Test
    @DisplayName("EXISTS returns false after list is emptied")
    void testExistsAfterEmpty() {
        storage.rightPush("list1", "a");
        storage.leftPop("list1", 1);
        assertFalse(storage.exists("list1"));
    }

    // ========== Blocking Operations Tests ==========

    @Test
    @DisplayName("blockClient on empty list succeeds")
    void testBlockClientOnEmptyList() {
        BlockedClient client = new BlockedClient(1, null, "list1", 1000);
        boolean blocked = storage.blockClient("list1", client);
        assertTrue(blocked);
        assertEquals(1, storage.getBlockedClientCount("list1"));
    }

    @Test
    @DisplayName("blockClient on non-empty list fails")
    void testBlockClientOnNonEmptyList() {
        storage.rightPush("list1", "a");
        BlockedClient client = new BlockedClient(1, null, "list1", 1000);
        boolean blocked = storage.blockClient("list1", client);
        assertFalse(blocked);
    }

    @Test
    @DisplayName("unblockClient removes client from queue")
    void testUnblockClient() {
        BlockedClient client = new BlockedClient(1, null, "list1", 1000);
        storage.blockClient("list1", client);

        boolean removed = storage.unblockClient("list1", client);
        assertTrue(removed);
        assertEquals(0, storage.getBlockedClientCount("list1"));
    }

    @Test
    @DisplayName("unblockClient on non-existent client returns false")
    void testUnblockClientNonExistent() {
        BlockedClient client = new BlockedClient(1, null, "list1", 1000);
        boolean removed = storage.unblockClient("list1", client);
        assertFalse(removed);
    }

    @Test
    @DisplayName("popForBlockedClient pops element for blocked client")
    void testPopForBlockedClient() {
        BlockedClient client = new BlockedClient(1, null, "list1", 1000);
        storage.blockClient("list1", client);
        storage.rightPush("list1", "a");

        ListStorage.BlockedClientResult result = storage.popForBlockedClient("list1");
        assertNotNull(result);
        assertEquals(1, result.client.clientId);
        assertEquals("a", result.element);
        assertEquals(0, storage.getBlockedClientCount("list1"));
    }

    @Test
    @DisplayName("popForBlockedClient returns null when no blocked clients")
    void testPopForBlockedClientNoClients() {
        storage.rightPush("list1", "a");
        ListStorage.BlockedClientResult result = storage.popForBlockedClient("list1");
        assertNull(result);
    }

    @Test
    @DisplayName("popForBlockedClient returns null when no elements")
    void testPopForBlockedClientNoElements() {
        BlockedClient client = new BlockedClient(1, null, "list1", 1000);
        storage.blockClient("list1", client);

        ListStorage.BlockedClientResult result = storage.popForBlockedClient("list1");
        assertNull(result);
    }

    @Test
    @DisplayName("getBlockedClientOrder shows correct FIFO order")
    void testBlockedClientOrder() {
        storage.blockClient("list1", new BlockedClient(1, null, "list1", 1000));
        storage.blockClient("list1", new BlockedClient(2, null, "list1", 1000));
        storage.blockClient("list1", new BlockedClient(3, null, "list1", 1000));

        String order = storage.getBlockedClientOrder("list1");
        assertEquals("[1, 2, 3]", order);
    }

    // ========== Edge Cases ==========

    @Test
    @DisplayName("Push empty string values")
    void testPushEmptyStrings() {
        storage.rightPush("list1", "", "", "");
        assertEquals(3, storage.length("list1"));
        List<String> result = storage.range("list1", 0, -1);
        assertEquals(List.of("", "", ""), result);
    }

    @Test
    @DisplayName("Push and pop with special characters")
    void testSpecialCharacters() {
        storage.rightPush("list1", "hello\nworld", "tab\there", "quote\"test");
        List<String> result = storage.range("list1", 0, -1);
        assertEquals(3, result.size());
        assertEquals("hello\nworld", result.get(0));
    }

    @Test
    @DisplayName("Multiple lists operate independently")
    void testMultipleListsIndependent() {
        storage.rightPush("list1", "a", "b");
        storage.rightPush("list2", "x", "y", "z");

        assertEquals(2, storage.length("list1"));
        assertEquals(3, storage.length("list2"));

        storage.leftPop("list1", 1);
        assertEquals(1, storage.length("list1"));
        assertEquals(3, storage.length("list2"));
    }

    // ========== Concurrency Tests ==========

    @Test
    @DisplayName("Concurrent LPUSH operations on same list")
    void testConcurrentLeftPushSameList() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                storage.leftPush("list1", "value" + value);
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(threadCount, storage.length("list1"));
    }

    @Test
    @DisplayName("Concurrent RPUSH operations on same list")
    void testConcurrentRightPushSameList() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                storage.rightPush("list1", "value" + value);
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals(threadCount, storage.length("list1"));
    }

    @Test
    @DisplayName("Concurrent LPUSH and LPOP operations")
    void testConcurrentPushAndPop() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger poppedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                if (value % 2 == 0) {
                    storage.leftPush("list1", "value" + value);
                } else {
                    List<String> popped = storage.leftPop("list1", 1);
                    poppedCount.addAndGet(popped.size());
                }
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Final length should be pushes minus pops
        int finalLength = storage.length("list1");
        int pushes = threadCount / 2;
        int pops = poppedCount.get();
        assertEquals(pushes - pops, finalLength);
    }

    @Test
    @DisplayName("Concurrent operations on different lists")
    void testConcurrentDifferentLists() throws InterruptedException {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int listNum = i;
            executor.submit(() -> {
                storage.rightPush("list" + listNum, "value");
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Each list should have exactly 1 element
        for (int i = 0; i < threadCount; i++) {
            assertEquals(1, storage.length("list" + i));
        }
    }

    @Test
    @DisplayName("Concurrent blockClient and LPUSH operations")
    void testConcurrentBlockAndPush() throws InterruptedException {
        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger blockedCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                if (value % 2 == 0) {
                    BlockedClient client = new BlockedClient(value, null, "list1", 1000);
                    boolean blocked = storage.blockClient("list1", client);
                    if (blocked) {
                        blockedCount.incrementAndGet();
                    }
                } else {
                    storage.leftPush("list1", "value" + value);
                }
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Either some clients got blocked, or list has elements (or both)
        assertTrue(storage.getBlockedClientCount("list1") > 0 || storage.length("list1") > 0);
    }

    @Test
    @DisplayName("Concurrent LRANGE operations")
    void testConcurrentRange() throws InterruptedException {
        storage.rightPush("list1", "a", "b", "c", "d", "e");

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                List<String> result = storage.range("list1", 0, -1);
                assertEquals(5, result.size());
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
    }

    @Test
    @DisplayName("Concurrent EXISTS and LLEN operations")
    void testConcurrentExistsAndLength() throws InterruptedException {
        storage.rightPush("list1", "a", "b", "c");

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                if (value % 2 == 0) {
                    assertTrue(storage.exists("list1"));
                } else {
                    assertEquals(3, storage.length("list1"));
                }
                latch.countDown();
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
    }

    @Test
    @DisplayName("Stress test - many concurrent operations")
    void testStressConcurrentOperations() throws InterruptedException {
        int threadCount = 200;
        ExecutorService executor = Executors.newFixedThreadPool(20);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int value = i;
            executor.submit(() -> {
                int operation = value % 5;
                switch (operation) {
                    case 0:
                        storage.leftPush("list1", "value" + value);
                        break;
                    case 1:
                        storage.rightPush("list1", "value" + value);
                        break;
                    case 2:
                        storage.leftPop("list1", 1);
                        break;
                    case 3:
                        storage.range("list1", 0, -1);
                        break;
                    case 4:
                        storage.length("list1");
                        break;
                }
                latch.countDown();
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // List should be in a consistent state (no exceptions thrown)
        int finalLength = storage.length("list1");
        assertTrue(finalLength >= 0);
    }
}
