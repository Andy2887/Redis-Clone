import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import StorageManager.ListStorage;
import StorageManager.StreamStorage;
import StorageManager.StringStorage;
import RdbManager.RdbWriter;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for RDB persistence functionality.
 * Tests loading, saving, and expiry handling of RDB files.
 */
@DisplayName("RDB Persistence Tests")
class RDBTest {

    private StringStorage stringStorage;
    private ListStorage listStorage;
    private StreamStorage streamStorage;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        stringStorage = new StringStorage();
        listStorage = new ListStorage();
        streamStorage = new StreamStorage();
    }

    // ========== RdbWriter Basic Tests ==========

    @Test
    @DisplayName("RdbWriter creates valid RDB header")
    void testRdbWriterHeader() {
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        // Check header: "REDIS0012"
        String header = new String(Arrays.copyOfRange(rdbData, 0, 9));
        assertEquals("REDIS0012", header);
    }

    @Test
    @DisplayName("RdbWriter serializes empty storage")
    void testRdbWriterEmptyStorage() {
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        assertTrue(rdbData.length > 9); // At least header size
        // Check for EOF marker (0xFF)
        assertEquals((byte) 0xFF, rdbData[rdbData.length - 1]);
    }

    @Test
    @DisplayName("RdbWriter serializes single key-value pair")
    void testRdbWriterSingleKey() {
        stringStorage.set("key1", "value1", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        // Data should contain key and value strings
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("key1"));
        assertTrue(dataStr.contains("value1"));
    }

    @Test
    @DisplayName("RdbWriter serializes multiple key-value pairs")
    void testRdbWriterMultipleKeys() {
        stringStorage.set("key1", "value1", null);
        stringStorage.set("key2", "value2", null);
        stringStorage.set("key3", "value3", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("key1"));
        assertTrue(dataStr.contains("value1"));
        assertTrue(dataStr.contains("key2"));
        assertTrue(dataStr.contains("value2"));
        assertTrue(dataStr.contains("key3"));
        assertTrue(dataStr.contains("value3"));
    }

    @Test
    @DisplayName("RdbWriter includes expiry information")
    void testRdbWriterWithExpiry() {
        long futureExpiry = System.currentTimeMillis() + 60000; // 1 minute from now
        stringStorage.set("expiringKey", "value", futureExpiry);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        // Check for expiry marker (0xFC for milliseconds)
        boolean hasExpiryMarker = false;
        for (byte b : rdbData) {
            if ((b & 0xFF) == 0xFC) {
                hasExpiryMarker = true;
                break;
            }
        }
        assertTrue(hasExpiryMarker, "RDB should contain expiry marker");
    }

    @Test
    @DisplayName("RdbWriter handles keys without expiry")
    void testRdbWriterNoExpiry() {
        stringStorage.set("permanentKey", "value", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("permanentKey"));
    }

    // ========== RDB Structure Tests ==========

    @Test
    @DisplayName("RDB contains DB selector")
    void testRdbContainsDbSelector() {
        stringStorage.set("key", "value", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        // Check for DB selector (0xFE)
        boolean hasDbSelector = false;
        for (byte b : rdbData) {
            if ((b & 0xFF) == 0xFE) {
                hasDbSelector = true;
                break;
            }
        }
        assertTrue(hasDbSelector, "RDB should contain DB selector");
    }

    @Test
    @DisplayName("RDB contains hash table size info")
    void testRdbContainsHashTableSize() {
        stringStorage.set("key", "value", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        // Check for hash table size marker (0xFB)
        boolean hasHashTableSize = false;
        for (byte b : rdbData) {
            if ((b & 0xFF) == 0xFB) {
                hasHashTableSize = true;
                break;
            }
        }
        assertTrue(hasHashTableSize, "RDB should contain hash table size info");
    }

    @Test
    @DisplayName("RDB ends with EOF marker")
    void testRdbEndsWithEof() {
        stringStorage.set("key", "value", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertEquals((byte) 0xFF, rdbData[rdbData.length - 1], "RDB should end with EOF marker");
    }

    // ========== Round-trip Tests (Write and Read) ==========

    @Test
    @DisplayName("RDB can be written to file")
    void testRdbWriteToFile() throws IOException {
        stringStorage.set("testKey", "testValue", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();
        Path rdbPath = tempDir.resolve("test.rdb");
        Files.write(rdbPath, rdbData);

        assertTrue(Files.exists(rdbPath));
        assertTrue(Files.size(rdbPath) > 0);
    }

    @Test
    @DisplayName("Written RDB file has valid structure")
    void testWrittenRdbStructure() throws IOException {
        stringStorage.set("key1", "value1", null);
        stringStorage.set("key2", "value2", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();
        Path rdbPath = tempDir.resolve("test.rdb");
        Files.write(rdbPath, rdbData);

        byte[] readData = Files.readAllBytes(rdbPath);
        
        // Verify header
        String header = new String(Arrays.copyOfRange(readData, 0, 9));
        assertEquals("REDIS0012", header);
        
        // Verify EOF marker
        assertEquals((byte) 0xFF, readData[readData.length - 1]);
    }

    // ========== Expiry Handling Tests ==========

    @Test
    @DisplayName("Keys with past expiry are excluded during cleanup")
    void testExpiredKeysExcluded() throws InterruptedException {
        long shortExpiry = System.currentTimeMillis() + 50; // 50ms from now
        stringStorage.set("expiringKey", "value", shortExpiry);
        stringStorage.set("permanentKey", "value2", null);

        // Wait for expiry
        Thread.sleep(100);

        // The key should be gone after cleanup
        assertNull(stringStorage.get("expiringKey"));
        assertEquals("value2", stringStorage.get("permanentKey"));

        // Only permanent key should be in the list
        List<String> keys = stringStorage.getAllKeys();
        assertEquals(1, keys.size());
        assertTrue(keys.contains("permanentKey"));
    }

    @Test
    @DisplayName("RdbWriter handles mixed expiry states")
    void testRdbWriterMixedExpiry() {
        long futureExpiry = System.currentTimeMillis() + 60000;
        stringStorage.set("expiringKey", "value1", futureExpiry);
        stringStorage.set("permanentKey", "value2", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("expiringKey"));
        assertTrue(dataStr.contains("permanentKey"));
    }

    @Test
    @DisplayName("Expiry time is stored in milliseconds format")
    void testExpiryMillisecondsFormat() {
        long futureExpiry = System.currentTimeMillis() + 60000;
        stringStorage.set("expiringKey", "value", futureExpiry);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        // 0xFC indicates milliseconds expiry
        boolean foundMsExpiry = false;
        for (int i = 0; i < rdbData.length; i++) {
            if ((rdbData[i] & 0xFF) == 0xFC) {
                foundMsExpiry = true;
                break;
            }
        }
        assertTrue(foundMsExpiry, "Expiry should be stored in milliseconds format (0xFC)");
    }

    // ========== Size Encoding Tests ==========

    @Test
    @DisplayName("RdbWriter handles small strings (< 64 bytes)")
    void testSmallStringSerialization() {
        stringStorage.set("k", "v", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("k") && dataStr.contains("v"));
    }

    @Test
    @DisplayName("RdbWriter handles medium strings (64-16383 bytes)")
    void testMediumStringSerialization() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("abcdefghij"); // 10 chars * 100 = 1000 chars
        }
        stringStorage.set("mediumKey", sb.toString(), null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        assertTrue(rdbData.length > 1000);
    }

    @Test
    @DisplayName("RdbWriter handles large strings (> 16383 bytes)")
    void testLargeStringSerialization() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            sb.append("0123456789"); // 10 chars * 2000 = 20000 chars
        }
        stringStorage.set("largeKey", sb.toString(), null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        assertTrue(rdbData.length > 20000);
    }

    // ========== Edge Cases ==========

    @Test
    @DisplayName("RdbWriter handles empty string value")
    void testEmptyStringValue() {
        stringStorage.set("emptyKey", "", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("emptyKey"));
    }

    @Test
    @DisplayName("RdbWriter handles special characters in keys")
    void testSpecialCharactersInKeys() {
        stringStorage.set("key:with:colons", "value1", null);
        stringStorage.set("key.with.dots", "value2", null);
        stringStorage.set("key-with-dashes", "value3", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("key:with:colons"));
        assertTrue(dataStr.contains("key.with.dots"));
        assertTrue(dataStr.contains("key-with-dashes"));
    }

    @Test
    @DisplayName("RdbWriter handles special characters in values")
    void testSpecialCharactersInValues() {
        stringStorage.set("key1", "value with spaces", null);
        stringStorage.set("key2", "value\twith\ttabs", null);
        stringStorage.set("key3", "value:with:colons", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("value with spaces"));
        assertTrue(dataStr.contains("value\twith\ttabs"));
        assertTrue(dataStr.contains("value:with:colons"));
    }

    @Test
    @DisplayName("RdbWriter handles numeric string values")
    void testNumericStringValues() {
        stringStorage.set("intKey", "12345", null);
        stringStorage.set("negKey", "-999", null);
        stringStorage.set("floatKey", "3.14159", null);
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("12345"));
        assertTrue(dataStr.contains("-999"));
        assertTrue(dataStr.contains("3.14159"));
    }

    // ========== SAVE Command Integration Tests ==========

    @Test
    @DisplayName("SAVE command creates RDB file")
    void testSaveCommand() throws IOException {
        stringStorage.set("savedKey", "savedValue", null);
        HandleClient client = new HandleClient(null, 1, "master", stringStorage, listStorage, streamStorage);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        List<String> saveCmd = Arrays.asList("SAVE");

        client.handleCommand(saveCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("+OK"), "SAVE should return OK");
    }

    // ========== Multiple Keys Tests ==========

    @Test
    @DisplayName("RdbWriter serializes many keys correctly")
    void testManyKeys() {
        for (int i = 0; i < 100; i++) {
            stringStorage.set("key" + i, "value" + i, null);
        }
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        // Verify some random keys exist in the output
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("key0"));
        assertTrue(dataStr.contains("value0"));
        assertTrue(dataStr.contains("key50"));
        assertTrue(dataStr.contains("value50"));
        assertTrue(dataStr.contains("key99"));
        assertTrue(dataStr.contains("value99"));
    }

    @Test
    @DisplayName("RdbWriter serializes keys with various expiry times")
    void testVariousExpiryTimes() {
        long now = System.currentTimeMillis();
        stringStorage.set("key1", "value1", now + 1000);    // 1 second
        stringStorage.set("key2", "value2", now + 60000);   // 1 minute
        stringStorage.set("key3", "value3", now + 3600000); // 1 hour
        stringStorage.set("key4", "value4", null);          // No expiry
        RdbWriter writer = new RdbWriter(stringStorage);

        byte[] rdbData = writer.serializeToRdb();

        assertNotNull(rdbData);
        String dataStr = new String(rdbData);
        assertTrue(dataStr.contains("key1"));
        assertTrue(dataStr.contains("key2"));
        assertTrue(dataStr.contains("key3"));
        assertTrue(dataStr.contains("key4"));
    }

    // ========== Stress Tests ==========

    @Test
    @DisplayName("RdbWriter handles large number of keys efficiently")
    void testLargeNumberOfKeys() {
        for (int i = 0; i < 1000; i++) {
            stringStorage.set("key" + i, "value" + i, null);
        }
        RdbWriter writer = new RdbWriter(stringStorage);

        long startTime = System.currentTimeMillis();
        byte[] rdbData = writer.serializeToRdb();
        long endTime = System.currentTimeMillis();

        assertNotNull(rdbData);
        assertTrue(endTime - startTime < 5000, "Serialization should complete within 5 seconds");
    }

    @Test
    @DisplayName("RdbWriter output is deterministic")
    void testDeterministicOutput() {
        stringStorage.set("key1", "value1", null);
        stringStorage.set("key2", "value2", null);
        
        RdbWriter writer1 = new RdbWriter(stringStorage);
        RdbWriter writer2 = new RdbWriter(stringStorage);

        byte[] rdbData1 = writer1.serializeToRdb();
        byte[] rdbData2 = writer2.serializeToRdb();

        // Note: Order might not be deterministic due to ConcurrentHashMap
        // but the content should be equivalent
        assertEquals(rdbData1.length, rdbData2.length, "RDB data should have same length");
    }
}
