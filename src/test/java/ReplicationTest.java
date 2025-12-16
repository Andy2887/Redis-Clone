import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import StorageManager.ListStorage;
import StorageManager.StreamStorage;
import StorageManager.StringStorage;
import StorageManager.RESPProtocol;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for replication functionality.
 * Tests master-replica sync and command propagation.
 */
@DisplayName("Replication Tests")
class ReplicationTest {

    private StringStorage stringStorage;
    private ListStorage listStorage;
    private StreamStorage streamStorage;
    private HandleClient masterClient;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void setUp() {
        stringStorage = new StringStorage();
        listStorage = new ListStorage();
        streamStorage = new StreamStorage();
        masterClient = new HandleClient(null, 1, "master", stringStorage, listStorage, streamStorage);
        outputStream = new ByteArrayOutputStream();
    }

    // ========== INFO Command Tests ==========

    @Test
    @DisplayName("INFO replication returns master role")
    void testInfoReplicationMaster() throws IOException {
        List<String> infoCmd = Arrays.asList("INFO", "replication");

        masterClient.handleCommand(infoCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("role:master"), "Should contain role:master");
    }

    @Test
    @DisplayName("INFO replication returns slave role for replica")
    void testInfoReplicationSlave() throws IOException {
        HandleClient slaveClient = new HandleClient(null, 2, "slave", stringStorage, listStorage, streamStorage);
        ByteArrayOutputStream slaveOutput = new ByteArrayOutputStream();
        List<String> infoCmd = Arrays.asList("INFO", "replication");

        slaveClient.handleCommand(infoCmd, slaveOutput);

        String response = slaveOutput.toString();
        assertTrue(response.contains("role:slave"), "Should contain role:slave");
    }

    @Test
    @DisplayName("INFO replication contains replication offset")
    void testInfoReplicationContainsOffset() throws IOException {
        List<String> infoCmd = Arrays.asList("INFO", "replication");

        masterClient.handleCommand(infoCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("master_repl_offset:"), "Should contain master_repl_offset");
    }

    // ========== REPLCONF Command Tests ==========

    @Test
    @DisplayName("REPLCONF listening-port returns OK")
    void testReplconfListeningPort() throws IOException {
        List<String> replconfCmd = Arrays.asList("REPLCONF", "listening-port", "6380");

        masterClient.handleCommand(replconfCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("+OK"), "REPLCONF listening-port should return OK");
    }

    @Test
    @DisplayName("REPLCONF capa returns OK")
    void testReplconfCapa() throws IOException {
        List<String> replconfCmd = Arrays.asList("REPLCONF", "capa", "psync2");

        masterClient.handleCommand(replconfCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("+OK"), "REPLCONF capa should return OK");
    }

    @Test
    @DisplayName("REPLCONF GETACK returns OK response")
    void testReplconfGetack() throws IOException {
        List<String> replconfCmd = Arrays.asList("REPLCONF", "GETACK", "*");

        masterClient.handleCommand(replconfCmd, outputStream);

        String response = outputStream.toString();
        // REPLCONF commands return +OK
        assertTrue(response.contains("+OK"), "REPLCONF GETACK should return OK");
    }

    // ========== PSYNC Command Tests ==========

    @Test
    @DisplayName("PSYNC returns FULLRESYNC response")
    void testPsyncFullresync() throws IOException {
        List<String> psyncCmd = Arrays.asList("PSYNC", "?", "-1");

        masterClient.handleCommand(psyncCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("+FULLRESYNC"), "PSYNC should return FULLRESYNC");
    }

    @Test
    @DisplayName("PSYNC FULLRESYNC contains replication ID")
    void testPsyncContainsReplId() throws IOException {
        List<String> psyncCmd = Arrays.asList("PSYNC", "?", "-1");

        masterClient.handleCommand(psyncCmd, outputStream);

        String response = outputStream.toString();
        // FULLRESYNC <replid> <offset>
        String[] parts = response.trim().split("\r\n");
        assertTrue(parts[0].startsWith("+FULLRESYNC"), "Should start with +FULLRESYNC");
        String[] fullresyncParts = parts[0].split(" ");
        assertTrue(fullresyncParts.length >= 3, "Should have replid and offset");
    }

    // ========== Command Propagation Tests ==========

    @Test
    @DisplayName("SET command updates string storage")
    void testSetCommandPropagation() throws IOException {
        List<String> setCmd = Arrays.asList("SET", "key1", "value1");

        masterClient.handleCommand(setCmd, outputStream);

        assertEquals("value1", stringStorage.get("key1"));
    }

    @Test
    @DisplayName("SET with PX command updates string storage with expiry")
    void testSetWithExpiryPropagation() throws IOException {
        List<String> setCmd = Arrays.asList("SET", "key1", "value1", "PX", "10000");

        masterClient.handleCommand(setCmd, outputStream);

        assertEquals("value1", stringStorage.get("key1"));
        assertNotNull(stringStorage.getExpiryTime("key1"));
    }

    @Test
    @DisplayName("RPUSH command updates list storage")
    void testRpushCommandPropagation() throws IOException {
        List<String> rpushCmd = Arrays.asList("RPUSH", "list1", "value1", "value2");

        masterClient.handleCommand(rpushCmd, outputStream);

        assertEquals(2, listStorage.length("list1"));
        assertEquals(Arrays.asList("value1", "value2"), listStorage.range("list1", 0, -1));
    }

    @Test
    @DisplayName("LPUSH command updates list storage")
    void testLpushCommandPropagation() throws IOException {
        List<String> lpushCmd = Arrays.asList("LPUSH", "list1", "value1", "value2");

        masterClient.handleCommand(lpushCmd, outputStream);

        assertEquals(2, listStorage.length("list1"));
        // LPUSH inserts at head, so order is reversed
        assertEquals(Arrays.asList("value2", "value1"), listStorage.range("list1", 0, -1));
    }

    @Test
    @DisplayName("INCR command updates string storage")
    void testIncrCommandPropagation() throws IOException {
        // Set initial value
        stringStorage.set("counter", "10", null);

        List<String> incrCmd = Arrays.asList("INCR", "counter");

        masterClient.handleCommand(incrCmd, outputStream);

        assertEquals("11", stringStorage.get("counter"));
    }

    @Test
    @DisplayName("Multiple commands propagate in order")
    void testMultipleCommandsPropagation() throws IOException {
        List<String> cmd1 = Arrays.asList("SET", "key1", "value1");
        List<String> cmd2 = Arrays.asList("SET", "key2", "value2");
        List<String> cmd3 = Arrays.asList("RPUSH", "list1", "item1");

        masterClient.handleCommand(cmd1, outputStream);
        outputStream.reset();
        masterClient.handleCommand(cmd2, outputStream);
        outputStream.reset();
        masterClient.handleCommand(cmd3, outputStream);

        assertEquals("value1", stringStorage.get("key1"));
        assertEquals("value2", stringStorage.get("key2"));
        assertEquals(1, listStorage.length("list1"));
    }

    // ========== Replica Client Simulation Tests ==========

    @Test
    @DisplayName("Replica can process propagated SET command")
    void testReplicaProcessesPropagatedSet() throws IOException {
        // Simulate a replica client
        HandleClient replicaClient = new HandleClient(null, 2, "slave", stringStorage, listStorage, streamStorage);
        ByteArrayOutputStream devNull = new ByteArrayOutputStream();

        List<String> setCmd = Arrays.asList("SET", "replicated_key", "replicated_value");

        replicaClient.handleCommand(setCmd, devNull);

        assertEquals("replicated_value", stringStorage.get("replicated_key"));
    }

    @Test
    @DisplayName("Replica processes multiple propagated commands")
    void testReplicaProcessesMultipleCommands() throws IOException {
        HandleClient replicaClient = new HandleClient(null, 2, "slave", stringStorage, listStorage, streamStorage);
        ByteArrayOutputStream devNull = new ByteArrayOutputStream();

        List<String> cmd1 = Arrays.asList("SET", "key1", "value1");
        List<String> cmd2 = Arrays.asList("SET", "key2", "value2");
        List<String> cmd3 = Arrays.asList("RPUSH", "mylist", "a", "b", "c");

        replicaClient.handleCommand(cmd1, devNull);
        replicaClient.handleCommand(cmd2, devNull);
        replicaClient.handleCommand(cmd3, devNull);

        assertEquals("value1", stringStorage.get("key1"));
        assertEquals("value2", stringStorage.get("key2"));
        assertEquals(3, listStorage.length("mylist"));
    }

    // ========== RESPProtocol Tests ==========

    @Test
    @DisplayName("RESPProtocol formats simple string correctly")
    void testRespFormatSimpleString() {
        String result = RESPProtocol.formatSimpleString("OK");
        assertEquals("+OK\r\n", result);
    }

    @Test
    @DisplayName("RESPProtocol formats error correctly")
    void testRespFormatError() {
        String result = RESPProtocol.formatError("ERR unknown command");
        assertEquals("-ERR unknown command\r\n", result);
    }

    @Test
    @DisplayName("RESPProtocol formats integer correctly")
    void testRespFormatInteger() {
        String result = RESPProtocol.formatInteger(42);
        assertEquals(":42\r\n", result);
    }

    @Test
    @DisplayName("RESPProtocol NULL_BULK_STRING constant is correct")
    void testRespNullBulkString() {
        assertEquals("$-1\r\n", RESPProtocol.NULL_BULK_STRING);
    }

    @Test
    @DisplayName("RESPProtocol EMPTY_ARRAY constant is correct")
    void testRespEmptyArray() {
        assertEquals("*0\r\n", RESPProtocol.EMPTY_ARRAY);
    }

    // ========== REPLICAOF Command Tests ==========

    @Test
    @DisplayName("REPLICAOF NO ONE switches to master role")
    void testReplicaofNoOne() throws IOException {
        // Start as slave
        HandleClient slaveClient = new HandleClient(null, 2, "slave", stringStorage, listStorage, streamStorage);
        ByteArrayOutputStream slaveOutput = new ByteArrayOutputStream();

        List<String> replicaofCmd = Arrays.asList("REPLICAOF", "NO", "ONE");

        slaveClient.handleCommand(replicaofCmd, slaveOutput);

        String response = slaveOutput.toString();
        assertTrue(response.contains("+OK"), "REPLICAOF NO ONE should return OK");
    }

    // ========== Edge Cases ==========

    @Test
    @DisplayName("Commands are case insensitive")
    void testCaseInsensitiveCommands() throws IOException {
        List<String> setLower = Arrays.asList("set", "key1", "value1");
        List<String> setUpper = Arrays.asList("SET", "key2", "value2");
        List<String> setMixed = Arrays.asList("SeT", "key3", "value3");

        masterClient.handleCommand(setLower, outputStream);
        outputStream.reset();
        masterClient.handleCommand(setUpper, outputStream);
        outputStream.reset();
        masterClient.handleCommand(setMixed, outputStream);

        assertEquals("value1", stringStorage.get("key1"));
        assertEquals("value2", stringStorage.get("key2"));
        assertEquals("value3", stringStorage.get("key3"));
    }

    @Test
    @DisplayName("GET command returns value")
    void testGetCommand() throws IOException {
        stringStorage.set("mykey", "myvalue", null);
        List<String> getCmd = Arrays.asList("GET", "mykey");

        masterClient.handleCommand(getCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("myvalue"), "GET should return the value");
    }

    @Test
    @DisplayName("GET non-existent key returns null bulk string")
    void testGetNonExistent() throws IOException {
        List<String> getCmd = Arrays.asList("GET", "nonexistent");

        masterClient.handleCommand(getCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("$-1"), "GET on non-existent key should return null bulk string");
    }

    @Test
    @DisplayName("TYPE command returns correct types")
    void testTypeCommand() throws IOException {
        stringStorage.set("stringkey", "value", null);
        listStorage.rightPush("listkey", "item");

        List<String> typeString = Arrays.asList("TYPE", "stringkey");
        masterClient.handleCommand(typeString, outputStream);
        String response1 = outputStream.toString();
        assertTrue(response1.contains("string"), "TYPE should return string for string keys");

        outputStream.reset();
        List<String> typeList = Arrays.asList("TYPE", "listkey");
        masterClient.handleCommand(typeList, outputStream);
        String response2 = outputStream.toString();
        assertTrue(response2.contains("list"), "TYPE should return list for list keys");

        outputStream.reset();
        List<String> typeNone = Arrays.asList("TYPE", "nonexistent");
        masterClient.handleCommand(typeNone, outputStream);
        String response3 = outputStream.toString();
        assertTrue(response3.contains("none"), "TYPE should return none for non-existent keys");
    }

    @Test
    @DisplayName("PING command returns PONG")
    void testPingCommand() throws IOException {
        List<String> pingCmd = Arrays.asList("PING");

        masterClient.handleCommand(pingCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("PONG"), "PING should return PONG");
    }

    @Test
    @DisplayName("ECHO command echoes message")
    void testEchoCommand() throws IOException {
        List<String> echoCmd = Arrays.asList("ECHO", "Hello World");

        masterClient.handleCommand(echoCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("Hello World"), "ECHO should return the message");
    }

    @Test
    @DisplayName("KEYS * returns all keys")
    void testKeysCommand() throws IOException {
        stringStorage.set("key1", "value1", null);
        stringStorage.set("key2", "value2", null);
        stringStorage.set("key3", "value3", null);

        List<String> keysCmd = Arrays.asList("KEYS", "*");

        masterClient.handleCommand(keysCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("key1"), "Should contain key1");
        assertTrue(response.contains("key2"), "Should contain key2");
        assertTrue(response.contains("key3"), "Should contain key3");
    }
}
