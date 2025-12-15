import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import StorageManager.ListStorage;
import StorageManager.StreamStorage;
import StorageManager.StringStorage;

import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@DisplayName("Transaction Tests")
class TransactionTest {

    private HandleClient client;
    private StringStorage stringStorage;
    private ListStorage listStorage;
    private StreamStorage streamStorage;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void setUp() {
        stringStorage = new StringStorage();
        listStorage = new ListStorage();
        streamStorage = new StreamStorage();
        // Create a mock socket or test client
        client = new HandleClient(null, 1, "master",
                                stringStorage, listStorage, streamStorage);
        outputStream = new ByteArrayOutputStream();
    }

    // ====== Basic MULTI Test ======

    @Test
    @DisplayName("MULTI returns OK")
    void testMultiReturnsOk() throws IOException {
        List<String> multiCmd = Arrays.asList("MULTI");

        client.handleCommand(multiCmd, outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("+OK"), "MULTI should return +OK");
    }

    @Test
    @DisplayName("MULTI initializes transaction state")
    void testMultiInitializesTransaction() throws IOException {
        List<String> multiCmd = Arrays.asList("MULTI");
        List<String> setCmd = Arrays.asList("SET", "Key1", "Value1");

        client.handleCommand(multiCmd, outputStream);
        outputStream.reset();

        client.handleCommand(setCmd, outputStream);
        String response = outputStream.toString();
        assertTrue(response.contains("+QUEUED"), "Commands after MULTI should be queued");
    }

    // ========== Command Queuing Tests ==========

    @Test
    @DisplayName("Commands are queued after MULTI")
    void testCommandsAreQueued() throws IOException {
        client.handleCommand(Arrays.asList("MULTI"), outputStream);
        outputStream.reset();

        // Queue multiple commands
        client.handleCommand(Arrays.asList("SET", "key1", "value1"), outputStream);
        assertTrue(outputStream.toString().contains("+QUEUED"), "Commands after MULTI should be queued");
        outputStream.reset();

        client.handleCommand(Arrays.asList("SET", "key2", "value2"), outputStream);
        assertTrue(outputStream.toString().contains("+QUEUED"), "Commands after MULTI should be queued");
        outputStream.reset();

        client.handleCommand(Arrays.asList("GET", "key1"), outputStream);
        assertTrue(outputStream.toString().contains("+QUEUED"), "Commands after MULTI should be queued");
    }

    @Test
    @DisplayName("Queued commands do not execute immediately")
    void testQueuedCommandsNotExecutedImmediately() throws IOException {
        client.handleCommand(Arrays.asList("MULTI"), outputStream);

        // Queue one SET command
        client.handleCommand(Arrays.asList("SET", "key1", "value1"), outputStream);

        assertTrue(stringStorage.get("key1") == null, "String Storage should not store key1");
        
    }

    // ========== EXEC Tests ==========

    @Test
    @DisplayName("EXEC executes all queued commands")
    void testExecExecutesQueuedCommands() throws IOException {
        client.handleCommand(Arrays.asList("MULTI"), outputStream);

        // Queue multiple commands
        client.handleCommand(Arrays.asList("SET", "key1", "value1"), outputStream);
        client.handleCommand(Arrays.asList("SET", "key2", "value2"), outputStream);
        client.handleCommand(Arrays.asList("GET", "key1"), outputStream);

        client.handleCommand(Arrays.asList("EXEC"), outputStream);
        // Verify commands were executed
        assertEquals("value1", stringStorage.get("key1"));
        assertEquals("value2", stringStorage.get("key2"));
    }

    @Test
    @DisplayName("EXEC clears transaction state")
    void testExecClearsTransactionState() throws IOException {
        // Execute transaction
        client.handleCommand(Arrays.asList("MULTI"), outputStream);
        client.handleCommand(Arrays.asList("SET", "key1", "value1"), outputStream);
        client.handleCommand(Arrays.asList("EXEC"), outputStream);

        // Next command should execute normally (not queue)
        outputStream.reset();
        client.handleCommand(Arrays.asList("SET", "key2", "value2"), outputStream);

        String response = outputStream.toString();
        assertTrue(response.contains("+OK"), "Commands after EXEC should execute normally");
        assertFalse(response.contains("QUEUED"), "Commands after EXEC should not be queued");
    }

}
