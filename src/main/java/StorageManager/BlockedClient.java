package StorageManager;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class BlockedClient {
  public final int clientId;
  public final OutputStream outputStream;
  public final String listKey; // For list operations (BLPOP)
  public final long timeoutMs; // 0 means wait indefinitely
  public final long blockStartTime;
  
  // Stream-specific fields
  public final List<String> streamKeys; // For stream operations (XREAD BLOCK)
  public final Map<String, String> lastIds; // Stream key -> last seen ID
  public final boolean isStreamOperation;
  
  // Constructor for list operations (existing BLPOP functionality)
  public BlockedClient(int clientId, OutputStream outputStream, String listKey, long timeoutMs) {
    this.clientId = clientId;
    this.outputStream = outputStream;
    this.listKey = listKey;
    this.timeoutMs = timeoutMs;
    this.blockStartTime = System.currentTimeMillis();
    this.streamKeys = null;
    this.lastIds = null;
    this.isStreamOperation = false;
  }
  
  // Constructor for stream operations (new XREAD BLOCK functionality)
  public BlockedClient(int clientId, OutputStream outputStream, List<String> streamKeys, 
                      Map<String, String> lastIds, long timeoutMs) {
    this.clientId = clientId;
    this.outputStream = outputStream;
    this.listKey = null;
    this.timeoutMs = timeoutMs;
    this.blockStartTime = System.currentTimeMillis();
    this.streamKeys = streamKeys;
    this.lastIds = lastIds;
    this.isStreamOperation = true;
  }
  
  public boolean isTimedOut() {
    if (timeoutMs == 0) return false; // Wait indefinitely
    return System.currentTimeMillis() - blockStartTime >= timeoutMs;
  }
}
