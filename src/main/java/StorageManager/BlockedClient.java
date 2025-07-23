package StorageManager;
import java.io.OutputStream;

public class BlockedClient {
  public final int clientId;
  public final OutputStream outputStream;
  public final String listKey;
  public final long timeoutMs; // 0 means wait indefinitely
  public final long blockStartTime;
  
  public BlockedClient(int clientId, OutputStream outputStream, String listKey, long timeoutMs) {
    this.clientId = clientId;
    this.outputStream = outputStream;
    this.listKey = listKey;
    this.timeoutMs = timeoutMs;
    this.blockStartTime = System.currentTimeMillis();
  }
  
  public boolean isTimedOut() {
    if (timeoutMs == 0) return false; // Wait indefinitely
    return System.currentTimeMillis() - blockStartTime >= timeoutMs;
  }
}
