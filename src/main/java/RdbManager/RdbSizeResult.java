package RdbManager;

public class RdbSizeResult {
    public final int value;
    public final int bytesRead;

    public RdbSizeResult(int value, int bytesRead) {
        this.value = value;
        this.bytesRead = bytesRead;
    }
}
