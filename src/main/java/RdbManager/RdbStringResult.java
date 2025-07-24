package RdbManager;

public class RdbStringResult {
    public final String value;
    public final int bytesRead;

    public RdbStringResult(String value, int bytesRead) {
        this.value = value;
        this.bytesRead = bytesRead;
    }
}
