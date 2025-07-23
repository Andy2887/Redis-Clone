package StorageManager;
import java.util.Map;

/**
 * Represents a Redis stream entry with ID and field-value pairs.
 */
public class StreamEntry {
    public final String id;
    public final Map<String, String> fields;
    
    public StreamEntry(String id, Map<String, String> fields) {
        this.id = id;
        this.fields = fields;
    }
    
    @Override
    public String toString() {
        return "StreamEntry{id='" + id + "', fields=" + fields + "}";
    }
}
