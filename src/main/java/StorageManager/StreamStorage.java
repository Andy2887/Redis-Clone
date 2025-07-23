package StorageManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * Storage manager for Redis stream data type.
 * Handles stream operations including XADD and XRANGE.
 */
public class StreamStorage {
    // Storage for streams (key -> list of stream entries)
    private final Map<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();
    
    /**
     * Adds an entry to a stream.
     * 
     * @param key The stream key
     * @param entryId The entry ID (may contain "*" for auto-generation)
     * @param fields The field-value pairs for the entry
     * @return The actual entry ID that was used, or null if validation failed
     * @throws IllegalArgumentException if the entry ID is invalid
     */
    public String addEntry(String key, String entryId, Map<String, String> fields) throws IllegalArgumentException {
        // Get or create the stream
        List<StreamEntry> stream = streams.computeIfAbsent(key, k -> new ArrayList<>());
        
        synchronized (stream) {
            // Auto-generate sequence number if needed
            String actualEntryId = StreamIdHelper.generateEntryId(entryId, key, stream);
            
            // Validate entry ID format and value
            String validationError = StreamIdHelper.validateEntryId(actualEntryId, stream);
            if (validationError != null) {
                throw new IllegalArgumentException(validationError);
            }
            
            // Create and add stream entry
            StreamEntry entry = new StreamEntry(actualEntryId, fields);
            stream.add(entry);
            
            return actualEntryId;
        }
    }
    
    /**
     * Gets entries from a stream within a specified range.
     * 
     * @param key The stream key
     * @param startId The start ID ("-" for minimum)
     * @param endId The end ID ("+" for maximum)
     * @return List of matching stream entries, or empty list if stream doesn't exist
     */
    public List<StreamEntry> getRange(String key, String startId, String endId) {
        List<StreamEntry> stream = streams.get(key);
        
        if (stream == null || stream.isEmpty()) {
            return new ArrayList<>(); // Empty result
        }
        
        synchronized (stream) {
            List<StreamEntry> matchingEntries = new ArrayList<>();
            
            for (StreamEntry entry : stream) {
                if (StreamIdHelper.isEntryInRange(entry.id, startId, endId)) {
                    matchingEntries.add(entry);
                }
            }
            
            return matchingEntries;
        }
    }
    
    /**
     * Gets entries from a stream that have IDs greater than the specified ID (exclusive).
     * This is used by the XREAD command.
     * 
     * @param key The stream key
     * @param afterId The ID to read after (exclusive)
     * @return List of matching stream entries, or empty list if stream doesn't exist
     */
    public List<StreamEntry> getEntriesAfter(String key, String afterId) {
        List<StreamEntry> stream = streams.get(key);
        
        if (stream == null || stream.isEmpty()) {
            return new ArrayList<>(); // Empty result
        }
        
        synchronized (stream) {
            List<StreamEntry> matchingEntries = new ArrayList<>();
            
            for (StreamEntry entry : stream) {
                // Only include entries with ID greater than afterId (exclusive)
                if (StreamIdHelper.compareStreamIds(entry.id, afterId) > 0) {
                    matchingEntries.add(entry);
                }
            }
            
            return matchingEntries;
        }
    }
    
    /**
     * Checks if a stream exists and is not empty.
     * 
     * @param key The stream key
     * @return true if the stream exists and has entries
     */
    public boolean exists(String key) {
        List<StreamEntry> stream = streams.get(key);
        return stream != null && !stream.isEmpty();
    }
    
    /**
     * Gets the number of entries in a stream.
     * 
     * @param key The stream key
     * @return The number of entries, or 0 if stream doesn't exist
     */
    public int length(String key) {
        List<StreamEntry> stream = streams.get(key);
        
        if (stream == null) {
            return 0;
        }
        
        synchronized (stream) {
            return stream.size();
        }
    }
    
    /**
     * Gets the last entry ID in a stream.
     * 
     * @param key The stream key
     * @return The last entry ID, or null if stream doesn't exist or is empty
     */
    public String getLastEntryId(String key) {
        List<StreamEntry> stream = streams.get(key);
        
        if (stream == null || stream.isEmpty()) {
            return null;
        }
        
        synchronized (stream) {
            if (stream.isEmpty()) {
                return null;
            }
            return stream.get(stream.size() - 1).id;
        }
    }
    
    /**
     * Gets the first entry ID in a stream.
     * 
     * @param key The stream key
     * @return The first entry ID, or null if stream doesn't exist or is empty
     */
    public String getFirstEntryId(String key) {
        List<StreamEntry> stream = streams.get(key);
        
        if (stream == null || stream.isEmpty()) {
            return null;
        }
        
        synchronized (stream) {
            if (stream.isEmpty()) {
                return null;
            }
            return stream.get(0).id;
        }
    }
    
    /**
     * Gets all entries in a stream (for debugging or full export).
     * 
     * @param key The stream key
     * @return All entries in the stream, or empty list if stream doesn't exist
     */
    public List<StreamEntry> getAllEntries(String key) {
        List<StreamEntry> stream = streams.get(key);
        
        if (stream == null) {
            return new ArrayList<>();
        }
        
        synchronized (stream) {
            return new ArrayList<>(stream); // Return a copy to avoid concurrent modification
        }
    }
    
    /**
     * Removes a stream completely.
     * 
     * @param key The stream key to remove
     * @return true if the stream existed and was removed
     */
    public boolean removeStream(String key) {
        return streams.remove(key) != null;
    }
    
    /**
     * Gets the number of streams.
     * 
     * @return The total number of streams
     */
    public int getStreamCount() {
        return streams.size();
    }
    
    /**
     * Gets all stream keys.
     * 
     * @return A list of all stream keys
     */
    public List<String> getAllStreamKeys() {
        return new ArrayList<>(streams.keySet());
    }
    
    /**
     * Clears all streams (for testing or reset).
     */
    public void clear() {
        streams.clear();
    }
}
