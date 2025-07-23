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
            
            // Notify blocked clients after adding the entry
            notifyBlockedClients(key);
            
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
        blockedClients.clear();
    }
    
    // Blocking operations support
    private final Map<String, List<BlockedClient>> blockedClients = new ConcurrentHashMap<>();
    
    /**
     * Blocks a client waiting for new entries on any of the specified streams.
     * 
     * @param client The blocked client information
     * @return true if the client was successfully blocked, false if entries are already available
     */
    public boolean blockClientOnStreams(BlockedClient client) {
        if (!client.isStreamOperation) {
            throw new IllegalArgumentException("Client is not configured for stream operations");
        }
        
        // First check if any of the streams already have new entries
        for (int i = 0; i < client.streamKeys.size(); i++) {
            String streamKey = client.streamKeys.get(i);
            String lastId = client.lastIds.get(streamKey);
            
            List<StreamEntry> newEntries = getEntriesAfter(streamKey, lastId);
            if (!newEntries.isEmpty()) {
                // There are already new entries, don't block
                return false;
            }
        }
        
        // No new entries found, block the client on all streams
        for (String streamKey : client.streamKeys) {
            blockedClients.computeIfAbsent(streamKey, k -> new ArrayList<>()).add(client);
        }
        
        return true;
    }
    
    /**
     * Unblocks a client from all streams it was waiting on.
     * 
     * @param client The client to unblock
     * @return true if the client was found and removed from blocking queues
     */
    public boolean unblockClient(BlockedClient client) {
        boolean wasBlocked = false;
        
        if (client.isStreamOperation && client.streamKeys != null) {
            for (String streamKey : client.streamKeys) {
                List<BlockedClient> clients = blockedClients.get(streamKey);
                if (clients != null) {
                    if (clients.remove(client)) {
                        wasBlocked = true;
                    }
                    if (clients.isEmpty()) {
                        blockedClients.remove(streamKey);
                    }
                }
            }
        }
        
        return wasBlocked;
    }
    
    /**
     * Notifies blocked clients when new entries are added to a stream.
     * This should be called after adding entries to a stream.
     * 
     * @param streamKey The stream that received new entries
     */
    public void notifyBlockedClients(String streamKey) {
        List<BlockedClient> clients = blockedClients.get(streamKey);
        if (clients == null || clients.isEmpty()) {
            return;
        }
        
        // Create a copy to avoid concurrent modification
        List<BlockedClient> clientsCopy = new ArrayList<>(clients);
        
        for (BlockedClient client : clientsCopy) {
            // Check if this client has new entries available
            Map<String, List<StreamEntry>> results = new java.util.LinkedHashMap<>();
            boolean hasNewEntries = false;
            
            for (int i = 0; i < client.streamKeys.size(); i++) {
                String key = client.streamKeys.get(i);
                String lastId = client.lastIds.get(key);
                List<StreamEntry> newEntries = getEntriesAfter(key, lastId);
                results.put(key, newEntries);
                if (!newEntries.isEmpty()) {
                    hasNewEntries = true;
                }
            }
            
            if (hasNewEntries) {
                // Unblock this client from all streams
                unblockClient(client);
                
                // Send the response
                try {
                    String response = RESPProtocol.formatXreadMultiResponse(results);
                    client.outputStream.write(response.getBytes());
                    client.outputStream.flush();
                    System.out.println("Client " + client.clientId + " - XREAD BLOCK unblocked with new entries");
                } catch (Exception e) {
                    System.out.println("Client " + client.clientId + " - IOException during XREAD BLOCK response: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Gets the number of blocked clients waiting on a specific stream.
     * 
     * @param streamKey The stream key
     * @return The number of blocked clients
     */
    public int getBlockedClientCount(String streamKey) {
        List<BlockedClient> clients = blockedClients.get(streamKey);
        return clients != null ? clients.size() : 0;
    }
}
