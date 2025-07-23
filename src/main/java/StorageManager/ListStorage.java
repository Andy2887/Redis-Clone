package StorageManager;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Storage manager for Redis list data type.
 * Handles list operations including LPUSH, RPUSH, LPOP, LRANGE, LLEN, and BLPOP.
 */
public class ListStorage {
    // Storage for lists (key -> list of elements)
    private final Map<String, List<String>> lists = new ConcurrentHashMap<>();
    // Storage for blocked clients waiting for list elements (listKey -> queue of blocked clients)
    private final Map<String, Queue<BlockedClient>> blockedClients = new ConcurrentHashMap<>();
    // Global lock for all list operations to ensure consistency with blocking operations
    private final Object listOperationsLock = new Object();
    
    /**
     * Pushes elements to the left (beginning) of a list.
     * 
     * @param key The list key
     * @param elements The elements to push
     * @return The new size of the list
     */
    public int leftPush(String key, String... elements) {
        synchronized (listOperationsLock) {
            List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
            
            // Insert elements at the beginning (reverse order to maintain command semantics)
            for (String element : elements) {
                list.add(0, element);
            }
            
            // Notify blocked clients waiting for this list
            notifyBlockedClients(key);
            
            return list.size();
        }
    }
    
    /**
     * Pushes elements to the right (end) of a list.
     * 
     * @param key The list key
     * @param elements The elements to push
     * @return The new size of the list
     */
    public int rightPush(String key, String... elements) {
        synchronized (listOperationsLock) {
            List<String> list = lists.computeIfAbsent(key, k -> new ArrayList<>());
            
            // Add elements to the end
            for (String element : elements) {
                list.add(element);
            }
            
            // Notify blocked clients waiting for this list
            notifyBlockedClients(key);
            
            return list.size();
        }
    }
    
    /**
     * Pops elements from the left (beginning) of a list.
     * 
     * @param key The list key
     * @param count The number of elements to pop (default 1)
     * @return The popped elements, or empty list if key doesn't exist or list is empty
     */
    public List<String> leftPop(String key, int count) {
        List<String> list = lists.get(key);
        List<String> result = new ArrayList<>();
        
        if (list == null || list.isEmpty()) {
            return result; // Empty result
        }
        
        synchronized (list) {
            int elementsToRemove = Math.min(count, list.size());
            
            for (int i = 0; i < elementsToRemove; i++) {
                if (!list.isEmpty()) {
                    result.add(list.remove(0));
                }
            }
            
            // Clean up empty list
            if (list.isEmpty()) {
                lists.remove(key);
            }
        }
        
        return result;
    }
    
    /**
     * Gets a range of elements from a list.
     * 
     * @param key The list key
     * @param start The start index (can be negative)
     * @param end The end index (can be negative)
     * @return The elements in the specified range, or empty list if key doesn't exist
     */
    public List<String> range(String key, int start, int end) {
        List<String> list = lists.get(key);
        
        if (list == null) {
            return new ArrayList<>(); // Empty result
        }
        
        synchronized (list) {
            int listSize = list.size();
            
            // Convert negative indexes to positive indexes
            int actualStart = convertNegativeIndex(start, listSize);
            int actualEnd = convertNegativeIndex(end, listSize);
            
            // Handle edge cases
            if (actualStart >= listSize || actualStart > actualEnd || actualEnd < 0) {
                return new ArrayList<>(); // Empty result
            }
            
            // Ensure indexes are within bounds
            actualStart = Math.max(0, actualStart);
            actualEnd = Math.min(actualEnd, listSize - 1);
            
            // Extract elements
            List<String> result = new ArrayList<>();
            for (int i = actualStart; i <= actualEnd; i++) {
                result.add(list.get(i));
            }
            
            return result;
        }
    }
    
    /**
     * Gets the length of a list.
     * 
     * @param key The list key
     * @return The length of the list, or 0 if key doesn't exist
     */
    public int length(String key) {
        List<String> list = lists.get(key);
        
        if (list == null) {
            return 0;
        }
        
        synchronized (list) {
            return list.size();
        }
    }
    
    /**
     * Checks if a list exists and is not empty.
     * 
     * @param key The list key
     * @return true if the list exists and has elements
     */
    public boolean exists(String key) {
        List<String> list = lists.get(key);
        return list != null && !list.isEmpty();
    }
    
    /**
     * Blocks a client waiting for elements on a list.
     * 
     * @param key The list key to wait for
     * @param blockedClient The client to block
     * @return true if client was blocked, false if list has elements (client should pop immediately)
     */
    public boolean blockClient(String key, BlockedClient blockedClient) {
        synchronized (listOperationsLock) {
            List<String> list = lists.get(key);
            
            // Check if list exists and has elements
            if (list != null && !list.isEmpty()) {
                return false; // Client should pop immediately
            }
            
            // Block the client
            Queue<BlockedClient> clientQueue = blockedClients.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>());
            clientQueue.offer(blockedClient);
            
            return true; // Client was blocked
        }
    }
    
    /**
     * Unblocks a client from waiting on a list.
     * 
     * @param key The list key
     * @param blockedClient The client to unblock
     * @return true if the client was found and removed from the queue
     */
    public boolean unblockClient(String key, BlockedClient blockedClient) {
        synchronized (listOperationsLock) {
            Queue<BlockedClient> queue = blockedClients.get(key);
            if (queue != null) {
                boolean removed = queue.remove(blockedClient);
                
                // Clean up empty queue
                if (queue.isEmpty()) {
                    blockedClients.remove(key);
                }
                
                return removed;
            }
            return false;
        }
    }
    
    /**
     * Attempts to pop an element for a blocked client.
     * 
     * @param key The list key
     * @return The popped element and client, or null if no elements or no blocked clients
     */
    public BlockedClientResult popForBlockedClient(String key) {
        synchronized (listOperationsLock) {
            // Get the first blocked client
            Queue<BlockedClient> clientQueue = blockedClients.get(key);
            if (clientQueue == null || clientQueue.isEmpty()) {
                return null;
            }
            
            // Get the list
            List<String> list = lists.get(key);
            if (list == null || list.isEmpty()) {
                return null;
            }
            
            // Pop client and element
            BlockedClient client = clientQueue.poll();
            String element = list.remove(0);
            
            // Clean up empty queue
            if (clientQueue.isEmpty()) {
                blockedClients.remove(key);
            }
            
            // Clean up empty list
            if (list.isEmpty()) {
                lists.remove(key);
            }
            
            return new BlockedClientResult(client, element);
        }
    }
    
    /**
     * Gets the order of blocked clients for a list (for debugging).
     * 
     * @param key The list key
     * @return A string representation of the client queue order
     */
    public String getBlockedClientOrder(String key) {
        Queue<BlockedClient> queue = blockedClients.get(key);
        if (queue == null || queue.isEmpty()) {
            return "[]";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (BlockedClient client : queue) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(client.clientId);
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }
    
    /**
     * Gets the number of blocked clients for a list.
     * 
     * @param key The list key
     * @return The number of blocked clients
     */
    public int getBlockedClientCount(String key) {
        Queue<BlockedClient> queue = blockedClients.get(key);
        return queue != null ? queue.size() : 0;
    }
    
    /**
     * Converts negative index to positive index.
     * 
     * @param index The index (can be negative)
     * @param listSize The size of the list
     * @return The converted positive index
     */
    private int convertNegativeIndex(int index, int listSize) {
        if (index < 0) {
            // Convert negative index to positive: -1 becomes listSize-1, -2 becomes listSize-2, etc.
            int convertedIndex = listSize + index;
            // If negative index is out of range (too negative), treat as 0
            return Math.max(0, convertedIndex);
        } else {
            // Positive index, return as is
            return index;
        }
    }
    
    /**
     * Notifies blocked clients when elements are added to a list.
     * This method should be called within the listOperationsLock.
     * 
     * @param key The list key that received new elements
     */
    private void notifyBlockedClients(String key) {
        // This will be called by popForBlockedClient from the HandleClient
        // We don't automatically notify here to give control to the caller
    }
    
    /**
     * Result class for blocked client operations.
     */
    public static class BlockedClientResult {
        public final BlockedClient client;
        public final String element;
        
        public BlockedClientResult(BlockedClient client, String element) {
            this.client = client;
            this.element = element;
        }
    }
}
