package StorageManager;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Storage manager for Redis string data type with expiry support.
 * Handles key-value operations including SET, GET, and expiry management.
 */
public class StringStorage {
    // Storage for key-value pairs
    private final Map<String, String> storage = new ConcurrentHashMap<>();
    // Storage for expiry times (key -> expiry timestamp in milliseconds)
    private final Map<String, Long> expiryTimes = new ConcurrentHashMap<>();
    
    /**
     * Sets a key-value pair with optional expiry time.
     * 
     * @param key The key to set
     * @param value The value to set
     * @param expiryTimeMs Optional expiry time in milliseconds (null for no expiry)
     */
    public void set(String key, String value, Long expiryTimeMs) {
        storage.put(key, value);
        
        if (expiryTimeMs != null) {
            expiryTimes.put(key, expiryTimeMs);
        } else {
            // Remove any existing expiry for this key
            expiryTimes.remove(key);
        }
    }
    
    /**
     * Gets a value by key, checking for expiry.
     * 
     * @param key The key to get
     * @return The value, or null if key doesn't exist or has expired
     */
    public String get(String key) {
        if (isKeyExpired(key)) {
            return null; // Key has expired
        }
        return storage.get(key);
    }
    
    /**
     * Checks if a key exists (and is not expired).
     * 
     * @param key The key to check
     * @return true if key exists and is not expired
     */
    public boolean exists(String key) {
        if (isKeyExpired(key)) {
            return false;
        }
        return storage.containsKey(key);
    }
    
    /**
     * Removes a key and its expiry.
     * 
     * @param key The key to remove
     * @return true if the key was removed, false if it didn't exist
     */
    public boolean remove(String key) {
        boolean existed = storage.remove(key) != null;
        expiryTimes.remove(key);
        return existed;
    }
    
    /**
     * Gets the number of keys (excluding expired ones).
     * 
     * @return The number of valid keys
     */
    public int size() {
        // Clean up expired keys and return count
        cleanupExpiredKeys();
        return storage.size();
    }
    
    /**
     * Checks if a key has expired and removes it if so.
     * 
     * @param key The key to check
     * @return true if the key has expired (and was removed)
     */
    private boolean isKeyExpired(String key) {
        Long expiryTime = expiryTimes.get(key);
        if (expiryTime == null) {
            return false; // No expiry set
        }
        
        long currentTime = System.currentTimeMillis();
        if (currentTime >= expiryTime) {
            // Key has expired, remove it from both maps
            storage.remove(key);
            expiryTimes.remove(key);
            return true;
        }
        
        return false;
    }
    
    /**
     * Cleans up all expired keys.
     * This can be called periodically to free memory.
     */
    public void cleanupExpiredKeys() {
        long currentTime = System.currentTimeMillis();
        
        // Find expired keys
        expiryTimes.entrySet().removeIf(entry -> {
            if (currentTime >= entry.getValue()) {
                // Remove from storage as well
                storage.remove(entry.getKey());
                return true;
            }
            return false;
        });
    }
    
    /**
     * Gets the expiry time for a key.
     * 
     * @param key The key to check
     * @return The expiry time in milliseconds, or null if no expiry is set
     */
    public Long getExpiryTime(String key) {
        return expiryTimes.get(key);
    }
    
    /**
     * Sets the expiry time for an existing key.
     * 
     * @param key The key to set expiry for
     * @param expiryTimeMs The expiry time in milliseconds
     * @return true if the key exists and expiry was set
     */
    public boolean setExpiry(String key, long expiryTimeMs) {
        if (storage.containsKey(key) && !isKeyExpired(key)) {
            expiryTimes.put(key, expiryTimeMs);
            return true;
        }
        return false;
    }
    
    /**
     * Removes the expiry for a key, making it persistent.
     * 
     * @param key The key to make persistent
     * @return true if the key exists and expiry was removed
     */
    public boolean removeExpiry(String key) {
        if (storage.containsKey(key) && !isKeyExpired(key)) {
            expiryTimes.remove(key);
            return true;
        }
        return false;
    }
}
