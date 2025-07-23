package StorageManager;
import java.util.List;

/**
 * Helper class for Redis stream ID operations including generation, validation,
 * comparison, and range checking for XADD and XRANGE commands.
 */
public class StreamIdHelper {
    
    /**
     * Represents a parsed stream ID with milliseconds and sequence components.
     */
    public static class ParsedStreamId {
        public final long milliseconds;
        public final long sequence;
        public final String original;
        
        public ParsedStreamId(long milliseconds, long sequence, String original) {
            this.milliseconds = milliseconds;
            this.sequence = sequence;
            this.original = original;
        }
        
        public String toString() {
            return milliseconds + "-" + sequence;
        }
    }
    
    /**
     * Generates a complete stream ID, handling auto-generation for "*" and "time-*" formats.
     * 
     * @param entryId The input ID (may contain "*" for auto-generation)
     * @param streamKey The stream key to check for existing entries
     * @param stream The existing stream entries (may be null or empty)
     * @return The generated complete stream ID
     */
    public static String generateEntryId(String entryId, String streamKey, List<StreamEntry> stream) {
        // Check if the entire entry ID is "*" (auto-generate both time and sequence)
        if (entryId.equals("*")) {
            long currentTimeMs = System.currentTimeMillis();
            long sequence = getNextSequenceNumber(currentTimeMs, stream);
            return currentTimeMs + "-" + sequence;
        }
        
        // Check if sequence number needs to be auto-generated
        String[] parts = entryId.split("-");
        if (parts.length != 2) {
            return entryId; // Invalid format, let validation handle it
        }
        
        // If sequence part is not "*", return the original ID
        if (!parts[1].equals("*")) {
            return entryId;
        }
        
        // Parse the milliseconds part
        long milliseconds;
        try {
            milliseconds = Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            return entryId; // Invalid format, let validation handle it
        }
        
        // Auto-generate sequence number
        long sequence = getNextSequenceNumber(milliseconds, stream);
        return milliseconds + "-" + sequence;
    }
    
    /**
     * Validates a stream ID format and ensures it's greater than existing entries.
     * 
     * @param entryId The stream ID to validate
     * @param stream The existing stream entries (may be null or empty)
     * @return Error message if validation fails, null if valid
     */
    public static String validateEntryId(String entryId, List<StreamEntry> stream) {
        ParsedStreamId parsed = parseStreamId(entryId);
        if (parsed == null) {
            return RESPProtocol.formatError("ERR Invalid stream ID specified as stream command argument");
        }
        
        // Check if ID is greater than 0-0
        if (parsed.milliseconds == 0 && parsed.sequence == 0) {
            return RESPProtocol.formatError("ERR The ID specified in XADD must be greater than 0-0");
        }
        
        // Check if ID is greater than the last entry
        if (stream != null && !stream.isEmpty()) {
            StreamEntry lastEntry = stream.get(stream.size() - 1);
            ParsedStreamId lastParsed = parseStreamId(lastEntry.id);
            
            if (lastParsed != null && compareStreamIds(parsed, lastParsed) <= 0) {
                return RESPProtocol.formatError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        
        return null; // No validation error
    }
    
    /**
     * Parses a stream ID string into its components.
     * 
     * @param streamId The stream ID string (format: "milliseconds-sequence")
     * @return ParsedStreamId object, or null if invalid format
     */
    public static ParsedStreamId parseStreamId(String streamId) {
        if (streamId == null || streamId.isEmpty()) {
            return null;
        }
        
        String[] parts = streamId.split("-");
        if (parts.length != 2) {
            return null;
        }
        
        try {
            long milliseconds = Long.parseLong(parts[0]);
            long sequence = Long.parseLong(parts[1]);
            return new ParsedStreamId(milliseconds, sequence, streamId);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Compares two parsed stream IDs.
     * 
     * @param id1 First stream ID
     * @param id2 Second stream ID
     * @return Negative if id1 < id2, 0 if equal, positive if id1 > id2
     */
    public static int compareStreamIds(ParsedStreamId id1, ParsedStreamId id2) {
        // Compare milliseconds first
        if (id1.milliseconds != id2.milliseconds) {
            return Long.compare(id1.milliseconds, id2.milliseconds);
        }
        
        // If milliseconds are equal, compare sequence numbers
        return Long.compare(id1.sequence, id2.sequence);
    }
    
    /**
     * Compares two stream ID strings.
     * 
     * @param id1 First stream ID string
     * @param id2 Second stream ID string
     * @return Negative if id1 < id2, 0 if equal, positive if id1 > id2
     */
    public static int compareStreamIds(String id1, String id2) {
        ParsedStreamId parsed1 = parseStreamId(id1);
        ParsedStreamId parsed2 = parseStreamId(id2);
        
        if (parsed1 == null || parsed2 == null) {
            return 0; // Invalid format, treat as equal
        }
        
        return compareStreamIds(parsed1, parsed2);
    }
    
    /**
     * Checks if a stream entry ID falls within the specified range.
     * 
     * @param entryId The entry ID to check
     * @param startId The range start ID ("-" for minimum)
     * @param endId The range end ID ("+" for maximum)
     * @return true if the entry is within range
     */
    public static boolean isEntryInRange(String entryId, String startId, String endId) {
        // Handle special cases for start and end
        String actualStartId = startId.equals("-") ? "0-0" : normalizeStreamId(startId);
        String actualEndId = endId.equals("+") ? Long.MAX_VALUE + "-" + Long.MAX_VALUE : normalizeStreamId(endId);
        
        // Compare entry ID with start and end
        return compareStreamIds(entryId, actualStartId) >= 0 && 
               compareStreamIds(entryId, actualEndId) <= 0;
    }
    
    /**
     * Normalizes a stream ID by adding default sequence number if missing.
     * 
     * @param id The stream ID (may be incomplete)
     * @return Normalized stream ID with both milliseconds and sequence
     */
    public static String normalizeStreamId(String id) {
        if (id == null || id.isEmpty()) {
            return "0-0";
        }
        
        // If ID doesn't contain sequence number, add default sequence (0)
        if (!id.contains("-")) {
            return id + "-0";
        }
        
        return id;
    }
    
    /**
     * Gets the next sequence number for a given milliseconds timestamp.
     * 
     * @param milliseconds The timestamp in milliseconds
     * @param stream The existing stream entries (may be null or empty)
     * @return The next available sequence number
     */
    private static long getNextSequenceNumber(long milliseconds, List<StreamEntry> stream) {
        if (stream == null || stream.isEmpty()) {
            // No existing entries
            return (milliseconds == 0) ? 1 : 0;
        }
        
        // Find the highest sequence number for the given milliseconds
        long maxSequence = -1;
        boolean foundSameTime = false;
        
        for (StreamEntry entry : stream) {
            ParsedStreamId parsed = parseStreamId(entry.id);
            if (parsed != null && parsed.milliseconds == milliseconds) {
                foundSameTime = true;
                maxSequence = Math.max(maxSequence, parsed.sequence);
            }
        }
        
        if (foundSameTime) {
            // Found entries with the same milliseconds, increment the max sequence
            return maxSequence + 1;
        } else {
            // No entries with the same milliseconds
            return (milliseconds == 0) ? 1 : 0;
        }
    }
}
