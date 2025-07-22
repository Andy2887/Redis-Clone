import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for handling Redis Serialization Protocol (RESP) parsing and formatting.
 * Provides methods for both parsing incoming RESP commands and formatting outgoing responses.
 */
public class RESPProtocol {
    
    // RESP data type prefixes
    private static final String SIMPLE_STRING_PREFIX = "+";
    private static final String ERROR_PREFIX = "-";
    private static final String INTEGER_PREFIX = ":";
    private static final String BULK_STRING_PREFIX = "$";
    private static final String ARRAY_PREFIX = "*";
    private static final String CRLF = "\r\n";
    
    // Common responses
    public static final String NULL_BULK_STRING = "$-1\r\n";
    public static final String EMPTY_ARRAY = "*0\r\n";
    public static final String OK_RESPONSE = "+OK\r\n";
    public static final String PONG_RESPONSE = "+PONG\r\n";
    
    /**
     * Parses a RESP array command from the BufferedReader.
     * 
     * @param reader the BufferedReader to read from
     * @param arrayLine the initial array line (e.g., "*3\r\n")
     * @return List of command elements, or null if parsing failed
     * @throws IOException if an I/O error occurs
     */
    public static List<String> parseRESPArray(BufferedReader reader, String arrayLine) throws IOException {
        // Parse array length from *N\r\n
        try {
            int arrayLength = Integer.parseInt(arrayLine.substring(1));
            List<String> elements = new ArrayList<>();
            
            for (int i = 0; i < arrayLength; i++) {
                // Read bulk string length line ($N\r\n)
                String lengthLine = reader.readLine();
                if (lengthLine == null || !lengthLine.startsWith(BULK_STRING_PREFIX)) {
                    return null;
                }
                
                int stringLength = Integer.parseInt(lengthLine.substring(1));
                
                // Read the actual string content
                String content = reader.readLine();
                if (content == null) {
                    return null;
                }
                
                elements.add(content);
            }
            
            return elements;
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Formats a simple string response (e.g., "+OK\r\n").
     * 
     * @param message the message content
     * @return formatted RESP simple string
     */
    public static String formatSimpleString(String message) {
        return SIMPLE_STRING_PREFIX + message + CRLF;
    }
    
    /**
     * Formats an error response (e.g., "-ERR message\r\n").
     * 
     * @param message the error message
     * @return formatted RESP error string
     */
    public static String formatError(String message) {
        return ERROR_PREFIX + message + CRLF;
    }
    
    /**
     * Formats an integer response (e.g., ":42\r\n").
     * 
     * @param value the integer value
     * @return formatted RESP integer
     */
    public static String formatInteger(long value) {
        return INTEGER_PREFIX + value + CRLF;
    }
    
    /**
     * Formats a bulk string response (e.g., "$5\r\nhello\r\n").
     * Returns null bulk string if content is null.
     * 
     * @param content the string content, or null for null bulk string
     * @return formatted RESP bulk string
     */
    public static String formatBulkString(String content) {
        if (content == null) {
            return NULL_BULK_STRING;
        }
        return BULK_STRING_PREFIX + content.length() + CRLF + content + CRLF;
    }
    
    /**
     * Formats an array response with the given elements.
     * Each element should be a properly formatted RESP string.
     * 
     * @param elements the array elements (already formatted as RESP)
     * @return formatted RESP array
     */
    public static String formatArray(List<String> elements) {
        if (elements == null || elements.isEmpty()) {
            return EMPTY_ARRAY;
        }
        
        StringBuilder response = new StringBuilder();
        response.append(ARRAY_PREFIX).append(elements.size()).append(CRLF);
        
        for (String element : elements) {
            response.append(element);
        }
        
        return response.toString();
    }
    
    /**
     * Formats an array of strings as bulk strings.
     * 
     * @param strings the string values
     * @return formatted RESP array of bulk strings
     */
    public static String formatStringArray(List<String> strings) {
        if (strings == null || strings.isEmpty()) {
            return EMPTY_ARRAY;
        }
        
        StringBuilder response = new StringBuilder();
        response.append(ARRAY_PREFIX).append(strings.size()).append(CRLF);
        
        for (String str : strings) {
            response.append(formatBulkString(str));
        }
        
        return response.toString();
    }
    
    /**
     * Formats a two-element array response [key, value] commonly used in blocking operations.
     * 
     * @param key the first element
     * @param value the second element
     * @return formatted RESP array with two bulk strings
     */
    public static String formatKeyValueArray(String key, String value) {
        StringBuilder response = new StringBuilder();
        response.append("*2").append(CRLF);
        response.append(formatBulkString(key));
        response.append(formatBulkString(value));
        return response.toString();
    }
    
    /**
     * Formats a stream entry as a RESP array.
     * Each entry is an array with 2 elements: [id, fields_array]
     * 
     * @param entryId the stream entry ID
     * @param fields the field-value pairs
     * @return formatted RESP array representing the stream entry
     */
    public static String formatStreamEntry(String entryId, Map<String, String> fields) {
        StringBuilder response = new StringBuilder();
        
        // Each entry is an array with 2 elements: [id, fields_array]
        response.append("*2").append(CRLF);
        
        // Entry ID as bulk string
        response.append(formatBulkString(entryId));
        
        // Fields array
        response.append(ARRAY_PREFIX).append(fields.size() * 2).append(CRLF);
        
        // Add field-value pairs (preserve insertion order)
        for (Map.Entry<String, String> fieldEntry : fields.entrySet()) {
            response.append(formatBulkString(fieldEntry.getKey()));
            response.append(formatBulkString(fieldEntry.getValue()));
        }
        
        return response.toString();
    }
    
    /**
     * Formats multiple stream entries as a RESP array.
     * 
     * @param entries the stream entries to format
     * @return formatted RESP array of stream entries
     */
    public static String formatStreamEntries(List<Map.Entry<String, Map<String, String>>> entries) {
        if (entries == null || entries.isEmpty()) {
            return EMPTY_ARRAY;
        }
        
        StringBuilder response = new StringBuilder();
        response.append(ARRAY_PREFIX).append(entries.size()).append(CRLF);
        
        for (Map.Entry<String, Map<String, String>> entry : entries) {
            response.append(formatStreamEntry(entry.getKey(), entry.getValue()));
        }
        
        return response.toString();
    }
    
    /**
     * Generic method to format stream entries from any object that has getId() and getFields() methods.
     * This method uses reflection-like approach by accepting objects with id and fields properties.
     * 
     * @param entries list of stream entry objects
     * @return formatted RESP array of stream entries
     */
    public static <T> String formatStreamEntryArray(List<T> entries) {
        if (entries == null || entries.isEmpty()) {
            return EMPTY_ARRAY;
        }
        
        StringBuilder response = new StringBuilder();
        response.append(ARRAY_PREFIX).append(entries.size()).append(CRLF);
        
        for (T entry : entries) {
            try {
                // Use reflection to get id and fields
                String id = (String) entry.getClass().getField("id").get(entry);
                @SuppressWarnings("unchecked")
                Map<String, String> fields = (Map<String, String>) entry.getClass().getField("fields").get(entry);
                response.append(formatStreamEntry(id, fields));
            } catch (Exception e) {
                // If reflection fails, skip this entry
                continue;
            }
        }
        
        return response.toString();
    }
    
    /**
     * Validates if a string contains valid RESP format.
     * 
     * @param resp the RESP string to validate
     * @return true if valid RESP format, false otherwise
     */
    public static boolean isValidRESP(String resp) {
        if (resp == null || resp.length() < 3) {
            return false;
        }
        
        char firstChar = resp.charAt(0);
        return (firstChar == '+' || firstChar == '-' || firstChar == ':' || 
                firstChar == '$' || firstChar == '*') && resp.endsWith(CRLF);
    }
    
    /**
     * Gets the appropriate error message for invalid command arguments.
     * 
     * @param commandName the command name
     * @return formatted error response
     */
    public static String getArgumentError(String commandName) {
        return formatError("ERR wrong number of arguments for '" + commandName.toLowerCase() + "' command");
    }
    
    /**
     * Gets the error message for unknown commands.
     * 
     * @param commandName the unknown command name
     * @return formatted error response
     */
    public static String getUnknownCommandError(String commandName) {
        return formatError("ERR unknown command '" + commandName + "'");
    }
    
    /**
     * Gets the error message for invalid integer values.
     * 
     * @return formatted error response
     */
    public static String getInvalidIntegerError() {
        return formatError("ERR value is not an integer or out of range");
    }
    
    /**
     * Gets the error message for out of range values.
     * 
     * @return formatted error response
     */
    public static String getOutOfRangeError() {
        return formatError("ERR value is out of range, must be positive");
    }
}
