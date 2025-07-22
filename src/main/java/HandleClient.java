import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Queue;

public class HandleClient implements Runnable {
  private Socket clientSocket;
  private int clientId;
  
  // Simple class to represent a stream entry
  private static class StreamEntry {
    public final String id;
    public final Map<String, String> fields;
    
    public StreamEntry(String id, Map<String, String> fields) {
      this.id = id;
      this.fields = fields;
    }
  }
  
  // Shared storage for all clients - using ConcurrentHashMap for thread safety
  private static final Map<String, String> storage = new ConcurrentHashMap<>();
  // Storage for expiry times (key -> expiry timestamp in milliseconds)
  private static final Map<String, Long> expiryTimes = new ConcurrentHashMap<>();
  // Storage for lists (key -> list of elements)
  private static final Map<String, List<String>> lists = new ConcurrentHashMap<>();
  // Storage for streams (key -> list of stream entries)
  private static final Map<String, List<StreamEntry>> streams = new ConcurrentHashMap<>();
  // Storage for blocked clients waiting for list elements (listKey -> queue of blocked clients)
  private static final Map<String, Queue<BlockedClient>> blockedClients = new ConcurrentHashMap<>();
  // A global lock for all list operations
  private static final Object listOperationsLock = new Object();
  
  public HandleClient(Socket clientSocket, int clientId) {
    this.clientSocket = clientSocket;
    this.clientId = clientId;
  }
  
  @Override
  public void run() {
    System.out.println("Client " + clientId + " connected: " + clientSocket.getRemoteSocketAddress());
    
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      OutputStream outputStream = clientSocket.getOutputStream();
      
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println("Client " + clientId + " - Raw line received: " + line);
        
        // Parse RESP array command
        if (line.startsWith("*")) {
          List<String> command = RESPProtocol.parseRESPArray(reader, line);
          if (command != null && !command.isEmpty()) {
            System.out.println("Client " + clientId + " - Parsed command: " + command);
            handleCommand(command, outputStream);
          }
        }
      }
      
      System.out.println("Client " + clientId + " disconnected");
    } catch (IOException e) {
      System.out.println("Client " + clientId + " - IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
          System.out.println("Client " + clientId + " - Socket closed");
        }
      } catch (IOException e) {
        System.out.println("Client " + clientId + " - IOException during cleanup: " + e.getMessage());
      }
    }
  }
  
  
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
  
  private void handleCommand(List<String> command, OutputStream outputStream) throws IOException {
    String commandName = command.get(0).toUpperCase();
    
    switch (commandName) {
      case "PING":
        handlePing(outputStream);
        break;
        
      case "ECHO":
        handleEcho(command, outputStream);
        break;
        
      case "SET":
        handleSet(command, outputStream);
        break;
        
      case "GET":
        handleGet(command, outputStream);
        break;
        
      case "RPUSH":
        handleRpush(command, outputStream);
        break;
        
      case "LPUSH":
        handleLpush(command, outputStream);
        break;
        
      case "LPOP":
        handleLpop(command, outputStream);
        break;
        
      case "BLPOP":
        handleBlpop(command, outputStream);
        break;
        
      case "LRANGE":
        handleLrange(command, outputStream);
        break;
        
      case "LLEN":
        handleLlen(command, outputStream);
        break;
        
      case "TYPE":
        handleType(command, outputStream);
        break;
        
      case "XADD":
        handleXadd(command, outputStream);
        break;
        
      case "XRANGE":
        handleXrange(command, outputStream);
        break;
        
      default:
        handleUnknownCommand(commandName, outputStream);
        break;
    }
    
    outputStream.flush();
  }
  
  private void handlePing(OutputStream outputStream) throws IOException {
    outputStream.write(RESPProtocol.PONG_RESPONSE.getBytes());
    System.out.println("Client " + clientId + " - Sent: +PONG");
  }
  
  private void handleEcho(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() > 1) {
      String echoArg = command.get(1);
      String response = RESPProtocol.formatBulkString(echoArg);
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - Sent ECHO response: " + echoArg);
    } else {
      outputStream.write(RESPProtocol.getArgumentError("echo").getBytes());
      System.out.println("Client " + clientId + " - Sent error: ECHO missing argument");
    }
  }
  
  private void handleSet(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 3) {
      String key = command.get(1);
      String value = command.get(2);
      
      // Check for PX option (expiry in milliseconds)
      Long expiryTime = null;
      if (command.size() >= 5) {
        for (int i = 3; i < command.size() - 1; i++) {
          if (command.get(i).toUpperCase().equals("PX")) {
            try {
              long expiryMs = Long.parseLong(command.get(i + 1));
              expiryTime = System.currentTimeMillis() + expiryMs;
              System.out.println("Client " + clientId + " - SET " + key + " with expiry in " + expiryMs + "ms");
              break;
            } catch (NumberFormatException e) {
              outputStream.write(RESPProtocol.formatError("ERR invalid expire time in set").getBytes());
              System.out.println("Client " + clientId + " - Sent error: invalid PX value");
              return;
            }
          }
        }
      }
      
      // Store the key-value pair
      storage.put(key, value);
      
      // Store expiry time if specified
      if (expiryTime != null) {
        expiryTimes.put(key, expiryTime);
      } else {
        // Remove any existing expiry for this key
        expiryTimes.remove(key);
      }
      
      outputStream.write(RESPProtocol.OK_RESPONSE.getBytes());
      System.out.println("Client " + clientId + " - SET " + key + " = " + value);
    } else {
      outputStream.write(RESPProtocol.getArgumentError("set").getBytes());
      System.out.println("Client " + clientId + " - Sent error: SET missing arguments");
    }
  }
  
  private void handleGet(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 2) {
      String key = command.get(1);
      
      // Check if key has expired
      if (isKeyExpired(key)) {
        outputStream.write(RESPProtocol.NULL_BULK_STRING.getBytes());
        System.out.println("Client " + clientId + " - GET " + key + " = (expired)");
      } else {
        String value = storage.get(key);
        String response = RESPProtocol.formatBulkString(value);
        outputStream.write(response.getBytes());
        if (value != null) {
          System.out.println("Client " + clientId + " - GET " + key + " = " + value);
        } else {
          System.out.println("Client " + clientId + " - GET " + key + " = (null)");
        }
      }
    } else {
      outputStream.write(RESPProtocol.getArgumentError("get").getBytes());
      System.out.println("Client " + clientId + " - Sent error: GET missing argument");
    }
  }
  
  private void handleRpush(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 3) {
      String listKey = command.get(1);
      
      synchronized (listOperationsLock) {
        // Get or create the list
        List<String> list = lists.computeIfAbsent(listKey, k -> new ArrayList<>());
        
        // Add all elements to the end of the list
        // Process all elements from index 2 onwards
        for (int i = 2; i < command.size(); i++) {
          String element = command.get(i);
          list.add(element);
          System.out.println("Client " + clientId + " - RPUSH " + listKey + " added '" + element + "'");
        }
        
        int listSize = list.size();
        int elementsAdded = command.size() - 2;
        
        // Return the number of elements in the list as a RESP integer
        String response = RESPProtocol.formatInteger(listSize);
        outputStream.write(response.getBytes());
        System.out.println("Client " + clientId + " - RPUSH " + listKey + " added " + elementsAdded + " elements, list size: " + listSize);
        
        // Notify blocked clients waiting for this list
        notifyBlockedClients(listKey);
      }
    } else {
      outputStream.write(RESPProtocol.getArgumentError("rpush").getBytes());
      System.out.println("Client " + clientId + " - Sent error: RPUSH missing arguments");
    }
  }
  
  private void handleLpush(List<String> command, OutputStream outputStream) throws IOException {
      if (command.size() >= 3) {
          String listKey = command.get(1);
          
          synchronized (listOperationsLock) { // Use the global lock like RPUSH
              // Get or create the list
              List<String> list = lists.computeIfAbsent(listKey, k -> new ArrayList<>());
              
              // Process elements from index 2 onwards, inserting each at position 0
              for (int i = 2; i < command.size(); i++) {
                  String element = command.get(i);
                  list.add(0, element);
                  System.out.println("Client " + clientId + " - LPUSH " + listKey + " prepended '" + element + "'");
              }
              
              int listSize = list.size();
              int elementsAdded = command.size() - 2;
              
              String response = RESPProtocol.formatInteger(listSize);
              outputStream.write(response.getBytes());
              System.out.println("Client " + clientId + " - LPUSH " + listKey + " added " + elementsAdded + " elements, list size: " + listSize);
              
              // Notify blocked clients waiting for this list
              notifyBlockedClients(listKey);
          }
      } else {
          outputStream.write(RESPProtocol.getArgumentError("lpush").getBytes());
          System.out.println("Client " + clientId + " - Sent error: LPUSH missing arguments");
      }
  }
  
  private void handleLpop(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 2) {
      String listKey = command.get(1);
      
      // Parse count argument if provided (default is 1)
      int count = 1;
      if (command.size() >= 3) {
        try {
          count = Integer.parseInt(command.get(2));
          if (count < 0) {
            outputStream.write(RESPProtocol.getOutOfRangeError().getBytes());
            System.out.println("Client " + clientId + " - Sent error: LPOP negative count");
            return;
          }
        } catch (NumberFormatException e) {
          outputStream.write(RESPProtocol.getInvalidIntegerError().getBytes());
          System.out.println("Client " + clientId + " - Sent error: LPOP invalid count format");
          return;
        }
      }
      
      List<String> list = lists.get(listKey);
      
      if (list == null || list.isEmpty()) {
        if (count == 1) {
          // Single element LPOP on empty/non-existent list returns null bulk string
          outputStream.write(RESPProtocol.NULL_BULK_STRING.getBytes());
          System.out.println("Client " + clientId + " - LPOP " + listKey + " (empty/non-existent) -> null");
        } else {
          // Multiple element LPOP on empty/non-existent list returns empty array
          outputStream.write(RESPProtocol.EMPTY_ARRAY.getBytes());
          System.out.println("Client " + clientId + " - LPOP " + listKey + " " + count + " (empty/non-existent) -> empty array");
        }
      } else {
        synchronized (list) {
          if (list.isEmpty()) {
            if (count == 1) {
              // Single element LPOP on empty list returns null bulk string
              outputStream.write(RESPProtocol.NULL_BULK_STRING.getBytes());
              System.out.println("Client " + clientId + " - LPOP " + listKey + " (empty) -> null");
            } else {
              // Multiple element LPOP on empty list returns empty array
              outputStream.write(RESPProtocol.EMPTY_ARRAY.getBytes());
              System.out.println("Client " + clientId + " - LPOP " + listKey + " " + count + " (empty) -> empty array");
            }
          } else {
            // Determine how many elements to actually remove
            int elementsToRemove = Math.min(count, list.size());
            List<String> removedElements = new ArrayList<>();
            
            // Remove elements from the beginning of the list
            for (int i = 0; i < elementsToRemove; i++) {
              String element = list.remove(0);
              removedElements.add(element);
            }
            
            if (count == 1) {
              // Single element LPOP returns bulk string
              String element = removedElements.get(0);
              String response = RESPProtocol.formatBulkString(element);
              outputStream.write(response.getBytes());
              System.out.println("Client " + clientId + " - LPOP " + listKey + " -> '" + element + "', remaining: " + list.size());
            } else {
              // Multiple element LPOP returns array
              String response = RESPProtocol.formatStringArray(removedElements);
              outputStream.write(response.getBytes());
              System.out.println("Client " + clientId + " - LPOP " + listKey + " " + count + " -> " + removedElements.size() + " elements: " + removedElements + ", remaining: " + list.size());
            }
            
            // Clean up empty list to save memory
            if (list.isEmpty()) {
              lists.remove(listKey);
              System.out.println("Client " + clientId + " - Removed empty list: " + listKey);
            }
          }
        }
      }
    } else {
      outputStream.write(RESPProtocol.getArgumentError("lpop").getBytes());
      System.out.println("Client " + clientId + " - Sent error: LPOP missing argument");
    }
  }
  
  private void handleBlpop(List<String> command, OutputStream outputStream) throws IOException {
    System.out.println("Started handling BLPOP command for " + clientId);
    if (command.size() >= 3) {
      String listKey = command.get(1);
      
      // Parse timeout argument
      double timeoutSeconds;
      try {
        timeoutSeconds = Double.parseDouble(command.get(2));
        if (timeoutSeconds < 0) {
          outputStream.write(RESPProtocol.formatError("ERR timeout is negative").getBytes());
          System.out.println("Client " + clientId + " - Sent error: BLPOP negative timeout");
          return;
        }
      } catch (NumberFormatException e) {
        outputStream.write(RESPProtocol.formatError("ERR timeout is not a float or out of range").getBytes());
        System.out.println("Client " + clientId + " - Sent error: BLPOP invalid timeout format");
        return;
      }
      
      // Convert timeout to milliseconds (0 means wait indefinitely)
      long timeoutMs = timeoutSeconds == 0 ? 0 : (long) (timeoutSeconds * 1000);
      
      // Declare blockedClient outside the synchronized block
      BlockedClient blockedClient = null;
      
      synchronized (listOperationsLock) {
        List<String> list = lists.get(listKey);
        
        // Check if list exists and has elements
        if (list != null && !list.isEmpty()) {
          // List has elements, pop one immediately
          String element = list.remove(0);
          
          // Return array with [listKey, element]
          String response = RESPProtocol.formatKeyValueArray(listKey, element);
          outputStream.write(response.getBytes());
          System.out.println("Client " + clientId + " - BLPOP " + listKey + " -> immediate ['" + listKey + "', '" + element + "'], remaining: " + list.size());
          
          // Clean up empty list
          if (list.isEmpty()) {
            lists.remove(listKey);
            System.out.println("Client " + clientId + " - Removed empty list: " + listKey);
          }
          return;
        }
        
        // List is empty or doesn't exist, block the client
        blockedClient = new BlockedClient(clientId, outputStream, listKey, timeoutMs);
        
        // Add to blocked clients queue for this list
        Queue<BlockedClient> clientQueue = blockedClients.computeIfAbsent(listKey, k -> new java.util.concurrent.ConcurrentLinkedQueue<>());
        clientQueue.offer(blockedClient);
        System.out.println("Client " + clientId + " - BLPOP " + listKey + " blocking (timeout: " + timeoutSeconds + "s), queue size: " + clientQueue.size() + ", queue order: " + getQueueOrder(clientQueue));
      }
      
      // Start timeout monitoring in a separate thread (outside the synchronized block)
      if (timeoutMs > 0) {
        // Capture the blockedClient reference for the timeout thread
        final BlockedClient finalBlockedClient = blockedClient;
        
        new Thread(() -> {
          try {
            Thread.sleep(timeoutMs);
            
            // Check if client is still blocked
            boolean wasBlocked = false;
            synchronized (listOperationsLock) {
              Queue<BlockedClient> queue = blockedClients.get(listKey);
              if (queue != null) {
                wasBlocked = queue.remove(finalBlockedClient);
                // Clean up empty queue
                if (queue.isEmpty()) {
                  blockedClients.remove(listKey);
                }
              }
            }
            
            if (wasBlocked) {
              // Client timed out, send null response
              try {
                outputStream.write(RESPProtocol.NULL_BULK_STRING.getBytes());
                outputStream.flush();
                System.out.println("Client " + clientId + " - BLPOP " + listKey + " timed out");
              } catch (IOException e) {
                System.out.println("Client " + clientId + " - IOException during timeout response: " + e.getMessage());
              }
            }
          } catch (InterruptedException e) {
            System.out.println("Client " + clientId + " - BLPOP timeout thread interrupted");
          }
        }).start();
      }
      
    } else {
      outputStream.write(RESPProtocol.getArgumentError("blpop").getBytes());
      System.out.println("Client " + clientId + " - Sent error: BLPOP missing arguments");
    }
  }
  
  private void handleLrange(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 4) {
      String listKey = command.get(1);
      try {
        int startIndex = Integer.parseInt(command.get(2));
        int endIndex = Integer.parseInt(command.get(3));

        List<String> list = lists.get(listKey);

        // If list doesn't exist, return empty array
        if (list == null) {
          outputStream.write(RESPProtocol.EMPTY_ARRAY.getBytes());
          System.out.println("Client " + clientId + " - LRANGE " + listKey + " (non-existent) -> empty array");
        } else {
          synchronized (list) {
            int listSize = list.size();

            // Convert negative indexes to positive indexes
            int actualStartIndex = convertNegativeIndex(startIndex, listSize);
            int actualEndIndex = convertNegativeIndex(endIndex, listSize);

            System.out.println("Client " + clientId + " - LRANGE " + listKey + " original[" + startIndex + ":" + endIndex + "] -> actual[" + actualStartIndex + ":" + actualEndIndex + "], list size: " + listSize);

            // Handle edge cases
            if (actualStartIndex >= listSize || actualStartIndex > actualEndIndex || actualEndIndex < 0) {
              // Start index out of bounds or start > end or end < 0 -> empty array
              outputStream.write(RESPProtocol.EMPTY_ARRAY.getBytes());
              System.out.println("Client " + clientId + " - LRANGE " + listKey + " -> empty array (out of bounds)");
            } else {
              // Ensure indexes are within bounds
              actualStartIndex = Math.max(0, actualStartIndex);
              actualEndIndex = Math.min(actualEndIndex, listSize - 1);

              // Calculate number of elements to return
              int numElements = actualEndIndex - actualStartIndex + 1;

              // Build RESP array response using RESPProtocol
              List<String> elements = new ArrayList<>();
              for (int i = actualStartIndex; i <= actualEndIndex; i++) {
                elements.add(list.get(i));
              }
              String response = RESPProtocol.formatStringArray(elements);
              outputStream.write(response.getBytes());
              System.out.println("Client " + clientId + " - LRANGE " + listKey + " [" + actualStartIndex + ":" + actualEndIndex + "] -> " + numElements + " elements");
            }
          }
        }
      } catch (NumberFormatException e) {
        outputStream.write(RESPProtocol.formatError("ERR value is not an integer or out of range").getBytes());
        System.out.println("Client " + clientId + " - Sent error: LRANGE invalid index format");
      }
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'lrange' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: LRANGE missing arguments");
    }
  }
  
  private void handleLlen(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 2) {
      String listKey = command.get(1);

      List<String> list = lists.get(listKey);

      int listLength;
      if (list == null) {
        // Non-existent list has length 0
        listLength = 0;
        System.out.println("Client " + clientId + " - LLEN " + listKey + " (non-existent) -> 0");
      } else {
        synchronized (list) {
          listLength = list.size();
          System.out.println("Client " + clientId + " - LLEN " + listKey + " -> " + listLength);
        }
      }

      // Return the length as a RESP integer using RESPProtocol
      String response = RESPProtocol.formatInteger(listLength);
      outputStream.write(response.getBytes());
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'llen' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: LLEN missing argument");
    }
  }
  
  private void handleType(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 2) {
      String key = command.get(1);

      // Check if key has expired first
      if (isKeyExpired(key)) {
        // Key has expired, treat as non-existent
        outputStream.write(RESPProtocol.formatSimpleString("none").getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> none (expired)");
        return;
      }

      // Check if it's a string type (stored in the main storage map)
      if (storage.containsKey(key)) {
        outputStream.write(RESPProtocol.formatSimpleString("string").getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> string");
        return;
      }

      // Check if it's a list type
      List<String> list = lists.get(key);
      if (list != null && !list.isEmpty()) {
        outputStream.write(RESPProtocol.formatSimpleString("list").getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> list");
        return;
      }

      // Check if it's a stream type
      List<StreamEntry> stream = streams.get(key);
      if (stream != null && !stream.isEmpty()) {
        outputStream.write(RESPProtocol.formatSimpleString("stream").getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> stream");
        return;
      }

      // Key doesn't exist
      outputStream.write(RESPProtocol.formatSimpleString("none").getBytes());
      System.out.println("Client " + clientId + " - TYPE " + key + " -> none");
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'type' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: TYPE missing argument");
    }
  }
  
  private void handleXadd(List<String> command, OutputStream outputStream) throws IOException {
    // XADD stream_key entry_id field1 value1 [field2 value2 ...]
    // Minimum: XADD stream_key entry_id field1 value1 (5 arguments)
    if (command.size() >= 5 && (command.size() % 2) == 1) {
      String streamKey = command.get(1);
      String entryId = command.get(2);
      
      // Auto-generate sequence number if needed
      String actualEntryId = generateEntryId(entryId, streamKey);
      
      // Validate entry ID format and value
      String validationError = validateEntryId(actualEntryId, streamKey);
      if (validationError != null) {
        outputStream.write(validationError.getBytes());
        System.out.println("Client " + clientId + " - XADD " + streamKey + " validation error: " + actualEntryId);
        return;
      }
      
      // Parse field-value pairs
      Map<String, String> fields = new java.util.LinkedHashMap<>();
      for (int i = 3; i < command.size(); i += 2) {
        String fieldName = command.get(i);
        String fieldValue = command.get(i + 1);
        fields.put(fieldName, fieldValue);
      }
      
      // Create stream entry
      StreamEntry entry = new StreamEntry(actualEntryId, fields);
      
      // Get or create the stream
      List<StreamEntry> stream = streams.computeIfAbsent(streamKey, k -> new ArrayList<>());
      
      // Add entry to stream
      synchronized (stream) {
        stream.add(entry);
        System.out.println("Client " + clientId + " - XADD " + streamKey + " " + actualEntryId + " -> " + fields.size() + " fields");
      }
      
      // Return the entry ID as a bulk string
      String response = RESPProtocol.formatBulkString(actualEntryId);
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - XADD " + streamKey + " returned entry ID: " + actualEntryId);
      
    } else {
      if (command.size() < 5) {
        outputStream.write(RESPProtocol.getArgumentError("xadd").getBytes());
        System.out.println("Client " + clientId + " - Sent error: XADD missing arguments");
      } else {
        outputStream.write(RESPProtocol.getArgumentError("xadd").getBytes());
        System.out.println("Client " + clientId + " - Sent error: XADD odd number of field-value pairs");
      }
    }
  }
  
  private void handleXrange(List<String> command, OutputStream outputStream) throws IOException {
    // XRANGE stream_key start end
    if (command.size() >= 4) {
      String streamKey = command.get(1);
      String startId = command.get(2);
      String endId = command.get(3);

      // Get the stream
      List<StreamEntry> stream = streams.get(streamKey);

      if (stream == null || stream.isEmpty()) {
        // Stream doesn't exist or is empty, return empty array
        outputStream.write(RESPProtocol.EMPTY_ARRAY.getBytes());
        System.out.println("Client " + clientId + " - XRANGE " + streamKey + " (empty/non-existent) -> empty array");
        return;
      }

      synchronized (stream) {
        // Filter entries based on start and end IDs
        List<StreamEntry> matchingEntries = new ArrayList<>();

        for (StreamEntry entry : stream) {
          if (isEntryInRange(entry.id, startId, endId)) {
            matchingEntries.add(entry);
          }
        }

        // Build RESP array response using RESPProtocol
        String response = RESPProtocol.formatStreamEntryArray(matchingEntries);
        outputStream.write(response.getBytes());
        System.out.println("Client " + clientId + " - XRANGE " + streamKey + " [" + startId + ":" + endId + "] -> " + matchingEntries.size() + " entries");
      }
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'xrange' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: XRANGE missing arguments");
    }
  }
  
  private String validateEntryId(String entryId, String streamKey) {
    // Parse the entry ID (format: milliseconds-sequence)
    String[] parts = entryId.split("-");
    if (parts.length != 2) {
      return RESPProtocol.formatError("ERR Invalid stream ID specified as stream command argument");
    }
    
    long milliseconds;
    long sequence;
    try {
      milliseconds = Long.parseLong(parts[0]);
      sequence = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
      return RESPProtocol.formatError("ERR Invalid stream ID specified as stream command argument");
    }
    
    // Check if ID is greater than 0-0
    if (milliseconds == 0 && sequence == 0) {
      return RESPProtocol.formatError("ERR The ID specified in XADD must be greater than 0-0");
    }
    
    // Get the stream to check the last entry
    List<StreamEntry> stream = streams.get(streamKey);
    if (stream != null && !stream.isEmpty()) {
      synchronized (stream) {
        if (!stream.isEmpty()) {
          // Get the last entry ID
          StreamEntry lastEntry = stream.get(stream.size() - 1);
          String lastId = lastEntry.id;
          String[] lastParts = lastId.split("-");
          
          if (lastParts.length == 2) {
            try {
              long lastMilliseconds = Long.parseLong(lastParts[0]);
              long lastSequence = Long.parseLong(lastParts[1]);
              
              // Check if new ID is greater than last ID
              if (milliseconds < lastMilliseconds || 
                  (milliseconds == lastMilliseconds && sequence <= lastSequence)) {
                return RESPProtocol.formatError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
              }
            } catch (NumberFormatException e) {
              // If we can't parse the last entry ID, allow the new entry
            }
          }
        }
      }
    }
    
    return null; // No validation error
  }
  
  private String generateEntryId(String entryId, String streamKey) {
    // Check if the entire entry ID is "*" (auto-generate both time and sequence)
    if (entryId.equals("*")) {
      long currentTimeMs = System.currentTimeMillis();
      long sequence = getNextSequenceNumber(currentTimeMs, streamKey);
      
      String generatedId = currentTimeMs + "-" + sequence;
      System.out.println("Client " + clientId + " - Generated entry ID: " + entryId + " -> " + generatedId);
      return generatedId;
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
    long sequence = getNextSequenceNumber(milliseconds, streamKey);
    
    String generatedId = milliseconds + "-" + sequence;
    System.out.println("Client " + clientId + " - Generated entry ID: " + entryId + " -> " + generatedId);
    return generatedId;
  }
  
  private long getNextSequenceNumber(long milliseconds, String streamKey) {
    // Get the stream to find the last sequence number for this milliseconds value
    List<StreamEntry> stream = streams.get(streamKey);
    
    if (stream == null || stream.isEmpty()) {
      // No existing entries
      if (milliseconds == 0) {
        return 1; // Special case: for time 0, default sequence is 1
      } else {
        return 0; // For other times, default sequence is 0
      }
    }
    
    synchronized (stream) {
      // Find the highest sequence number for the given milliseconds
      long maxSequence = -1;
      boolean foundSameTime = false;
      
      for (StreamEntry entry : stream) {
        String[] entryParts = entry.id.split("-");
        if (entryParts.length == 2) {
          try {
            long entryMilliseconds = Long.parseLong(entryParts[0]);
            long entrySequence = Long.parseLong(entryParts[1]);
            
            if (entryMilliseconds == milliseconds) {
              foundSameTime = true;
              maxSequence = Math.max(maxSequence, entrySequence);
            }
          } catch (NumberFormatException e) {
            // Skip invalid entries
          }
        }
      }
      
      if (foundSameTime) {
        // Found entries with the same milliseconds, increment the max sequence
        return maxSequence + 1;
      } else {
        // No entries with the same milliseconds
        if (milliseconds == 0) {
          return 1; // Special case: for time 0, default sequence is 1
        } else {
          return 0; // For other times, default sequence is 0
        }
      }
    }
  }
  
  private boolean isEntryInRange(String entryId, String startId, String endId) {
    // Handle special cases for start and end
    String actualStartId = startId.equals("-") ? "0-0" : normalizeId(startId);
    String actualEndId = endId.equals("+") ? Long.MAX_VALUE + "-" + Long.MAX_VALUE : normalizeId(endId);
    
    // Compare entry ID with start and end
    return compareIds(entryId, actualStartId) >= 0 && compareIds(entryId, actualEndId) <= 0;
  }
  
  private String normalizeId(String id) {
    // If ID doesn't contain sequence number, add default sequence (0 for start, max for end)
    if (!id.contains("-")) {
      return id + "-0";
    }
    return id;
  }
  
  private int compareIds(String id1, String id2) {
    String[] parts1 = id1.split("-");
    String[] parts2 = id2.split("-");
    
    if (parts1.length != 2 || parts2.length != 2) {
      return 0; // Invalid format, treat as equal
    }
    
    try {
      long milliseconds1 = Long.parseLong(parts1[0]);
      long sequence1 = Long.parseLong(parts1[1]);
      long milliseconds2 = Long.parseLong(parts2[0]);
      long sequence2 = Long.parseLong(parts2[1]);
      
      // Compare milliseconds first
      if (milliseconds1 != milliseconds2) {
        return Long.compare(milliseconds1, milliseconds2);
      }
      
      // If milliseconds are equal, compare sequence numbers
      return Long.compare(sequence1, sequence2);
    } catch (NumberFormatException e) {
      return 0; // Invalid format, treat as equal
    }
  }
  
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
  
  private void handleUnknownCommand(String commandName, OutputStream outputStream) throws IOException {
    String errorMsg = RESPProtocol.formatError("ERR unknown command '" + commandName + "'");
    outputStream.write(errorMsg.getBytes());
    System.out.println("Client " + clientId + " - Sent error: unknown command " + commandName);
  }
  
  private String getQueueOrder(Queue<BlockedClient> queue) {
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
  
  private static void notifyBlockedClients(String listKey) {
    // This method should be called within the listOperationsLock
    BlockedClient blockedClient = null;
    
    // Get the first blocked client
    Queue<BlockedClient> clientQueue = blockedClients.get(listKey);
    if (clientQueue == null || clientQueue.isEmpty()) {
      return;
    }
    blockedClient = clientQueue.poll();
    System.out.println("Notifying blocked client " + blockedClient.clientId + " for list " + listKey + ", remaining queue size: " + clientQueue.size());
    
    // Clean up empty queue
    if (clientQueue.isEmpty()) {
      blockedClients.remove(listKey);
    }
    
    // Get the list to check if it has elements
    List<String> list = lists.get(listKey);
    if (list == null || list.isEmpty()) {
      return;
    }
    
    if (!list.isEmpty()) {
      String element = list.remove(0);
      
      try {
        // Send response array [listKey, element] using RESPProtocol
        String response = RESPProtocol.formatKeyValueArray(listKey, element);
        
        blockedClient.outputStream.write(response.getBytes());
        blockedClient.outputStream.flush();
        
        System.out.println("Client " + blockedClient.clientId + " - BLPOP " + listKey + " unblocked with ['" + listKey + "', '" + element + "'], remaining: " + list.size());
        
        // Clean up empty list
        if (list.isEmpty()) {
          lists.remove(listKey);
          System.out.println("Client " + blockedClient.clientId + " - Removed empty list: " + listKey);
        }
        
      } catch (IOException e) {
        System.out.println("Client " + blockedClient.clientId + " - IOException during BLPOP response: " + e.getMessage());
        // Put the element back at the beginning of the list
        list.add(0, element);
      }
    }
  }
}
