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
          List<String> command = parseRESPArray(reader, line);
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
  
  private List<String> parseRESPArray(BufferedReader reader, String arrayLine) throws IOException {
    // Parse array length from *N\r\n
    int arrayLength = Integer.parseInt(arrayLine.substring(1));
    List<String> elements = new ArrayList<>();
    
    for (int i = 0; i < arrayLength; i++) {
      // Read bulk string length line ($N\r\n)
      String lengthLine = reader.readLine();
      if (lengthLine == null || !lengthLine.startsWith("$")) {
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
        
      default:
        handleUnknownCommand(commandName, outputStream);
        break;
    }
    
    outputStream.flush();
  }
  
  private void handlePing(OutputStream outputStream) throws IOException {
    outputStream.write("+PONG\r\n".getBytes());
    System.out.println("Client " + clientId + " - Sent: +PONG");
  }
  
  private void handleEcho(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() > 1) {
      String echoArg = command.get(1);
      String response = "$" + echoArg.length() + "\r\n" + echoArg + "\r\n";
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - Sent ECHO response: " + echoArg);
    } else {
      outputStream.write("-ERR wrong number of arguments for 'echo' command\r\n".getBytes());
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
              outputStream.write("-ERR invalid expire time in set\r\n".getBytes());
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
      
      outputStream.write("+OK\r\n".getBytes());
      System.out.println("Client " + clientId + " - SET " + key + " = " + value);
    } else {
      outputStream.write("-ERR wrong number of arguments for 'set' command\r\n".getBytes());
      System.out.println("Client " + clientId + " - Sent error: SET missing arguments");
    }
  }
  
  private void handleGet(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 2) {
      String key = command.get(1);
      
      // Check if key has expired
      if (isKeyExpired(key)) {
        outputStream.write("$-1\r\n".getBytes());
        System.out.println("Client " + clientId + " - GET " + key + " = (expired)");
      } else {
        String value = storage.get(key);
        if (value != null) {
          String response = "$" + value.length() + "\r\n" + value + "\r\n";
          outputStream.write(response.getBytes());
          System.out.println("Client " + clientId + " - GET " + key + " = " + value);
        } else {
          // Return null bulk string for non-existent key
          outputStream.write("$-1\r\n".getBytes());
          System.out.println("Client " + clientId + " - GET " + key + " = (null)");
        }
      }
    } else {
      outputStream.write("-ERR wrong number of arguments for 'get' command\r\n".getBytes());
      System.out.println("Client " + clientId + " - Sent error: GET missing argument");
    }
  }
  
  private void handleRpush(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 3) {
      String listKey = command.get(1);
      
      // Get or create the list
      List<String> list = lists.computeIfAbsent(listKey, k -> new ArrayList<>());
      
      // Add all elements to the end of the list
      synchronized (list) {
        // Process all elements from index 2 onwards
        for (int i = 2; i < command.size(); i++) {
          String element = command.get(i);
          list.add(element);
          System.out.println("Client " + clientId + " - RPUSH " + listKey + " added '" + element + "'");
        }
        
        int listSize = list.size();
        int elementsAdded = command.size() - 2;
        
        // Return the number of elements in the list as a RESP integer
        String response = ":" + listSize + "\r\n";
        outputStream.write(response.getBytes());
        System.out.println("Client " + clientId + " - RPUSH " + listKey + " added " + elementsAdded + " elements, list size: " + listSize);
        
        // Notify blocked clients waiting for this list
        notifyBlockedClients(listKey);
      }
    } else {
      outputStream.write("-ERR wrong number of arguments for 'rpush' command\r\n".getBytes());
      System.out.println("Client " + clientId + " - Sent error: RPUSH missing arguments");
    }
  }
  
  private void handleLpush(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 3) {
      String listKey = command.get(1);
      
      // Get or create the list
      List<String> list = lists.computeIfAbsent(listKey, k -> new ArrayList<>());
      
      // Add all elements to the beginning of the list (insert from left to right)
      synchronized (list) {
        // Process elements from index 2 onwards, inserting each at position 0
        // This will result in the elements appearing in reverse order of how they're specified
        for (int i = 2; i < command.size(); i++) {
          String element = command.get(i);
          list.add(0, element); // Insert at the beginning
          System.out.println("Client " + clientId + " - LPUSH " + listKey + " prepended '" + element + "'");
        }
        
        int listSize = list.size();
        int elementsAdded = command.size() - 2;
        
        // Return the number of elements in the list as a RESP integer
        String response = ":" + listSize + "\r\n";
        outputStream.write(response.getBytes());
        System.out.println("Client " + clientId + " - LPUSH " + listKey + " added " + elementsAdded + " elements, list size: " + listSize);
        
        // Notify blocked clients waiting for this list
        notifyBlockedClients(listKey);
      }
    } else {
      outputStream.write("-ERR wrong number of arguments for 'lpush' command\r\n".getBytes());
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
            outputStream.write("-ERR value is out of range, must be positive\r\n".getBytes());
            System.out.println("Client " + clientId + " - Sent error: LPOP negative count");
            return;
          }
        } catch (NumberFormatException e) {
          outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());
          System.out.println("Client " + clientId + " - Sent error: LPOP invalid count format");
          return;
        }
      }
      
      List<String> list = lists.get(listKey);
      
      if (list == null || list.isEmpty()) {
        if (count == 1) {
          // Single element LPOP on empty/non-existent list returns null bulk string
          outputStream.write("$-1\r\n".getBytes());
          System.out.println("Client " + clientId + " - LPOP " + listKey + " (empty/non-existent) -> null");
        } else {
          // Multiple element LPOP on empty/non-existent list returns empty array
          outputStream.write("*0\r\n".getBytes());
          System.out.println("Client " + clientId + " - LPOP " + listKey + " " + count + " (empty/non-existent) -> empty array");
        }
      } else {
        synchronized (list) {
          if (list.isEmpty()) {
            if (count == 1) {
              // Single element LPOP on empty list returns null bulk string
              outputStream.write("$-1\r\n".getBytes());
              System.out.println("Client " + clientId + " - LPOP " + listKey + " (empty) -> null");
            } else {
              // Multiple element LPOP on empty list returns empty array
              outputStream.write("*0\r\n".getBytes());
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
              String response = "$" + element.length() + "\r\n" + element + "\r\n";
              outputStream.write(response.getBytes());
              System.out.println("Client " + clientId + " - LPOP " + listKey + " -> '" + element + "', remaining: " + list.size());
            } else {
              // Multiple element LPOP returns array
              StringBuilder response = new StringBuilder();
              response.append("*").append(removedElements.size()).append("\r\n");
              
              for (String element : removedElements) {
                response.append("$").append(element.length()).append("\r\n");
                response.append(element).append("\r\n");
              }
              
              outputStream.write(response.toString().getBytes());
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
      outputStream.write("-ERR wrong number of arguments for 'lpop' command\r\n".getBytes());
      System.out.println("Client " + clientId + " - Sent error: LPOP missing argument");
    }
  }
  
  private void handleBlpop(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 3) {
      String listKey = command.get(1);
      
      // Parse timeout argument
      double timeoutSeconds;
      try {
        timeoutSeconds = Double.parseDouble(command.get(2));
        if (timeoutSeconds < 0) {
          outputStream.write("-ERR timeout is negative\r\n".getBytes());
          System.out.println("Client " + clientId + " - Sent error: BLPOP negative timeout");
          return;
        }
      } catch (NumberFormatException e) {
        outputStream.write("-ERR timeout is not a float or out of range\r\n".getBytes());
        System.out.println("Client " + clientId + " - Sent error: BLPOP invalid timeout format");
        return;
      }
      
      // Convert timeout to milliseconds (0 means wait indefinitely)
      long timeoutMs = timeoutSeconds == 0 ? 0 : (long) (timeoutSeconds * 1000);
      
      List<String> list = lists.get(listKey);
      
      // Check if list exists and has elements
      if (list != null) {
        synchronized (list) {
          if (!list.isEmpty()) {
            // List has elements, pop one immediately
            String element = list.remove(0);
            
            // Return array with [listKey, element]
            StringBuilder response = new StringBuilder();
            response.append("*2\r\n");
            response.append("$").append(listKey.length()).append("\r\n").append(listKey).append("\r\n");
            response.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
            
            outputStream.write(response.toString().getBytes());
            System.out.println("Client " + clientId + " - BLPOP " + listKey + " -> immediate ['" + listKey + "', '" + element + "'], remaining: " + list.size());
            
            // Clean up empty list
            if (list.isEmpty()) {
              lists.remove(listKey);
              System.out.println("Client " + clientId + " - Removed empty list: " + listKey);
            }
            return;
          }
        }
      }
      
      // List is empty or doesn't exist, block the client
      BlockedClient blockedClient = new BlockedClient(clientId, outputStream, listKey, timeoutMs);
      
      // Add to blocked clients queue for this list with proper synchronization
      synchronized (blockedClients) {
        Queue<BlockedClient> clientQueue = blockedClients.computeIfAbsent(listKey, k -> new java.util.concurrent.ConcurrentLinkedQueue<>());
        clientQueue.offer(blockedClient);
        System.out.println("Client " + clientId + " - BLPOP " + listKey + " blocking (timeout: " + timeoutSeconds + "s), queue size: " + clientQueue.size() + ", queue order: " + getQueueOrder(clientQueue));
      }
      
      // Start timeout monitoring in a separate thread
      if (timeoutMs > 0) {
        new Thread(() -> {
          try {
            Thread.sleep(timeoutMs);
            
            // Check if client is still blocked with proper synchronization
            boolean wasBlocked = false;
            synchronized (blockedClients) {
              Queue<BlockedClient> queue = blockedClients.get(listKey);
              if (queue != null) {
                wasBlocked = queue.remove(blockedClient);
                // Clean up empty queue
                if (queue.isEmpty()) {
                  blockedClients.remove(listKey);
                }
              }
            }
            
            if (wasBlocked) {
              // Client timed out, send null response
              try {
                outputStream.write("$-1\r\n".getBytes());
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
      
      // Note: The actual response will be sent when an element is added to the list
      // or when the timeout expires (handled above)
      
    } else {
      outputStream.write("-ERR wrong number of arguments for 'blpop' command\r\n".getBytes());
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
          outputStream.write("*0\r\n".getBytes());
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
              outputStream.write("*0\r\n".getBytes());
              System.out.println("Client " + clientId + " - LRANGE " + listKey + " -> empty array (out of bounds)");
            } else {
              // Ensure indexes are within bounds
              actualStartIndex = Math.max(0, actualStartIndex);
              actualEndIndex = Math.min(actualEndIndex, listSize - 1);
              
              // Calculate number of elements to return
              int numElements = actualEndIndex - actualStartIndex + 1;
              
              // Build RESP array response
              StringBuilder response = new StringBuilder();
              response.append("*").append(numElements).append("\r\n");
              
              for (int i = actualStartIndex; i <= actualEndIndex; i++) {
                String element = list.get(i);
                response.append("$").append(element.length()).append("\r\n");
                response.append(element).append("\r\n");
              }
              
              outputStream.write(response.toString().getBytes());
              System.out.println("Client " + clientId + " - LRANGE " + listKey + " [" + actualStartIndex + ":" + actualEndIndex + "] -> " + numElements + " elements");
            }
          }
        }
      } catch (NumberFormatException e) {
        outputStream.write("-ERR value is not an integer or out of range\r\n".getBytes());
        System.out.println("Client " + clientId + " - Sent error: LRANGE invalid index format");
      }
    } else {
      outputStream.write("-ERR wrong number of arguments for 'lrange' command\r\n".getBytes());
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
      
      // Return the length as a RESP integer
      String response = ":" + listLength + "\r\n";
      outputStream.write(response.getBytes());
    } else {
      outputStream.write("-ERR wrong number of arguments for 'llen' command\r\n".getBytes());
      System.out.println("Client " + clientId + " - Sent error: LLEN missing argument");
    }
  }
  
  private void handleType(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 2) {
      String key = command.get(1);
      
      // Check if key has expired first
      if (isKeyExpired(key)) {
        // Key has expired, treat as non-existent
        outputStream.write("+none\r\n".getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> none (expired)");
        return;
      }
      
      // Check if it's a string type (stored in the main storage map)
      if (storage.containsKey(key)) {
        outputStream.write("+string\r\n".getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> string");
        return;
      }
      
      // Check if it's a list type
      List<String> list = lists.get(key);
      if (list != null && !list.isEmpty()) {
        outputStream.write("+list\r\n".getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> list");
        return;
      }
      
      // Check if it's a stream type
      List<StreamEntry> stream = streams.get(key);
      if (stream != null && !stream.isEmpty()) {
        outputStream.write("+stream\r\n".getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> stream");
        return;
      }
      
      // Key doesn't exist
      outputStream.write("+none\r\n".getBytes());
      System.out.println("Client " + clientId + " - TYPE " + key + " -> none");
    } else {
      outputStream.write("-ERR wrong number of arguments for 'type' command\r\n".getBytes());
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
      Map<String, String> fields = new ConcurrentHashMap<>();
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
      String response = "$" + actualEntryId.length() + "\r\n" + actualEntryId + "\r\n";
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - XADD " + streamKey + " returned entry ID: " + actualEntryId);
      
    } else {
      if (command.size() < 5) {
        outputStream.write("-ERR wrong number of arguments for 'xadd' command\r\n".getBytes());
        System.out.println("Client " + clientId + " - Sent error: XADD missing arguments");
      } else {
        outputStream.write("-ERR wrong number of arguments for 'xadd' command\r\n".getBytes());
        System.out.println("Client " + clientId + " - Sent error: XADD odd number of field-value pairs");
      }
    }
  }
  
  private String validateEntryId(String entryId, String streamKey) {
    // Parse the entry ID (format: milliseconds-sequence)
    String[] parts = entryId.split("-");
    if (parts.length != 2) {
      return "-ERR Invalid stream ID specified as stream command argument\r\n";
    }
    
    long milliseconds;
    long sequence;
    try {
      milliseconds = Long.parseLong(parts[0]);
      sequence = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
      return "-ERR Invalid stream ID specified as stream command argument\r\n";
    }
    
    // Check if ID is greater than 0-0
    if (milliseconds == 0 && sequence == 0) {
      return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
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
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
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
    String errorMsg = "-ERR unknown command '" + commandName + "'\r\n";
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
  
  private void notifyBlockedClients(String listKey) {
    BlockedClient blockedClient = null;
    
    // Get the first blocked client with proper synchronization
    synchronized (blockedClients) {
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
    }
    
    // Get the list to check if it has elements
    List<String> list = lists.get(listKey);
    if (list == null || list.isEmpty()) {
      return;
    }
    
    synchronized (list) {
      if (!list.isEmpty()) {
        String element = list.remove(0);
        
        try {
          // Send response array [listKey, element]
          StringBuilder response = new StringBuilder();
          response.append("*2\r\n");
          response.append("$").append(listKey.length()).append("\r\n").append(listKey).append("\r\n");
          response.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
          
          blockedClient.outputStream.write(response.toString().getBytes());
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
}
