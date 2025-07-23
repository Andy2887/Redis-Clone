import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import StorageManager.*;

public class HandleClient implements Runnable {
  private Socket clientSocket;
  private int clientId;
  
  // Storage managers for different data types
  private static final StringStorage stringStorage = new StringStorage();
  private static final ListStorage listStorage = new ListStorage();
  private static final StreamStorage streamStorage = new StreamStorage();
  
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
        
      case "XREAD":
        handleXread(command, outputStream);
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
      
      // Store the key-value pair using StringStorage
      stringStorage.set(key, value, expiryTime);
      
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
      
      // Get value using StringStorage (handles expiry automatically)
      String value = stringStorage.get(key);
      String response = RESPProtocol.formatBulkString(value);
      outputStream.write(response.getBytes());
      
      if (value != null) {
        System.out.println("Client " + clientId + " - GET " + key + " = " + value);
      } else {
        System.out.println("Client " + clientId + " - GET " + key + " = (null/expired)");
      }
    } else {
      outputStream.write(RESPProtocol.getArgumentError("get").getBytes());
      System.out.println("Client " + clientId + " - Sent error: GET missing argument");
    }
  }
  
  private void handleRpush(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 3) {
      String listKey = command.get(1);
      
      // Extract elements to push
      String[] elements = new String[command.size() - 2];
      for (int i = 2; i < command.size(); i++) {
        elements[i - 2] = command.get(i);
      }
      
      // Use ListStorage to push elements
      int listSize = listStorage.rightPush(listKey, elements);
      
      // Return the number of elements in the list as a RESP integer
      String response = RESPProtocol.formatInteger(listSize);
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - RPUSH " + listKey + " added " + elements.length + " elements, list size: " + listSize);
      
      // Notify blocked clients waiting for this list
      notifyBlockedClients(listKey);
    } else {
      outputStream.write(RESPProtocol.getArgumentError("rpush").getBytes());
      System.out.println("Client " + clientId + " - Sent error: RPUSH missing arguments");
    }
  }
  
  private void handleLpush(List<String> command, OutputStream outputStream) throws IOException {
      if (command.size() >= 3) {
          String listKey = command.get(1);
          
          // Extract elements to push
          String[] elements = new String[command.size() - 2];
          for (int i = 2; i < command.size(); i++) {
            elements[i - 2] = command.get(i);
          }
          
          // Use ListStorage to push elements
          int listSize = listStorage.leftPush(listKey, elements);
          
          String response = RESPProtocol.formatInteger(listSize);
          outputStream.write(response.getBytes());
          System.out.println("Client " + clientId + " - LPUSH " + listKey + " added " + elements.length + " elements, list size: " + listSize);
          
          // Notify blocked clients waiting for this list
          notifyBlockedClients(listKey);
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
      
      // Use ListStorage to pop elements
      List<String> removedElements = listStorage.leftPop(listKey, count);
      
      if (removedElements.isEmpty()) {
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
        if (count == 1) {
          // Single element LPOP returns bulk string
          String element = removedElements.get(0);
          String response = RESPProtocol.formatBulkString(element);
          outputStream.write(response.getBytes());
          System.out.println("Client " + clientId + " - LPOP " + listKey + " -> '" + element + "', remaining: " + listStorage.length(listKey));
        } else {
          // Multiple element LPOP returns array
          String response = RESPProtocol.formatStringArray(removedElements);
          outputStream.write(response.getBytes());
          System.out.println("Client " + clientId + " - LPOP " + listKey + " " + count + " -> " + removedElements.size() + " elements: " + removedElements + ", remaining: " + listStorage.length(listKey));
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
      
      // First try to pop immediately if list has elements
      List<String> poppedElements = listStorage.leftPop(listKey, 1);
      if (!poppedElements.isEmpty()) {
        // List has elements, return immediately
        String element = poppedElements.get(0);
        String response = RESPProtocol.formatKeyValueArray(listKey, element);
        outputStream.write(response.getBytes());
        System.out.println("Client " + clientId + " - BLPOP " + listKey + " -> immediate ['" + listKey + "', '" + element + "'], remaining: " + listStorage.length(listKey));
        return;
      }
      
      // List is empty or doesn't exist, need to block the client
      BlockedClient blockedClient = new BlockedClient(clientId, outputStream, listKey, timeoutMs);
      
      // Try to block the client using ListStorage
      boolean wasBlocked = listStorage.blockClient(listKey, blockedClient);
      if (!wasBlocked) {
        // This means elements were added between our check and blocking attempt
        // Try to pop again
        poppedElements = listStorage.leftPop(listKey, 1);
        if (!poppedElements.isEmpty()) {
          String element = poppedElements.get(0);
          String response = RESPProtocol.formatKeyValueArray(listKey, element);
          outputStream.write(response.getBytes());
          System.out.println("Client " + clientId + " - BLPOP " + listKey + " -> immediate ['" + listKey + "', '" + element + "'], remaining: " + listStorage.length(listKey));
          return;
        }
        // If still no elements, try to block again
        wasBlocked = listStorage.blockClient(listKey, blockedClient);
      }
      
      if (wasBlocked) {
        System.out.println("Client " + clientId + " - BLPOP " + listKey + " blocking (timeout: " + timeoutSeconds + "s), queue size: " + listStorage.getBlockedClientCount(listKey) + ", queue order: " + listStorage.getBlockedClientOrder(listKey));
        
        // Start timeout monitoring in a separate thread
        if (timeoutMs > 0) {
          // Capture the blockedClient reference for the timeout thread
          final BlockedClient finalBlockedClient = blockedClient;
          
          new Thread(() -> {
            try {
              Thread.sleep(timeoutMs);
              
              // Check if client is still blocked and remove it
              boolean wasStillBlocked = listStorage.unblockClient(listKey, finalBlockedClient);
              
              if (wasStillBlocked) {
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

        // Use ListStorage to get range
        List<String> elements = listStorage.range(listKey, startIndex, endIndex);
        
        String response = RESPProtocol.formatStringArray(elements);
        outputStream.write(response.getBytes());
        System.out.println("Client " + clientId + " - LRANGE " + listKey + " [" + startIndex + ":" + endIndex + "] -> " + elements.size() + " elements");
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

      // Use ListStorage to get length
      int listLength = listStorage.length(listKey);
      
      // Return the length as a RESP integer using RESPProtocol
      String response = RESPProtocol.formatInteger(listLength);
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - LLEN " + listKey + " -> " + listLength);
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'llen' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: LLEN missing argument");
    }
  }
  
  private void handleType(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() >= 2) {
      String key = command.get(1);

      // Check if it's a string type using StringStorage
      if (stringStorage.exists(key)) {
        outputStream.write(RESPProtocol.formatSimpleString("string").getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> string");
        return;
      }

      // Check if it's a list type using ListStorage
      if (listStorage.exists(key)) {
        outputStream.write(RESPProtocol.formatSimpleString("list").getBytes());
        System.out.println("Client " + clientId + " - TYPE " + key + " -> list");
        return;
      }

      // Check if it's a stream type using StreamStorage
      if (streamStorage.exists(key)) {
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
      
      // Parse field-value pairs
      Map<String, String> fields = new java.util.LinkedHashMap<>();
      for (int i = 3; i < command.size(); i += 2) {
        String fieldName = command.get(i);
        String fieldValue = command.get(i + 1);
        fields.put(fieldName, fieldValue);
      }
      
      try {
        // Use StreamStorage to add entry
        String actualEntryId = streamStorage.addEntry(streamKey, entryId, fields);
        
        // Return the entry ID as a bulk string
        String response = RESPProtocol.formatBulkString(actualEntryId);
        outputStream.write(response.getBytes());
        System.out.println("Client " + clientId + " - XADD " + streamKey + " returned entry ID: " + actualEntryId);
        
      } catch (IllegalArgumentException e) {
        // Validation error from StreamStorage
        outputStream.write(e.getMessage().getBytes());
        System.out.println("Client " + clientId + " - XADD " + streamKey + " validation error: " + e.getMessage());
      }
      
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

      // Use StreamStorage to get range
      List<StreamEntry> matchingEntries = streamStorage.getRange(streamKey, startId, endId);

      // Build RESP array response using RESPProtocol
      String response = RESPProtocol.formatStreamEntryArray(matchingEntries);
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - XRANGE " + streamKey + " [" + startId + ":" + endId + "] -> " + matchingEntries.size() + " entries");
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'xrange' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: XRANGE missing arguments");
    }
  }
  
  private void handleXread(List<String> command, OutputStream outputStream) throws IOException {
    // XREAD streams stream_key entry_id
    // Minimum: XREAD streams stream_key entry_id (4 arguments)
    if (command.size() >= 4 && command.get(1).equalsIgnoreCase("streams")) {
      String streamKey = command.get(2);
      String afterId = command.get(3);
      
      // Use StreamStorage to get entries after the specified ID (exclusive)
      List<StreamEntry> matchingEntries = streamStorage.getEntriesAfter(streamKey, afterId);
      
      // Build RESP array response using RESPProtocol
      String response = RESPProtocol.formatXreadResponse(streamKey, matchingEntries);
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - XREAD streams " + streamKey + " " + afterId + " -> " + matchingEntries.size() + " entries");
      
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'xread' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: XREAD missing arguments or invalid format");
    }
  }
  
  private void handleUnknownCommand(String commandName, OutputStream outputStream) throws IOException {
    String errorMsg = RESPProtocol.formatError("ERR unknown command '" + commandName + "'");
    outputStream.write(errorMsg.getBytes());
    System.out.println("Client " + clientId + " - Sent error: unknown command " + commandName);
  }
  
  private void notifyBlockedClients(String listKey) {
    // Use ListStorage to handle blocked client notification
    ListStorage.BlockedClientResult result = listStorage.popForBlockedClient(listKey);
    
    if (result != null) {
      try {
        // Send response array [listKey, element] using RESPProtocol
        String response = RESPProtocol.formatKeyValueArray(listKey, result.element);
        
        result.client.outputStream.write(response.getBytes());
        result.client.outputStream.flush();
        
        System.out.println("Client " + result.client.clientId + " - BLPOP " + listKey + " unblocked with ['" + listKey + "', '" + result.element + "'], remaining: " + listStorage.length(listKey));
        
      } catch (IOException e) {
        System.out.println("Client " + result.client.clientId + " - IOException during BLPOP response: " + e.getMessage());
        // Re-add the element back to the list since we couldn't send it
        listStorage.leftPush(listKey, result.element);
      }
    }
  }
}
