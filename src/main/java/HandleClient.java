import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import StorageManager.*;
import RdbManager.RdbWriter;

public class HandleClient implements Runnable {
  private Socket clientSocket;
  private int clientId;
  private String serverRole;
  private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  private static final String MASTER_REPL_OFFSET = "0";
  // Track the replica's OutputStream and connection state
  private static final List<OutputStream> replicaOutputStreams = new CopyOnWriteArrayList<>();
  private boolean inTransaction = false;
  private List<List<String>> queuedCommands = new ArrayList<>();
  
  // Storage managers for different data types
  private final StringStorage stringStorage;
  private static final ListStorage listStorage = new ListStorage();
  private static final StreamStorage streamStorage = new StreamStorage();
  
  public HandleClient(Socket clientSocket, int clientId, String serverRole, StringStorage stringStorage) {
      this.clientSocket = clientSocket;
      this.clientId = clientId;
      this.serverRole = serverRole;
      this.stringStorage = stringStorage;
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
  
  
  public void handleCommand(List<String> command, OutputStream outputStream) throws IOException {
    String commandName = command.get(0).toUpperCase();

    // If in transaction and not MULTI/EXEC, queue the command
    if (inTransaction && !commandName.equals("MULTI") && !commandName.equals("EXEC") && !commandName.equals("DISCARD")) {
      queuedCommands.add(command);
      outputStream.write(RESPProtocol.formatSimpleString("QUEUED").getBytes());
      System.out.println("Client " + clientId + " - Queued command: " + command);
      outputStream.flush();
      return;
    }
    
    // List of write commands to propagate
    boolean isWriteCommand = commandName.equals("SET") ||
                             commandName.equals("DEL") ||
                             commandName.equals("RPUSH") ||
                             commandName.equals("LPUSH") ||
                             commandName.equals("LPOP") ||
                             commandName.equals("BLPOP") ||
                             commandName.equals("XADD");

    if (isWriteCommand) {
      propagateToReplica(command);
    }
    
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

      case "INFO":
        handleInfo(command, outputStream);
        break;

      case "REPLCONF":
        handleReplconf(command, outputStream);
        break;
      
      case "REPLICAOF":
        handleReplicaof(command, outputStream);
        break;

      case "PSYNC":
        handlePsync(command, outputStream);
        break;

      case "CONFIG":
        handleConfig(command, outputStream);
        break;

      case "KEYS":
        handleKeys(command, outputStream);
        break;

      case "INCR":
        handleIncr(command, outputStream);
        break;

      case "MULTI":
        handleMulti(command, outputStream);
        break;

      case "EXEC":
        handleExec(command, outputStream);
        break;

      case "DISCARD":
        handleDiscard(command, outputStream);
        break;

      case "SAVE":
        handleSave(command, outputStream);
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

  //
  // Store key-value pairs with optional expiry
  //
  // Syntax:
  // SET key value [PX milliseconds]
  // 
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

  //
  // Retrieve values by key
  //
  // Syntax:
  // GET key
  // 
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

  //
  // Add elements to the right of a list
  //
  // Syntax:
  // RPUSH key element [element ...]
  // 
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

  //
  // Add elements to the left of a list
  //
  // Syntax:
  // LPUSH key element [element ...]
  // 
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

  //
  // Remove and return elements from the left of a list
  //
  // Syntax:
  // LPOP key [count]
  // 
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

  //
  // Blocking left pop with timeout support
  //
  // Syntax:
  // BLPOP key timeout
  // 
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
      
      // Add the client to the blocked queue
      boolean wasBlocked = listStorage.blockClient(listKey, blockedClient);
      
      if (wasBlocked) {
        System.out.println("Client " + clientId + " - BLPOP " + listKey + " is blocked (timeout: " + timeoutSeconds + "s), queue size: " + listStorage.getBlockedClientCount(listKey) + ", queue order: " + listStorage.getBlockedClientOrder(listKey));
        
        // Start timeout monitoring in a separate thread
        if (timeoutMs > 0) {
          // Capture the blockedClient reference for the timeout thread
          final BlockedClient finalBlockedClient = blockedClient;
          
          new Thread(() -> {
            try {
              Thread.sleep(timeoutMs);
              
              // Remove client from blocked queue; true if client was still blocked (not yet unblocked
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

  //
  // Get a range of elements from a list
  //
  // Syntax:
  // LRANGE key start stop
  // 
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

  //
  // Get the length of a list
  //
  // Syntax:
  // LLEN key
  // 
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

  //
  // Check type of a key
  //
  // Syntax:
  // TYPE key
  //
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

  //
  // Add entries to a stream
  //
  // Syntax:
  // XADD key id field value [field value ...]
  //
  private void handleXadd(List<String> command, OutputStream outputStream) throws IOException {
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

  //
  // Get a range of entries from a stream
  //
  // Syntax:
  // XRANGE key start end
  //
  private void handleXrange(List<String> command, OutputStream outputStream) throws IOException {
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

  //
  // Read new entries from one or more streams, optionally blocking
  //
  // Syntax:
  // XREAD [BLOCK timeout] streams key [key ...] id [id ...]
  //
  private void handleXread(List<String> command, OutputStream outputStream) throws IOException {
    int streamsIndex = -1;
    long blockTimeoutMs = -1; // -1 means non-blocking
    
    // Parse optional BLOCK parameter and get streamsIndex
    for (int i = 1; i < command.size(); i++) {
      if (command.get(i).equalsIgnoreCase("block")) {
        if (i + 1 >= command.size()) {
          outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'xread' command").getBytes());
          System.out.println("Client " + clientId + " - Sent error: XREAD BLOCK missing timeout");
          return;
        }
        try {
          long timeoutMs = Long.parseLong(command.get(i + 1));
          if (timeoutMs < 0) {
            outputStream.write(RESPProtocol.formatError("ERR timeout is negative").getBytes());
            System.out.println("Client " + clientId + " - Sent error: XREAD BLOCK negative timeout");
            return;
          }
          blockTimeoutMs = timeoutMs;
          i++; // Skip the timeout value
        } catch (NumberFormatException e) {
          outputStream.write(RESPProtocol.formatError("ERR timeout is not an integer or out of range").getBytes());
          System.out.println("Client " + clientId + " - Sent error: XREAD BLOCK invalid timeout format");
          return;
        }
      } else if (command.get(i).equalsIgnoreCase("streams")) {
        streamsIndex = i;
        break;
      }
    }
    
    final long finalBlockTimeoutMs = blockTimeoutMs;

    System.out.println("Final timeout: " + finalBlockTimeoutMs);
    
    if (streamsIndex == -1) {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'xread' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: XREAD missing 'streams' keyword");
      return;
    }
    
    // Calculate number of streams
    // Total arguments after "streams" should be even (N keys + N ids)
    int argsAfterStreams = command.size() - streamsIndex - 1; // Subtract everything before and including "streams"
    if (argsAfterStreams % 2 != 0 || argsAfterStreams < 2) {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'xread' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: XREAD uneven number of stream keys and IDs");
      return;
    }
    int numStreams = argsAfterStreams / 2;
    
    // Extract stream keys and IDs
    List<String> streamKeys = new ArrayList<>();
    List<String> afterIds = new ArrayList<>();
    
    for (int i = 0; i < numStreams; i++) {
      String streamKey = command.get(streamsIndex + 1 + i); // Stream keys start after "streams"
      String id = command.get(streamsIndex + 1 + numStreams + i); // IDs start after all stream keys

      // If the ID is "$", replace it with the current max ID in the stream (or "0-0" if stream is empty)
      if (id.equals("$")) {
        String lastId = streamStorage.getLastEntryId(streamKey);
        if (lastId == null) {
          id = "0-0";
        } else {
          id = lastId;
        }
      }
      streamKeys.add(streamKey);
      afterIds.add(id);
    }
    
    // Query each stream and collect results
    Map<String, List<StreamEntry>> streamResults = new java.util.LinkedHashMap<>();
    
    for (int i = 0; i < numStreams; i++) {
      String streamKey = streamKeys.get(i);
      String afterId = afterIds.get(i);
      
      // Use StreamStorage to get entries after the specified ID (exclusive)
      List<StreamEntry> matchingEntries = streamStorage.getEntriesAfter(streamKey, afterId);
      streamResults.put(streamKey, matchingEntries);
    }
    
    // Check if we have any new entries
    boolean hasNewEntries = streamResults.values().stream().anyMatch(entries -> !entries.isEmpty());
    
    if (hasNewEntries || finalBlockTimeoutMs == -1) {
      // Non-blocking mode or we have entries, send response immediately
      String response = RESPProtocol.formatXreadMultiResponse(streamResults);
      outputStream.write(response.getBytes());
      
      int totalEntries = streamResults.values().stream().mapToInt(List::size).sum();
      System.out.println("Client " + clientId + " - XREAD" + (finalBlockTimeoutMs != -1 ? " BLOCK" : "") + " streams " + streamKeys + " " + afterIds + " -> " + totalEntries + " total entries (immediate)");
      
    } else {
      // Blocking mode and no entries found, block the client
      Map<String, String> lastIdMap = new java.util.LinkedHashMap<>();
      for (int i = 0; i < streamKeys.size(); i++) {
        lastIdMap.put(streamKeys.get(i), afterIds.get(i));
      }
      
      BlockedClient blockedClient = new BlockedClient(clientId, outputStream, streamKeys, lastIdMap, finalBlockTimeoutMs);
      
      // Try to block the client
      boolean wasBlocked = streamStorage.blockClientOnStreams(blockedClient);
      if (!wasBlocked) {
        // Entries were added between our check and blocking attempt, try again
        for (int i = 0; i < numStreams; i++) {
          String streamKey = streamKeys.get(i);
          String afterId = afterIds.get(i);
          List<StreamEntry> matchingEntries = streamStorage.getEntriesAfter(streamKey, afterId);
          streamResults.put(streamKey, matchingEntries);
        }
        
        String response = RESPProtocol.formatXreadMultiResponse(streamResults);
        outputStream.write(response.getBytes());
        
        int totalEntries = streamResults.values().stream().mapToInt(List::size).sum();
        System.out.println("Client " + clientId + " - XREAD BLOCK streams " + streamKeys + " " + afterIds + " -> " + totalEntries + " total entries (race condition)");
      } else {
        System.out.println("Client " + clientId + " - XREAD BLOCK streams " + streamKeys + " " + afterIds + " blocking (timeout: " + (finalBlockTimeoutMs / 1000.0) + "s)");
        
        // Start timeout monitoring in a separate thread
        if (finalBlockTimeoutMs > 0) {
          final BlockedClient finalBlockedClient = blockedClient;
          
          new Thread(() -> {
            try {
              Thread.sleep(finalBlockTimeoutMs);
              
              // Check if client is still blocked and remove it
              boolean wasStillBlocked = streamStorage.unblockClient(finalBlockedClient);
              
              if (wasStillBlocked) {
                // Client timed out, send null response
                try {
                  outputStream.write(RESPProtocol.NULL_BULK_STRING.getBytes());
                  outputStream.flush();
                  System.out.println("Client " + clientId + " - XREAD BLOCK streams " + streamKeys + " timed out");
                } catch (IOException e) {
                  System.out.println("Client " + clientId + " - IOException during XREAD BLOCK timeout response: " + e.getMessage());
                }
              }
            } catch (InterruptedException e) {
              System.out.println("Client " + clientId + " - XREAD BLOCK timeout thread interrupted");
            }
          }).start();
        }
      }
    }
  }

  private void handleInfo(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() == 2 && command.get(1).equalsIgnoreCase("replication")) {
      StringBuilder info = new StringBuilder();
      info.append("role:").append(serverRole).append("\r\n");
      if ("master".equals(serverRole)) {
        info.append("master_replid:").append(MASTER_REPLID).append("\r\n");
        info.append("master_repl_offset:").append(MASTER_REPL_OFFSET).append("\r\n");
      }
      String response = RESPProtocol.formatBulkString(info.toString());
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - INFO replication ->\n" + info);
    } else {
      outputStream.write(RESPProtocol.formatError("ERR only INFO replication is supported").getBytes());
      System.out.println("Client " + clientId + " - Sent error: INFO only supports replication section");
    }
  }

  private void handleReplconf(List<String> command, OutputStream outputStream) throws IOException {
    outputStream.write(RESPProtocol.OK_RESPONSE.getBytes());
    System.out.println("Client " + clientId + " - REPLCONF received, responded with +OK");
  }

  //
  // Turn the server into master
  //
  // Syntax:
  // REPLICAOF NO ONE
  //
  private void handleReplicaof(List<String> command, OutputStream outputStream) throws IOException {
    // Note: The original REPLICAOF command has two options: 1, turn the server into master. 2, set the current server to be replica of a server
    // Currently, only 1 is implemented in my code
    if (command.size() == 3 && command.get(1).equalsIgnoreCase("NO") && command.get(2).equalsIgnoreCase("ONE")) {
        // Become master
        Main.serverRole = "master";
        // Optionally: stop any ongoing replication threads/connections here
        outputStream.write(RESPProtocol.OK_RESPONSE.getBytes());
        System.out.println("Client " + clientId + " - REPLICAOF NO ONE: Now acting as master");
    } else {
        outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'replicaof' command").getBytes());
    }
  }

  private void handlePsync(List<String> command, OutputStream outputStream) throws IOException {
      // Always respond with FULLRESYNC <REPL_ID> 0
      String replid = MASTER_REPLID;
      String offset = MASTER_REPL_OFFSET;
      String response = "+FULLRESYNC " + replid + " " + offset + "\r\n";
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - PSYNC received, responded with: " + response.trim());

      // Send RDB file as a bulk string (no trailing \r\n after binary)
      RdbWriter writer = new RdbWriter(stringStorage);
      byte[] rdbFileBytes = writer.serializeToRdb();
      String bulkHeader = "$" + rdbFileBytes.length + "\r\n";
      outputStream.write(bulkHeader.getBytes());
      outputStream.write(rdbFileBytes); // No trailing \r\n
      outputStream.flush();
      System.out.println("Client " + clientId + " - Sent RDB file (" + rdbFileBytes.length + " bytes)");

      // Add this replica's OutputStream to the list
      synchronized (HandleClient.class) {
        replicaOutputStreams.add(outputStream);
      }
  }

  private void handleConfig(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() == 3 && command.get(1).equalsIgnoreCase("GET")) {
      String param = command.get(2);
      String value = null;
      if (param.equalsIgnoreCase("dir")) {
        value = Main.dir;
      } else if (param.equalsIgnoreCase("dbfilename")) {
        value = Main.dbfilename;
      } else {
        value = ""; // Redis returns empty string for unknown config keys
      }
      List<String> resp = new ArrayList<>();
      resp.add(RESPProtocol.formatBulkString(param));
      resp.add(RESPProtocol.formatBulkString(value));
      String response = "*" + resp.size() + "\r\n" + resp.get(0) + resp.get(1);
      outputStream.write(response.getBytes());
      System.out.println("Client " + clientId + " - CONFIG GET " + param + " -> " + value);
    } else {
      outputStream.write(RESPProtocol.formatError("ERR wrong number of arguments for 'config' command").getBytes());
      System.out.println("Client " + clientId + " - Sent error: CONFIG wrong arguments");
    }
  }

  private void handleKeys(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() == 2 && command.get(1).equals("*")) {
      List<String> keys = new ArrayList<>(stringStorage.getAllKeys());
      String resp = StorageManager.RESPProtocol.formatStringArray(keys);
      outputStream.write(resp.getBytes());
      System.out.println("Client " + clientId + " - KEYS * -> " + keys);
    } else {
      outputStream.write(StorageManager.RESPProtocol.formatError("ERR only KEYS * is supported").getBytes());
    }
  }

  private void handleIncr(List<String> command, OutputStream outputStream) throws IOException {
    if (command.size() == 2) {
      String key = command.get(1);
      String value = stringStorage.get(key);
      if (value != null) {
        try {
          long num = Long.parseLong(value);
          num += 1;
          stringStorage.set(key, Long.toString(num), null);
          outputStream.write(RESPProtocol.formatInteger(num).getBytes());
          System.out.println("Client " + clientId + " - INCR " + key + " -> " + num);
        } catch (NumberFormatException e) {
          outputStream.write(RESPProtocol.formatError("ERR value is not an integer or out of range").getBytes());
          System.out.println("Client " + clientId + " - INCR " + key + " failed: not an integer");
        }
      } else {
        // Key does not exist: set to 1 and return 1
        stringStorage.set(key, "1", null);
        outputStream.write(RESPProtocol.formatInteger(1).getBytes());
        System.out.println("Client " + clientId + " - INCR " + key + " (missing) -> 1");
      }
    } else {
      outputStream.write(RESPProtocol.getArgumentError("incr").getBytes());
      System.out.println("Client " + clientId + " - Sent error: INCR missing argument");
    }
  }

  private void handleMulti(List<String> command, OutputStream outputStream) throws IOException {
    inTransaction = true;
    queuedCommands.clear();
    outputStream.write(RESPProtocol.OK_RESPONSE.getBytes());
    System.out.println("Client " + clientId + " - MULTI -> OK");
  }

  private void handleExec(List<String> command, OutputStream outputStream) throws IOException {
    if (inTransaction) {
      List<String> responses = new ArrayList<>();
      for (List<String> queued : queuedCommands) {
        // Use a temporary buffer to capture the response for each command
        java.io.ByteArrayOutputStream tempOut = new java.io.ByteArrayOutputStream();
        // Execute the command as normal, but not as a transaction
        boolean prevInTransaction = inTransaction;
        inTransaction = false;
        handleCommand(queued, tempOut);
        inTransaction = prevInTransaction;
        responses.add(tempOut.toString());
      }
      String respArray = RESPProtocol.formatArray(responses);
      outputStream.write(respArray.getBytes());
      System.out.println("Client " + clientId + " - EXEC executed " + queuedCommands.size() + " commands");
      inTransaction = false;
      queuedCommands.clear();
    } else {
      outputStream.write(RESPProtocol.formatError("ERR EXEC without MULTI").getBytes());
      System.out.println("Client " + clientId + " - EXEC called without MULTI");
    }
  }

  private void handleDiscard(List<String> command, OutputStream outputStream) throws IOException {
    if (inTransaction) {
      inTransaction = false;
      queuedCommands.clear();
      outputStream.write(RESPProtocol.OK_RESPONSE.getBytes());
      System.out.println("Client " + clientId + " - DISCARD -> OK");
    } else {
      outputStream.write(RESPProtocol.formatError("ERR DISCARD without MULTI").getBytes());
      System.out.println("Client " + clientId + " - DISCARD called without MULTI");
    }
  }

  private void handleSave(List<String> command, OutputStream outputStream) throws IOException {
    RdbWriter writer = new RdbWriter(stringStorage);
    byte[] rdbFileBytes = writer.serializeToRdb();
    String currentDir = System.getProperty("user.dir");
    File tempFile = new File(currentDir, "dump.rdb.tmp");
    File rdbFile = new File(currentDir, "dump.rdb");
    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
        fos.write(rdbFileBytes);
    }
    // Atomically rename
    if (!tempFile.renameTo(rdbFile)) {
        throw new IOException("Failed to rename temp RDB file");
    }
    System.out.println("RDB file created at: " + rdbFile.getAbsolutePath());
    outputStream.write(RESPProtocol.OK_RESPONSE.getBytes());
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

  // Propagate write command to replica if connected
  private void propagateToReplica(List<String> command) {
    synchronized (HandleClient.class) {
      if (!replicaOutputStreams.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(command.size()).append("\r\n");
        for (String arg : command) {
          sb.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        byte[] resp = sb.toString().getBytes();
        for (OutputStream out : replicaOutputStreams) {
          try {
            out.write(resp);
            out.flush();
          } catch (IOException e) {
            System.out.println("Failed to propagate to replica: " + e.getMessage());
          }
        }
        System.out.println("Propagated to replicas: " + command);
      }
    }
  }
}
