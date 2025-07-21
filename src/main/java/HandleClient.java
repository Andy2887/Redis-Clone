import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class HandleClient implements Runnable {
  private Socket clientSocket;
  private int clientId;
  
  // Shared storage for all clients - using ConcurrentHashMap for thread safety
  private static final Map<String, String> storage = new ConcurrentHashMap<>();
  // Storage for expiry times (key -> expiry timestamp in milliseconds)
  private static final Map<String, Long> expiryTimes = new ConcurrentHashMap<>();
  // Storage for lists (key -> list of elements)
  private static final Map<String, List<String>> lists = new ConcurrentHashMap<>();
  
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
        
      case "LRANGE":
        handleLrange(command, outputStream);
        break;
        
      case "LLEN":
        handleLlen(command, outputStream);
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
      }
    } else {
      outputStream.write("-ERR wrong number of arguments for 'lpush' command\r\n".getBytes());
      System.out.println("Client " + clientId + " - Sent error: LPUSH missing arguments");
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
}
