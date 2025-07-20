import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class HandleClient implements Runnable {
  private Socket clientSocket;
  private int clientId;
  
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
  
  private void handleCommand(List<String> command, OutputStream outputStream) throws IOException {
    String commandName = command.get(0).toUpperCase();
    
    switch (commandName) {
      case "PING":
        outputStream.write("+PONG\r\n".getBytes());
        System.out.println("Client " + clientId + " - Sent: +PONG");
        break;
        
      case "ECHO":
        if (command.size() > 1) {
          String echoArg = command.get(1);
          String response = "$" + echoArg.length() + "\r\n" + echoArg + "\r\n";
          outputStream.write(response.getBytes());
          System.out.println("Client " + clientId + " - Sent ECHO response: " + echoArg);
        } else {
          outputStream.write("-ERR wrong number of arguments for 'echo' command\r\n".getBytes());
          System.out.println("Client " + clientId + " - Sent error: ECHO missing argument");
        }
        break;
        
      default:
        String errorMsg = "-ERR unknown command '" + commandName + "'\r\n";
        outputStream.write(errorMsg.getBytes());
        System.out.println("Client " + clientId + " - Sent error: unknown command " + commandName);
        break;
    }
    
    outputStream.flush();
  }
}
