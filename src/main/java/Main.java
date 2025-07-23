import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static String serverRole = "master";
  private static String masterHost = null;
  private static int masterPort = -1;
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = getPortFromArgs(args);
    parseReplicaOfFlag(args);
    int clientCounter = 0;
    
    // If replica, connect to master and send PING, then REPLCONF commands
    if ("slave".equals(serverRole) && masterHost != null && masterPort > 0) {
      new Thread(() -> {
        try (Socket masterSocket = new Socket(masterHost, masterPort)) {
          OutputStream out = masterSocket.getOutputStream();
          java.io.InputStream in = masterSocket.getInputStream();

          // Send RESP array: *1\r\n$4\r\nPING\r\n
          String pingResp = "*1\r\n$4\r\nPING\r\n";
          out.write(pingResp.getBytes());
          out.flush();

          // Wait for +PONG or +OK from master
          byte[] buffer = new byte[1024];
          int len = in.read(buffer);
          String response = new String(buffer, 0, len);
          System.out.println("Replica received from master: " + response.trim());

          // Send REPLCONF listening-port <PORT>
          String portStr = String.valueOf(port);
          String replconfListeningPort =
              "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + portStr + "\r\n";
          out.write(replconfListeningPort.getBytes());
          out.flush();

          // Wait for +OK from master
          len = in.read(buffer);
          response = new String(buffer, 0, len);
          System.out.println("Replica received from master: " + response.trim());

          // Send REPLCONF capa psync2
          String replconfCapa =
              "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
          out.write(replconfCapa.getBytes());
          out.flush();

          // Wait for +OK from master
          len = in.read(buffer);
          response = new String(buffer, 0, len);
          System.out.println("Replica received from master: " + response.trim());

          // Send PSYNC ? -1
          String psyncCmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
          out.write(psyncCmd.getBytes());
          out.flush();

          // Wait for +FULLRESYNC from master (can be ignored for now)
          len = in.read(buffer);
          response = new String(buffer, 0, len);
          System.out.println("Replica received from master: " + response.trim());

          System.out.println("Replica handshake with master complete.");
        } catch (Exception e) {
          System.out.println("Failed to connect/send handshake to master: " + e.getMessage());
        }
      }).start();
    }

    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting
      // SO_REUSEADDR ensures that we don't run into 'Address already in use'
      // errors
      serverSocket.setReuseAddress(true);
      System.out.println("Redis server started on port " + port);

      // Continuously accept new client connections
      while (true) {
        try {
          // Wait for connection from client.
          Socket clientSocket = serverSocket.accept();
          clientCounter++;

          // Create a new thread to handle this client
          Thread clientThread = new Thread(new HandleClient(clientSocket, clientCounter, serverRole));
          clientThread.start();

          System.out.println("Started thread for client " + clientCounter);
        } catch (IOException e) {
          System.out.println("Error accepting client connection: " + e.getMessage());
        }
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (serverSocket != null) {
          serverSocket.close();
          System.out.println("Server socket closed");
        }
      } catch (IOException e) {
        System.out.println("IOException during server cleanup: " + e.getMessage());
      }
    }
  }

  private static int getPortFromArgs(String[] args) {
    int port = 6379;
    for (int i = 0; i < args.length; i++) {
      if ("--port".equals(args[i]) && i + 1 < args.length) {
        try {
          port = Integer.parseInt(args[i + 1]);
        } catch (NumberFormatException e) {
          System.out.println("Invalid port number: " + args[i + 1] + ", using default 6379");
          port = 6379;
        }
        break;
      }
    }
    return port;
  }

  private static void parseReplicaOfFlag(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if ("--replicaof".equals(args[i]) && i + 1 < args.length) {
        serverRole = "slave";
        String[] parts = args[i + 1].split(" ");
        if (parts.length == 2) {
          masterHost = parts[0];
          try {
            masterPort = Integer.parseInt(parts[1]);
          } catch (NumberFormatException e) {
            masterPort = -1;
          }
        }
        break;
      }
    }
  }

}