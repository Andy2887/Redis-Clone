import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static String serverRole = "master";
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = getPortFromArgs(args);
    parseReplicaOfFlag(args);
    int clientCounter = 0;

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
      if ("--replicaof".equals(args[i])) {
        serverRole = "slave";
        break;
      }
    }
  }

}