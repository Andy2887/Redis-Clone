import java.io.IOException;
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
      HandleReplica.startReplica(masterHost, masterPort, port);
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
      System.out.println("Parsing replicaof flag...");
      for (int i = 0; i < args.length; i++) {
          if ("--replicaof".equals(args[i])) {
              serverRole = "slave";
              System.out.println("Server role set to: " + serverRole);
              // Support both --replicaof "host port" and --replicaof host port
              if (i + 2 < args.length) {
                  // Try the two-argument form: --replicaof host port
                  masterHost = args[i + 1];
                  try {
                      masterPort = Integer.parseInt(args[i + 2]);
                      System.out.println("Master Host set to: " + masterHost);
                      System.out.println("Master Port set to: " + masterPort);
                  } catch (NumberFormatException e) {
                      masterPort = -1;
                      System.out.println("Sent error: invalid port value");
                  }
              } else if (i + 1 < args.length) {
                  // Try the single-argument form: --replicaof "host port"
                  String[] parts = args[i + 1].split(" ");
                  if (parts.length == 2) {
                      masterHost = parts[0];
                      try {
                          masterPort = Integer.parseInt(parts[1]);
                          System.out.println("Master Host set to: " + masterHost);
                          System.out.println("Master Port set to: " + masterPort);
                      } catch (NumberFormatException e) {
                          masterPort = -1;
                          System.out.println("Sent error: invalid port value");
                      }
                  }
              }
              break;
          }
      }
  }

}