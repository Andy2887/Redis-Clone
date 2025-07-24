import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

import StorageManager.StringStorage;
import RdbManager.RdbStringResult;
import RdbManager.RdbSizeResult;

public class Main {
  public static String serverRole = "master";
  private static String masterHost = null;
  private static int masterPort = -1;
  public static String dir = "/tmp";
  public static String dbfilename = "dump.rdb";
  public static final StringStorage stringStorage = new StringStorage();

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    int port = getPortFromArgs(args);
    parseConfigFlags(args);
    parseReplicaOfFlag(args);
    // Load RDB file if it exists
    loadRdbFile();
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

  private static void parseConfigFlags(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if ("--dir".equals(args[i]) && i + 1 < args.length) {
        dir = args[i + 1];
      }
      if ("--dbfilename".equals(args[i]) && i + 1 < args.length) {
        dbfilename = args[i + 1];
      }
    }
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

  private static void loadRdbFile() {
    File rdbFile = new File(dir, dbfilename);
    if (!rdbFile.exists()) {
      System.out.println("RDB file not found: " + rdbFile.getAbsolutePath());
      return;
    }
    try (FileInputStream in = new FileInputStream(rdbFile)) {
      byte[] data = in.readAllBytes();
      parseRdb(data);
    } catch (IOException e) {
      System.out.println("Failed to load RDB: " + e.getMessage());
    }
  }

  private static void parseRdb(byte[] data) {
    int i = 0;
    // Check header
    if (data.length < 9 || !new String(Arrays.copyOfRange(data, 0, 9)).equals("REDIS0011")) {
      System.out.println("Invalid RDB header");
      return;
    }
    i = 9;
    // Skip metadata and go to database section
    while (i < data.length) {
      int b = data[i] & 0xFF;
      if (b == 0xFE) { // DB selector
        i++;
        i += readSize(data, i).bytesRead; // skip db index
      } else if (b == 0xFB) { // hash table sizes
        i++;
        i += readSize(data, i).bytesRead; // skip hash table size
        i += readSize(data, i).bytesRead; // skip expires hash table size
      } else if (b == 0xFC || b == 0xFD) { // expire info
        boolean ms = (b == 0xFC);
        i++;
        i += ms ? 8 : 4; // skip expire timestamp
      } else if (b == 0xFF) {
        break; // End of file
      } else {
        // Value type (expect 0 for string)
        int valueType = data[i++] & 0xFF;
        if (valueType == 0) {
          // Key
          RdbStringResult keyRes = readRdbString(data, i);
          String key = keyRes.value;
          i += keyRes.bytesRead;
          // Value
          RdbStringResult valRes = readRdbString(data, i);
          String value = valRes.value;
          i += valRes.bytesRead;
          stringStorage.set(key, value, null);
        }
      }
    }
  }

  // Helper for size encoding
  private static RdbSizeResult readSize(byte[] data, int i) {
    int b = data[i] & 0xFF;
    int type = (b & 0xC0) >> 6;
    if (type == 0) return new RdbSizeResult(b & 0x3F, 1);
    if (type == 1) return new RdbSizeResult(((b & 0x3F) << 8) | (data[i+1] & 0xFF), 2);
    if (type == 2) return new RdbSizeResult(((data[i+1]&0xFF)<<24)|((data[i+2]&0xFF)<<16)|((data[i+3]&0xFF)<<8)|(data[i+4]&0xFF), 5);
    // type == 3: string encoding, not used for size
    return new RdbSizeResult(0, 1);
  }

  // Helper for string encoding
  private static RdbStringResult readRdbString(byte[] data, int i) {
    RdbSizeResult sizeRes = readSize(data, i);
    int b = data[i] & 0xFF;
    int type = (b & 0xC0) >> 6;
    if (type == 0 || type == 1 || type == 2) {
      int len = sizeRes.value;
      String s = new String(data, i + sizeRes.bytesRead, len);
      return new RdbStringResult(s, sizeRes.bytesRead + len);
    }
    // Integer encodings (0xC0, 0xC1, 0xC2)
    if ((b & 0xFF) == 0xC0) { // 8-bit int
      int v = data[i+1];
      return new RdbStringResult(Integer.toString(v), 2);
    }
    if ((b & 0xFF) == 0xC1) { // 16-bit int
      int v = ((data[i+2]&0xFF)<<8)|(data[i+1]&0xFF);
      return new RdbStringResult(Integer.toString(v), 3);
    }
    if ((b & 0xFF) == 0xC2) { // 32-bit int
      int v = ((data[i+4]&0xFF)<<24)|((data[i+3]&0xFF)<<16)|((data[i+2]&0xFF)<<8)|(data[i+1]&0xFF);
      return new RdbStringResult(Integer.toString(v), 5);
    }
    return new RdbStringResult("", sizeRes.bytesRead);
  }
}