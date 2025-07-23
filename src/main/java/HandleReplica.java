import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class HandleReplica {
    public static void startReplica(String masterHost, int masterPort, int port) {
        new Thread(() -> {
            try (Socket masterSocket = new Socket(masterHost, masterPort)) {
                OutputStream out = masterSocket.getOutputStream();
                java.io.InputStream in = masterSocket.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));

                // Send RESP array: *1\r\n$4\r\nPING\r\n
                String pingResp = "*1\r\n$4\r\nPING\r\n";
                out.write(pingResp.getBytes());
                out.flush();

                // Wait for +PONG or +OK from master
                byte[] buffer = new byte[1024];
                int len = in.read(buffer);
                String response = new String(buffer, 0, len);
                System.out.println("Raw String replica received from master: " + response.trim());

                // Send REPLCONF listening-port <PORT>
                String portStr = String.valueOf(port);
                String replconfListeningPort =
                        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + portStr.length() + "\r\n" + portStr + "\r\n";
                out.write(replconfListeningPort.getBytes());
                out.flush();

                // Wait for +OK from master
                len = in.read(buffer);
                response = new String(buffer, 0, len);
                System.out.println("Raw String replica received from master: " + response.trim());

                // Send REPLCONF capa psync2
                String replconfCapa =
                        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                out.write(replconfCapa.getBytes());
                out.flush();

                // Wait for +OK from master
                len = in.read(buffer);
                response = new String(buffer, 0, len);
                System.out.println("Raw String replica received from master: " + response.trim());

                // Send PSYNC ? -1
                String psyncCmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                out.write(psyncCmd.getBytes());
                out.flush();

                // Wait for +FULLRESYNC from master (can be ignored for now)
                len = in.read(buffer);
                response = new String(buffer, 0, len);
                System.out.println("Raw String replica received from master: " + response.trim());

                // Read and discard the RDB file (parse RESP bulk string header, then read N bytes)
                String bulkHeader = "";
                int b;
                while ((b = in.read()) != -1) {
                    bulkHeader += (char) b;
                    if (bulkHeader.endsWith("\r\n")) break;
                }
                if (bulkHeader.startsWith("$")) {
                    int rdbLen = Integer.parseInt(bulkHeader.substring(1, bulkHeader.length() - 2));
                    int read = 0;
                    while (read < rdbLen) {
                        int skipped = (int) in.skip(rdbLen - read);
                        if (skipped <= 0) break;
                        read += skipped;
                    }
                }

                System.out.println("Replica handshake with master complete.");

                // Now process propagated commands from master
                while (true) {
                    String line = reader.readLine();
                    if (line == null) break;
                    if (line.startsWith("*")) {
                        List<String> command = StorageManager.RESPProtocol.parseRESPArray(reader, line);
                        if (command != null && !command.isEmpty()) {
                            System.out.println("Replica - Received propagated command: " + command);
                            // Process the command, but do NOT send a response to master
                            // Use a dummy OutputStream that discards output
                            HandleClient dummyClient = new HandleClient(null, -1, "slave");
                            java.io.OutputStream devNull = new java.io.OutputStream() {
                                public void write(int b) {}
                            };
                            dummyClient.handleCommand(command, devNull);
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed to connect/send handshake to master: " + e.getMessage());
            }
        }).start();
    }
}