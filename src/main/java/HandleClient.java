import java.io.IOException;
import java.net.Socket;

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
      while (true) {
        // Read input from client.
        byte[] input = new byte[1024];
        int bytesRead = clientSocket.getInputStream().read(input);
        
        if (bytesRead == -1) {
          // Client disconnected
          System.out.println("Client " + clientId + " disconnected");
          break;
        }
        
        String inputString = new String(input, 0, bytesRead).trim();
        System.out.println("Client " + clientId + " - Received: " + inputString);
        
        // Send PONG response
        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
        System.out.println("Client " + clientId + " - Sent: +PONG");
      }
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
}
