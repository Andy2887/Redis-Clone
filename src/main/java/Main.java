import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
       ServerSocket serverSocket = null;
       Socket clientSocket = null;
       int port = 6379;
       try {
         serverSocket = new ServerSocket(port);
         // Since the tester restarts your program quite often, setting SO_REUSEADDR
         // ensures that we don't run into 'Address already in use' errors
         serverSocket.setReuseAddress(true);
         System.out.println("Server started on port " + port);
         
         // Wait for connection from client.
         System.out.println("Waiting for client connection...");
         clientSocket = serverSocket.accept();
         System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());
         
         BufferedReader inputReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         OutputStream outputStream = clientSocket.getOutputStream();
         
         String inputLine;
         int commandCount = 0;
         // Keep reading commands until the connection is closed
         while ((inputLine = inputReader.readLine()) != null) {
           commandCount++;
           System.out.println("Received command #" + commandCount + ": " + inputLine);
           
           // For now, we'll respond to any command with PONG
           // In a real Redis implementation, we'd parse the command
           outputStream.write("+PONG\r\n".getBytes());
           outputStream.flush();
           System.out.println("Sent response #" + commandCount + ": +PONG");
         }
         System.out.println("Client disconnected. Total commands processed: " + commandCount);
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
         e.printStackTrace();
       } finally {
         System.out.println("Cleaning up resources...");
         try {
           if (clientSocket != null) {
             clientSocket.close();
             System.out.println("Client socket closed");
           }
           if (serverSocket != null) {
             serverSocket.close();
             System.out.println("Server socket closed");
           }
         } catch (IOException e) {
           System.out.println("IOException during cleanup: " + e.getMessage());
         }
       }
  }
}
