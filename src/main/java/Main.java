import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main extends Thread {

    public static void main(String[] args) {
        int port = 6379;
        try (
                ServerSocket serverSocket = new ServerSocket(port);
        ) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.

            while (true) {
                new ClientHandler(serverSocket.accept()).start();
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static class ClientHandler extends Thread {
        Socket clientSocket = null;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            ) {
                String content;
                while ((content = reader.readLine()) != null) {
                    System.out.println("::" + content);
                    if ("ping".equalsIgnoreCase(content)) {
                        writer.write("+PONG\r\n");
                        writer.flush();
                    }
                }

            } catch (IOException e) {
                try {
                    if (clientSocket != null) {
                        clientSocket.close();
                    }
                } catch (IOException _) {
                }
                throw new RuntimeException(e);
            }

        }
    }

}
