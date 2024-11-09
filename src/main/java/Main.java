import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Main extends Thread {
    public static final String OK = "+OK\r\n";
    public static final String NULL_BULK_STRING = "$-1\r\n";
    public static HashMap<String, String> hashMap = new HashMap<>();

    public static void main(String[] args) {
        int port = 6379;
        try (
                ServerSocket serverSocket = new ServerSocket(port)
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
        Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        public void run() {
            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))
            ) {
                String content;
                while ((content = reader.readLine()) != null) {
                    System.out.println("::" + content);
                    if ("ping".equalsIgnoreCase(content)) {
                        pingHandler(writer);
                    }

                    if (content.toLowerCase().startsWith("echo")) {
                        echoHandler(reader, writer);
                    }

                    if (content.toLowerCase().startsWith("set")) {
                        setHandler(reader, writer);
                    }

                    if (content.toLowerCase().startsWith("get")) {
                        getHandler(reader, writer);
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

        private void getHandler(BufferedReader reader, BufferedWriter writer) throws IOException {
            reader.readLine();
            String key = reader.readLine();
            System.out.println("::" + key);

            String value = hashMap.get(key);
            System.out.println("::" + value);

            if ("NULL".equals(value)) {
                writer.write(NULL_BULK_STRING);
                writer.flush();
                return;
            }

            writer.write(getFormat(value));
            writer.flush();
        }

        private void setHandler(BufferedReader reader, BufferedWriter writer) throws IOException {
            reader.readLine();
            String key = reader.readLine();
            System.out.println("::" + key);

            reader.readLine();
            String value = reader.readLine();
            System.out.println("::" + value);

            hashMap.put(key, value);
            writer.write(OK);
            writer.flush();
        }

        private static void echoHandler(BufferedReader reader, BufferedWriter writer) throws IOException {
            reader.readLine();
            String content = reader.readLine();
            System.out.println("::" + content);

            writer.write(getFormat(content));
            writer.flush();
        }

        private static void pingHandler(BufferedWriter writer) throws IOException {
            writer.write("+PONG\r\n");
            writer.flush();
        }

        private static String getFormat(String content) {
            return String.format("$%d\r\n%s\r\n", content.length(), content);
        }
    }

}
