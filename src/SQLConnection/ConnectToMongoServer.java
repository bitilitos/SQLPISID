package SQLConnection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class ConnectToMongoServer extends Thread{

    private Socket socket;
    private InetAddress serverAddress;
    private int serverPort;


    public ConnectToMongoServer(InetAddress serverAddress, int serverPort) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }

    public void connect() {
        try{
            socket = new Socket(serverAddress, serverPort);
            SenderToMongoServer senderToMongoServer = new SenderToMongoServer(socket, new Message("id", MessageType.MONGO_SERVER_READY, "All OK"));
            senderToMongoServer.sendMessage();

            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            while (!socket.isClosed() && socket.isConnected()) {
                System.out.println("Mongo receiver waiting...");
                Message message = (Message) ois.readObject();
                System.out.println(message);
            }


        } catch (IOException e) {
            System.out.println("Failed to connect to " + serverAddress + ":" + serverPort );
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("Failed to read object!");
            e.printStackTrace();
        }

    }


        public static void main(String[] args) {

            InetAddress serverAddress;

            {
                try {
                    serverAddress = InetAddress.getByName("127.0.0.1");
                    int serverPort = 8888;
                    ConnectToMongoServer connectToMongoServer = new ConnectToMongoServer(serverAddress, serverPort);
                    connectToMongoServer.connect();
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
        }


}
