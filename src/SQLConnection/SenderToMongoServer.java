package SQLConnection;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class SenderToMongoServer {

    private Socket socket;
    private Message message;

    public SenderToMongoServer (Socket socket, Message message) {
        this.socket = socket;
        this.message = message;
    }

    public void sendMessage() {

        try {
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(message);
            oos.flush();
            System.out.println("Message " + message + "\n was sent!");
        } catch (IOException e) {
            System.out.println("Message " + message + "\n cloudn't be sent!");
            e.printStackTrace();
        }
    }
}
