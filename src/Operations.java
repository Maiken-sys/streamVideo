import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class Operations extends Thread {
    private Broker broker;
    public ObjectInputStream in;
    public ObjectOutputStream out;

    public Operations(Socket connection, Broker broker) {
        this.broker = broker;
        //Αρχικοποίηση ροών αντικειμένων για την επικοινωνία με τον εκάστοτε client
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            //out: sending data to client

            in = new ObjectInputStream(connection.getInputStream());
            //in: reading data from client
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        int option;

        while (true) {
            try {
                option = in.readInt();
                switch (option) {
                    case 1:         //the case where broker pulls the new video
                        try {
                            int nOfChunks = in.readInt();
                            System.out.println(nOfChunks + " chunks expected...");
                            ArrayList<Value> newVid = new ArrayList<>();
                            for (int i = 0; i < nOfChunks; i++) {
                                newVid.add((Value) in.readObject());
                            }
                            System.out.println("Retrieved " + newVid.size() + " chunks");


                        } catch (ClassNotFoundException | IOException e) {
                            e.printStackTrace();
                        }



                    case 2:
                        break;

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
