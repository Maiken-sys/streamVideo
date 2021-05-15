import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

//Operations is a class that the servers' (broker) actions take place

public class Operations extends Thread{
    public ObjectInputStream in;
    public ObjectOutputStream out;
    LinkedBlockingQueue<Value> queue;
    Broker broker;
    public Operations(Socket connection, Broker broker, LinkedBlockingQueue<Value> queue){
        //Αρχικοποίηση ροών αντικειμένων για την επικοινωνία με τον εκάστοτε client
        this.queue = queue;
        this.broker = broker;
        try{
            out = new ObjectOutputStream(connection.getOutputStream());
            //out: sending data to client

            in = new ObjectInputStream(connection.getInputStream());
            //in: reading data from client
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void run(){
        // wait for messages
        String request;
        try {
            while(true){
                request = (String)in.readObject();
                if(request.matches("video\n")){
                    String videoName = (String)in.readObject();
                    int nOfChunks = in.readInt();
                    System.out.println(nOfChunks + " chunks expected...");
                    ArrayList<Value> newVid = new ArrayList<>();
                    for(int i=0; i<nOfChunks; i++) {
                        newVid.add((Value) in.readObject());
                    }
                    System.out.println("Retrieved " + newVid.size() + " chunks");
                    VideoFile file = new VideoFile(newVid, videoName);
                    file.saveVideo();
                }else if(request.matches("send-brokers\n")){
                    out.writeObject(broker.getBrokers());
                }else if(request.matches("hash\n")){

                }
            }
        } catch (IOException exception) {
            System.err.println("Client disconnected");
        } catch (ClassNotFoundException classNotFoundException) {
            classNotFoundException.printStackTrace();
        }
    }
}