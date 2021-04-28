import java.io.*;
import java.net.Socket;

//Operations is a class that the servers' (broker) actions take place

public class Operations extends Thread{
    public ObjectInputStream in;
    public ObjectOutputStream out;

    public Operations(Socket connection){
        //Αρχικοποίηση ροών αντικειμένων για την επικοινωνία με τον εκάστοτε client
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
        System.out.println("The run method of Broker just executed");
    }


}