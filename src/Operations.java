import java.io.*;
import java.net.Socket;
import java.util.ArrayList;

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
    public void run() {

           try{
               int nOfChunks = in.readInt();
               System.out.println(nOfChunks + " chunks expected...");
               ArrayList<Value> newVid = new ArrayList<>();
               for(int i=0; i<nOfChunks; i++) {
                   newVid.add((Value) in.readObject());
               }
               System.out.println("Retrieved " + newVid.size() + " chunks");



           }catch (ClassNotFoundException | IOException e){
               e.printStackTrace();
           }finally{
               try{
                   System.out.println("Server Closing Connection");
                   in.close();
                   out.close();
               }catch (IOException e){
                   e.printStackTrace();
               }
           }



    }


}



