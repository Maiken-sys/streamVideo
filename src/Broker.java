import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class Broker implements BrokerImpl {
    private String ip;
    private int port;                      //port of each broker
    private ServerSocket serverSocket;    //the server socket for each broker (the socket that is used for accepting
                                        // and rejecting clients
    private List<Broker> brokers = null;
    private List<Consumer> registeredConsumers = new ArrayList<>();
    private List<Publisher> registeredPublishers = new ArrayList<>();
    ObjectOutputStream out = null;
    ObjectInputStream in = null;

    public static void main(String [] args){

        Broker broker = new Broker("127.0.0.1" ,4321);
        broker.connect();
    }


    Broker(String ip, int port){
        this.ip = ip;
        this.port = port;
    }



    //*******************************************************************************
    //Node override methods****************************************************
    //*******************************************************************************

    @Override
    public void init(int x) {

    }

    @Override
    public List<Broker> getBrokers() {
        return null;
    }

    @Override
    //Η συνάρτηση connect() αρχικοποιεί το serverSocket του εκάστοτε broker
    public void connect() {
        try{
            this.serverSocket = new ServerSocket(this.port, 10);
            this.ip = serverSocket.getInetAddress().toString();
            serverSocket.setReuseAddress(true);   //allows the socket to be bound even though a previous connection is in a timeout state.
            while(true){

                Socket clientSocket = this.serverSocket.accept();
                System.out.println("New consumer connected "+ clientSocket.getInetAddress().getHostAddress() );
                Thread clientThread = new Operations(clientSocket);
                clientThread.start();
            }
        } catch (IOException e){
            System.out.println("Can't setup server on this port number or Can't accept client connection. ");
        }finally {
            this.disconnect();
        }
    }

    @Override
    public void disconnect() {
        try {
            this.in.close();
            this.out.close();
            this.serverSocket.close();
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    @Override
    public void updateNodes() {

    }



    //*******************************************************************************
    //BrokerImpl-Only methods****************************************************
    //*******************************************************************************


    @Override
    public void calculateKeys() {

    }

    @Override
    public Publisher acceptConnection(Publisher publisher) {
        this.registeredPublishers.add(publisher);
        return publisher;
    }

    @Override
    public Consumer acceptConnection(Consumer consumer) {
        this.registeredConsumers.add(consumer);
        return consumer;
    }


    @Override
    public void notifyPublisher(String s) {

    }

    @Override
    public void notifyBrokersOnChanges() {

    }

    @Override
    public void pull(String s) {

    }

    @Override
    public void filterConsumers(String s) {

    }


    public String getIp(){
        return ip;
    }

    public int getPort(){
        return port;
    }




}