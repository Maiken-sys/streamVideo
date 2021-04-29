import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class Broker implements BrokerImpl{
    public int port;                      //port of each broker
    public ServerSocket serverSocket;    //the server socket for each broker (the socket that is used for accepting
                                        // and rejecting clients
    public String ip;

    static List<Consumer> registeredUsers = new ArrayList<>();
    static List<Publisher> registeredPublishers = new ArrayList<>();

    private HashMap<Integer, String> brokers = new HashMap<>();   // port, ip broker pair

    public static void main(String[] args){

        Broker broker = new Broker(4321);
        broker.update_broker_map();
        broker.connect();
    }

    private void update_broker_map(){
        Properties properties = new Properties();
        String broker_cfg = "src/broker_data.cfg";
        InputStream is = null;

        try{
            is = new FileInputStream(broker_cfg);
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }
        try{
            properties.load(is);
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        String[] ips = properties.getProperty("brokers.ip").split(",");
        String[] ports = properties.getProperty("brokers.port").split(",");
        for(int i = 0; i < ips.length; i++){
            brokers.put(Integer.parseInt(ports[i]), ips[i]);
        }
    }


    Broker(int port){
        this.port = port;
    }


    public String getIp(){
        return ip;
    }
    public int getPort(){
        return port;
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
            e.printStackTrace();
        }
    }

    @Override
    public void disconnect() {

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
        return null;
    }

    @Override
    public Consumer acceptConnection(Consumer consumer) {
        return null;
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




}