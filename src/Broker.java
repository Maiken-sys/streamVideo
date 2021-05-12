import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Broker implements BrokerImpl, Serializable {
    private String ip;
    private int port;                      //port of each broker
    private ServerSocket serverSocket;    //the server socket for each broker (the socket that is used for accepting
                                        // and rejecting clients
    private List<Broker> brokers = null;
    private List<Consumer> registeredConsumers = new ArrayList<>();
    private List<Publisher> registeredPublishers = new ArrayList<>();
    ObjectOutputStream out = null;
    ObjectInputStream in = null;
    final LinkedBlockingQueue<Value> queue = new LinkedBlockingQueue<Value>();
    final LinkedBlockingQueue<Value> consumer_requests = new LinkedBlockingQueue<Value>();

    public static void main(String [] args){

        Broker broker = new Broker("127.0.0.1" ,Integer.parseInt(args[0]));
        broker.update_broker_map();
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
        return brokers;
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
                System.out.println("Client connected "+ clientSocket.getInetAddress().getHostAddress());
                Thread clientThread = new Operations(clientSocket, queue);
                clientThread.start();
            }
        } catch (IOException e){
            System.out.println("Can't setup server on this port number or Can't accept client connection. ");
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            this.disconnect();
        }
    }

    private void update_broker_map(){
        Properties properties = new Properties();
        String broker_cfg = "data/broker_data.cfg";
        InputStream is = null;
        brokers = new ArrayList<>();
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
            brokers.add(new Broker(ips[i], Integer.parseInt(ports[i])));
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
    public void calculateKeys() {   // hashtags or names

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
        try {
            out.writeObject("pull\n");
            out.flush();
            out.writeObject(s);
            out.flush();
            consumer_requests.offer((Value) in.readObject());
        } catch (IOException | ClassNotFoundException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void filterConsumers(String s) {

    }

    private Broker hash(String s){
        ArrayList<BigInteger> brokers_hash = new ArrayList<BigInteger>();
        for(Broker broker : brokers){
            brokers_hash.add(md5(String.valueOf(Integer.parseInt(broker.getIp().replaceAll("\\.", "")) + broker.getPort())));
        }
        Collections.sort(brokers_hash);
        BigInteger hashValue = md5(s).mod(brokers_hash.get(brokers_hash.size()-1));
        for(BigInteger val : brokers_hash){
            if(hashValue.compareTo(val) == -1){
                for (Broker broker : brokers){
                    if(val.compareTo(md5(String.valueOf(Integer.parseInt(broker.getIp().replaceAll("\\.", "")) + broker.getPort()))) == 0){
                        return broker;
                    }
                }
            }
        }
        return null;
    }

    private BigInteger md5(String s){
        byte[] msg = s.getBytes(StandardCharsets.UTF_8);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.digest(msg);
            byte[] hash_values = md.digest(msg);
            return new BigInteger(bytesToHex(hash_values));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String bytesToHex(byte[] bytes) {
        final char[] hexArray = "0123456789ABCDEF".toCharArray();
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }


    public String getIp(){
        return ip;
    }

    public int getPort(){
        return port;
    }


    private class Operations extends Thread{
        public ObjectInputStream in;
        public ObjectOutputStream out;
        LinkedBlockingQueue<Value> queue;
        public Operations(Socket connection, LinkedBlockingQueue<Value> queue){
            //Αρχικοποίηση ροών αντικειμένων για την επικοινωνία με τον εκάστοτε client
            this.queue = queue;
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
                        out.writeObject(Broker.this.getBrokers());
                    }else if(request.matches("hash\n")){
                        String value = (String)in.readObject();
                        out.writeObject(Broker.this.hash(value));   // send broker with based on hash value
                    }
                }
            } catch (IOException exception) {
                System.err.println("Client disconnected");
            } catch (ClassNotFoundException classNotFoundException) {
                classNotFoundException.printStackTrace();
            }
        }
    }
}