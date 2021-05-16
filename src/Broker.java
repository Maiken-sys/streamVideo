import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Broker implements BrokerImpl, Serializable {
    private int brokerId;
    private String ip;
    private int port;                      //port of each broker
    private ServerSocket serverSocket;    //the server socket for each broker (the socket that is used for accepting
                                        // and rejecting clients
    private List<Broker> brokers = null;
    private List<Consumer> registeredConsumers = new ArrayList<>();
    private List<Publisher> registeredPublishers = new ArrayList<>();
    private ObjectOutputStream out = null;
    private ObjectInputStream in = null;
    private LinkedBlockingQueue<String> consumer_requests = new LinkedBlockingQueue<String>(); // contains consumer requests.
    private LinkedBlockingQueue<ArrayList<Value>> publisher_responses = new LinkedBlockingQueue<ArrayList<Value>>(); // contains chunked videos.
    private HashMap<ChannelName, ChannelName> channel_subscriptions = new HashMap<ChannelName, ChannelName>();  // contains channel subscriptions.
    private HashMap<ChannelName, String> hashtag_subscriptions = new HashMap<ChannelName, String>();    // contains hashtag subscriptions.
    private Object lock = new Object();

    public static void main(String [] args){

        Broker broker = new Broker("127.0.0.1" ,Integer.parseInt(args[0]));
        broker.setBrokerId(Integer.parseInt(args[1]));
        broker.update_broker_map();
        broker.connect();
    }
    Broker(String ip, int port){
        this.ip = ip;
        this.port = port;
    }
    public void setBrokerId(int id){
        brokerId = id;
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
                Thread clientThread = new Connection(clientSocket, consumer_requests, publisher_responses, lock);
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
        } catch (IOException ioException) {
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
                    BigInteger bhash = md5(String.valueOf(Integer.parseInt(broker.getIp().replaceAll("\\.", "")) + broker.getPort()));
                    if(val.compareTo(bhash) == 0){
                        return broker;
                    }
                }
            }
        }
        return null;
    }

    private BigInteger md5(String s){
        try {
            byte[] msg = s.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.digest(msg);
            byte[] hash_values = md.digest(msg);
            return new BigInteger(bytesToHex(hash_values), 16);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
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


    private class Connection extends Thread{
        private ObjectInputStream in;
        private ObjectOutputStream out;
        private Socket connection;
        Publisher publisher;
        Consumer consumer;
        Object lock;
        public Connection(Socket connection, LinkedBlockingQueue<String> consumer_requests, LinkedBlockingQueue<ArrayList<Value>> publisher_responses, Object lock){
            this.connection = connection;
            this.lock = lock;
            try{
                out = new ObjectOutputStream(connection.getOutputStream());
                in = new ObjectInputStream(connection.getInputStream());
            }catch (IOException e){
                e.printStackTrace();
            }
        }

        @Override
        public void run(){
            // wait for messages
            String request;
            String name;
            try {
                while(true){
                    request = (String)in.readObject();
                    if(request.matches("consumer\n")) {
                        ChannelName channel = (ChannelName) in.readObject();
                        name = channel.getName();
                        boolean exists = false;
                        for(Consumer con : registeredConsumers){
                            if(con.getChannelName().getName().equals(name)){
                                exists = true;
                            }
                        }
                        if(!exists){
                            request = (String)in.readObject();
                            if(request.matches("register\n")){
                                consumer = Broker.this.acceptConnection(new Consumer(name));
                                out.writeObject("success\n");
                                out.flush();
                            }else{
                                connection.close();
                            }
                        }
                    }else if(request.matches("publisher\n")){   // publisher connection returns published hashtags
                        ChannelName channel = (ChannelName) in.readObject();
                        name = channel.getName();
                        boolean exists = false;
                        for(Publisher pub : registeredPublishers){
                            if(pub.getChannelName().getName().equals(name)){
                                exists = true;
                                out.writeObject(pub.getChannelName().getHashtagsPublished());
                                out.flush();
                            }
                        }
                        if(!exists){
                            publisher = Broker.this.acceptConnection(new Publisher(name));
                            out.writeObject(null);
                            out.flush();
                        }/*else{
                            for(Publisher pub : registeredPublishers){
                                if(pub.getChannelName().getName().equals(name)){
                                    out.writeObject(pub.getChannelName().getHashtagsPublished());
                                    break;
                                }
                            }
                        }*/
                    }else if(request.matches("video\n")){
                        String videoName = (String)in.readObject();
                        int nOfChunks = in.readInt();
                        System.out.println(nOfChunks + " chunks expected...");
                        ArrayList<Value> newVid = new ArrayList<>();
                        for(int i=0; i<nOfChunks; i++) {
                            newVid.add((Value) in.readObject());
                        }
                        System.out.println("Retrieved " + newVid.size() + " chunks");
                        //videos.offer(newVid);
                        //VideoFile file = new VideoFile(newVid, videoName);
                        //file.saveVideo();
                    }else if(request.matches("send-brokers\n")){
                        out.writeObject(Broker.this.getBrokers());
                    }else if(request.matches("hash\n")){    // if client requests value hashing.
                        String value = (String)in.readObject();
                        Broker broker = Broker.this.hash(value);
                        out.writeObject(broker.getIp());   // send broker ip + port with based on hash value
                        out.flush();
                        out.writeObject(broker.getPort());
                        out.flush();
                    }else if(request.matches("new\n")){ // if new video is available.
                        out.writeObject("send\n");
                        out.flush();
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