import java.awt.*;
import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.List;
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
    private LinkedBlockingQueue<ArrayList<Value>> videos = new LinkedBlockingQueue<ArrayList<Value>>(); // contains chunked videos.
    private HashMap<ChannelName, ArrayList<ChannelName>> channel_subscriptions = new HashMap<ChannelName, ArrayList<ChannelName>>();  // contains channel subscriptions.
    private HashMap<ChannelName, ArrayList<String>> hashtag_subscriptions = new HashMap<ChannelName, ArrayList<String>>();    // contains hashtag subscriptions.
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
                Thread clientThread = new Connection(clientSocket, videos, lock);
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
    public void pull(String s) {    // TODO
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
        private LinkedBlockingQueue<ArrayList<Value>> videos;
        private ArrayList<ArrayList<Value>> current_videos;
        private int size=0;
        Publisher publisher;
        Consumer consumer;
        Object lock;
        public Connection(Socket connection, LinkedBlockingQueue<ArrayList<Value>> videos, Object lock){
            this.connection = connection;
            this.lock = lock;
            this.videos = videos;
            this.size = videos.size();
            this.current_videos = new ArrayList<>(videos);
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
                    request = (String)in.readObject();  // request = consumer or publisher request.
                    if(request == null){    // if consumer doesn't send anything check for new available videos to send.
                        for(ArrayList<Value> values : videos){
                            if(!current_videos.contains(values)){ // if new video was added check hashtags/name and send it.
                                ArrayList<String> video_hashtags = values.get(0).getVideoFile().getAssociatedHashtags();
                                ArrayList<ChannelName> subbed_channels = Broker.this.channel_subscriptions.get(consumer);
                                ArrayList<String> subbed_hashtags = Broker.this.hashtag_subscriptions.get(consumer);
                                boolean sent = false;
                                for(ChannelName sub : subbed_channels){     // check if consumer is subbed to channel.
                                    if(sub.getName().equals(values.get(0).getChannelName())){
                                        //TODO send video.
                                        sent = true;
                                    }
                                }
                                if(!sent){  // check if consumer is subbed to hashtags.
                                    for(String hs : video_hashtags){
                                        if(subbed_hashtags.contains(hs)){
                                            //TODO send video.
                                        }
                                    }
                                }
                                this.current_videos = new ArrayList<>(videos);
                            }
                        }
                    }
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
                        int pub_port = (Integer)in.readObject();
                        boolean exists = false;
                        for(Publisher pub : registeredPublishers){
                            if(pub.getChannelName().getName().equals(name)){
                                exists = true;
                                out.writeObject(pub.getChannelName().getHashtagsPublished());
                                out.flush();
                            }
                        }
                        if(!exists){
                            publisher = Broker.this.acceptConnection(new Publisher(name, pub_port));
                            out.writeObject(null);
                            out.flush();
                        }
                    }else if(request.matches("video\n")){
                        String videoName = (String)in.readObject();
                        int nOfChunks = in.readInt();
                        System.out.println(nOfChunks + " chunks expected...");
                        ArrayList<Value> newVid = new ArrayList<>();
                        for(int i=0; i<nOfChunks; i++) {
                            newVid.add((Value) in.readObject());
                        }
                        System.out.println("Retrieved " + newVid.size() + " chunks");
                        videos.offer(newVid);
                        //VideoFile file = new VideoFile(newVid, videoName);
                        //file.saveVideo();
                    }else if(request.matches("send-brokers\n")){
                        out.writeObject(brokers.size());
                        out.flush();
                        for(Broker broker :brokers){
                            out.writeObject(broker.toString());
                        }
                    }else if(request.matches("hash\n")){    // if client requests value hashing.
                        String value = (String)in.readObject();
                        Broker broker = Broker.this.hash(value);
                        out.writeObject(broker.getIp());   // send broker ip + port with based on hash value
                        out.flush();
                        out.writeObject(broker.getPort());
                        out.flush();
                    }else if(request.matches("new\n")){ // if new video is available send ack to receive the video.
                        out.writeObject("send\n");
                        out.flush();
                    }else if(request.matches("subscribe\n")){
                        String topic = (String)in.readObject();
                        boolean exists = false;
                        for(Publisher pub : Broker.this.registeredPublishers){
                            if(pub.getChannelName().getName().equals(topic)){
                                if(Broker.this.channel_subscriptions.containsKey(consumer.getChannelName())){
                                    Broker.this.channel_subscriptions.get(consumer).add(pub.getChannelName());
                                }else{
                                    Broker.this.channel_subscriptions.put(consumer.getChannelName(), new ArrayList<>());
                                    Broker.this.channel_subscriptions.get(consumer.getChannelName()).add(pub.getChannelName());
                                }
                                exists = true;
                                break;
                            }
                        }
                        if(!exists){    // sub to hashtag.
                            if(Broker.this.hashtag_subscriptions.containsKey(consumer.getChannelName())){
                                Broker.this.hashtag_subscriptions.get(consumer.getChannelName()).add(topic);
                            }
                        }
                    }else if(request.matches("sending-value\n")){
                        request = (String)in.readObject();  // requested string.
                        ArrayList<ArrayList<Value>> videos_to_send = getDataFromPublishers(request);
                        if(videos_to_send.size() == 0)
                            out.writeObject("no-videos-found\n");
                        else{
                            out.writeObject("good\n");
                            out.flush();
                            out.writeObject(videos_to_send.size());
                            for(ArrayList<Value> values : videos_to_send){
                                out.writeObject(values.size());
                                for(Value value : values){
                                    out.writeObject(value);
                                }
                            }
                        }
                    }
                }
            } catch (IOException exception) {
                System.err.println("Client disconnected");
            } catch (ClassNotFoundException classNotFoundException) {
                classNotFoundException.printStackTrace();
            }
        }

        public ArrayList getDataFromPublishers(String request){
            Socket con;
            ObjectInputStream in;
            ObjectOutputStream out;
            ArrayList<ArrayList<Value>> videos_to_send = new ArrayList<>();
            for(Publisher publisher : registeredPublishers){
                try{
                con = new Socket(publisher.getIp(), publisher.getPort());
                in = new ObjectInputStream(con.getInputStream());
                out = new ObjectOutputStream(con.getOutputStream());
                out.writeObject(request);
                String answer = (String)in.readObject();
                if(answer.matches("error\n"))
                    return null;
                else{
                    answer = String.valueOf((Integer)in.readObject());
                    int number_of_videos = Integer.parseInt(answer);
                    for(int i = 0; i < number_of_videos; i++){
                        ArrayList<Value> video = new ArrayList<>();
                        in.readObject();
                        String videoName = (String)in.readObject();
                        int nOfChunks = in.readInt();
                        ArrayList<Value> newVid = new ArrayList<>();
                        for(int j=0; j<nOfChunks; j++) {
                            Value value = (Value)in.readObject();
                            value.getVideoFile().setVideoName(videoName);
                            newVid.add(value);
                        }
                        videos_to_send.add(newVid);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
                }
            }
            return videos_to_send;
        }
    }

    public String toString(){
        return this.brokerId+"-"+this.ip+"-"+this.port;
    }
}