package com.distributedsystems.pubsubsystem;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class Broker {
    private String ip;
    private int port;
    private ServerSocket serverSocket;
    private ArrayList<Broker> brokers;
    private LinkedBlockingQueue<Consumer> registeredConsumers = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Publisher> registeredPublishers = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ArrayList<Value>> videos = new LinkedBlockingQueue<>();

    public static void main(String [] args){

        Broker broker = new Broker("127.0.0.1" ,Integer.parseInt(args[0]));
        broker.update_broker_data();
        broker.connect();
    }
    Broker(String ip, int port){
        this.ip = ip;
        this.port = port;
    }


    public void connect() {
        try{
            this.serverSocket = new ServerSocket(this.port, 20);
            serverSocket.setReuseAddress(true);
            while(true){
                Socket clientSocket = this.serverSocket.accept();
                System.out.println("Client connected " + clientSocket.getInetAddress().getHostAddress());
                Thread clientThread = new Connection(clientSocket, videos, registeredConsumers, registeredPublishers);
                clientThread.start();
            }
        } catch (IOException e){
            e.printStackTrace();
            this.disconnect();
        }
    }

    private void update_broker_data(){
        brokers = new ArrayList<>();
        Properties properties = new Properties();
        String broker_cfg = "data/broker_data.cfg";
        try{
            InputStream is = new FileInputStream(broker_cfg);
            properties.load(is);
        }
        catch (IOException e){
            e.printStackTrace();
        }
        String[] ips = properties.getProperty("brokers.ip").split(",");
        String[] ports = properties.getProperty("brokers.port").split(",");
        for(int i = 0; i < ips.length; i++){
            brokers.add(new Broker(ips[i], Integer.parseInt(ports[i])));
        }
    }

    public void disconnect() {
        try {
            this.serverSocket.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public Info getBrokerInfo(Consumer consumer){
        Info info = new Info();
        try{
            for(Broker broker : brokers){
                if(broker.getPort() != this.getPort()){
                    info.addBroker(broker.getPort(), broker.getIp());
                    Socket socket = new Socket(broker.getIp(), broker.getPort());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    out.writeObject("broker\n");
                    out.flush();
                    out.writeObject(consumer.getChannelName().getName());
                    out.flush();
                    info.update((Info)in.readObject());
                }
            }
            info.addBroker(this.getPort(), this.getIp());
            synchronized (videos){
                for(ArrayList<Value> value : videos){
                    info.addBrokerData(this.getPort(), value.get(0).getChannelName().getName());
                    for(String hs : value.get(0).getVideoFile().getAssociatedHashtags()){
                        info.addBrokerData(this.getPort(), hs);
                    }
                }
            }
            synchronized (registeredPublishers){
                for(Publisher publisher1 : registeredPublishers){
                    info.addBrokerData(Broker.this.getPort(), publisher1.getChannelName().getName());
                }
            }
            synchronized (registeredConsumers){
                for(Consumer consumer1 : registeredConsumers){
                    if(consumer1.getChannelName().getName().equals(consumer.getChannelName().getName())){
                        info.addSubscriber(this.getPort());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return info;
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
        private LinkedBlockingQueue<Consumer> consumers;
        private LinkedBlockingQueue<Publisher> publishers;
        Publisher publisher;
        Consumer consumer;
        public Connection(Socket connection, LinkedBlockingQueue<ArrayList<Value>> videos, LinkedBlockingQueue<Consumer> consumers, LinkedBlockingQueue<Publisher> publishers){
            this.connection = connection;
            this.videos = videos;
            this.consumers = consumers;
            this.publishers = publishers;
            try{
                out = new ObjectOutputStream(connection.getOutputStream());
                in = new ObjectInputStream(connection.getInputStream());
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        @Override
        public void run(){
            String request;
            try {
                request = (String)in.readObject();
                if(request.matches("broker\n")) {
                    String consumer_str = (String)in.readObject();
                    Info broker_data = new Info();
                    broker_data.addBroker(Broker.this.getPort(), Broker.this.getIp());
                    synchronized (videos){
                        for(ArrayList<Value> values : videos){
                            Broker temp_broker = Broker.this.hash(values.get(0).getChannelName().getName());
                            if(temp_broker.getPort() == Broker.this.getPort())
                                broker_data.addBrokerData(Broker.this.getPort(), values.get(0).getChannelName().getName());
                            for(String hashtag : values.get(0).getVideoFile().getAssociatedHashtags()){
                                temp_broker = Broker.this.hash(hashtag);
                                if(temp_broker.getPort() == Broker.this.getPort())
                                    broker_data.addBrokerData(Broker.this.getPort(), hashtag);
                            }
                        }
                    }
                    synchronized (publishers){
                        for(Publisher publisher1 : publishers){
                            broker_data.addBrokerData(Broker.this.getPort(), publisher1.getChannelName().getName());
                        }
                    }
                    synchronized (consumers){
                        for(Consumer c : consumers){
                            if(c.getChannelName().getName().equals(consumer_str)){
                                broker_data.addSubscriber(Broker.this.getPort());
                            }
                        }
                    }
                    out.writeObject(broker_data);
                    out.flush();
                }else if(request.matches("consumer\n")) {
                    ChannelName channel = (ChannelName) in.readObject();
                    consumer = new Consumer(channel.getName());
                    boolean exists = false;
                    for(Consumer con : registeredConsumers){
                        if(con.getChannelName().getName().equals(channel.getName())){
                            exists = true;
                        }
                    }
                    out.writeBoolean(exists);
                    out.flush();
                    request = (String) in.readObject();
                    if(request.matches("listener\n")){
                        send_to_subscribers();
                    }else{
                        wait_for_requests();
                    }
                }else if(request.matches("publisher\n")){
                    ChannelName channel = (ChannelName) in.readObject();
                    publisher = new Publisher(channel.getName());
                    wait_for_publisher();
                }else if(request.matches("hash\n")){    // if client requests value hashing.
                    String value = (String)in.readObject();
                    Broker broker = Broker.this.hash(value);
                    out.writeObject(broker.getIp());   // send broker ip + port with based on hash value
                    out.flush();
                    out.writeObject(String.valueOf(broker.getPort()));
                    out.flush();
                }
            } catch (IOException exception) {
                System.err.println("Client disconnected");
            } catch (ClassNotFoundException classNotFoundException) {
                classNotFoundException.printStackTrace();
            }
        }

        private void wait_for_publisher(){     // publisher connection waiting for new videos.
            String request;
            try {
                while (true) {
                    request = (String) in.readObject();
                    if(request.matches("new-video\n")){
                        boolean exists = false;
                        synchronized (publishers){
                            for(Publisher pub : publishers){
                                if(pub.getChannelName().getName().equals(publisher.getChannelName().getName())){
                                    exists = true;
                                }
                            }
                            if(!exists){
                                publisher = new Publisher(publisher.getChannelName().getName());
                                publishers.offer(publisher);
                            }
                        }
                        out.writeObject("ack\n");
                        out.flush();
                    }else if(request.matches("video\n")){
                        ArrayList<Value> newVid = new ArrayList<>();
                        Date date_uploaded = (Date) in.readObject();
                        int nOfHashtags = in.readInt();
                        ArrayList<String> hs = new ArrayList<>();
                        for(int i=0; i<nOfHashtags;i++){
                            hs.add((String) in.readObject());
                        }
                        int nOfChunks = in.readInt();
                        for(int i=0; i<nOfChunks; i++) {
                            Value value = (Value)in.readObject();
                            value.setDate_uploaded(date_uploaded);
                            for(String s : hs){
                                value.addHashtag(s);
                            }
                            newVid.add(value);
                        }
                        synchronized (videos){
                            videos.offer(newVid);
                            videos.notifyAll();
                        }
                        System.out.println("VIDEO " + newVid.get(0) + " RECEIVED");
                    }else if(request.matches("delete\n")){
                        String title = (String)in.readObject();
                        synchronized (videos){
                            for(ArrayList<Value> values : videos){
                                if(values.get(0).toString().equals(title)){
                                    System.out.println("VIDEO " + values.get(0) + " DELETED");
                                    videos.remove(values);
                                    break;
                                }
                            }
                        }
                    }else if(request.matches("init\n")){
                        synchronized (publishers){
                            boolean registered = false;
                            for(Publisher publisher1 : publishers){
                                if(publisher1.getChannelName().getName().equals(publisher.getChannelName().getName())){
                                    registered = true;
                                }
                            }
                            if(!registered)
                                publishers.add(publisher);
                        }
                        synchronized (videos){
                            int total = 0;
                            for(ArrayList<Value> values : videos){
                                if(values.get(0).getChannelName().getName().equals(publisher.getChannelName().getName())){
                                    total+=1;
                                }
                            }
                            out.writeObject(total);
                            out.flush();
                            for(ArrayList<Value> values : videos){
                                if(values.get(0).getChannelName().getName().equals(publisher.getChannelName().getName())){
                                    Value value = values.get(0);
                                    value.discardVideo();
                                    out.writeObject(value);
                                    out.flush();
                                }
                            }
                        }
                    }
                }
            }catch (IOException | ClassNotFoundException e){
                System.err.println("Disconnected");
            }
        }
        private void wait_for_requests(){
            String request;
            try{
                while(true){
                    request = (String)in.readObject();
                    if(request.matches("^channel.*$")){
                        sendVideoRequested(findVideo(request));
                    }else if (request.matches("send-brokers\n")){
                        Info brokerInfo = Broker.this.getBrokerInfo(consumer);
                        out.writeObject(brokerInfo);
                        out.flush();
                    }else if(request.matches("subscribe\n")){
                        String topic = (String)in.readObject();
                        boolean exists = false;
                        synchronized (consumers){
                            consumer.subscribe(topic);
                            for(Consumer c : consumers){
                                if(c.getChannelName().getName().equals(consumer.getChannelName().getName())){
                                    c.subscribe(topic);
                                    exists = true;
                                }
                            }
                            if(!exists){
                                consumers.offer(consumer);
                            }
                        }
                    }else if(request.matches("check-subscription\n")){
                        String subscription = (String)in.readObject();
                        boolean is_subscribed = false;
                        synchronized (consumers){
                            for(Consumer c : consumers){
                                if(c.getChannelName().getName().equals(consumer.getChannelName().getName())){
                                    for(String sub : c.getSubscriptions()){
                                        if(sub.equals(subscription)){
                                            is_subscribed = true;
                                        }
                                    }
                                }
                            }
                            out.writeBoolean(is_subscribed);
                            out.flush();
                        }
                    }else if(request.matches("unsubscribe\n")){
                        String topic = (String)in.readObject();
                        synchronized (consumers){
                            for(Consumer c : consumers){
                                if(c.getChannelName().getName().equals(consumer.getChannelName().getName())){
                                    c.unsubscribe(topic);
                                }
                                if(c.getSubscriptions().isEmpty()){
                                    consumers.remove(c);
                                    break;
                                }
                            }
                        }
                        synchronized (videos){
                            videos.notifyAll(); // notify to check if consumer doesn't have active subscriptions.
                        }
                    }else{  // if request is of a topic (channel name/hashtag).
                        ArrayList<String> video_data = getVideoData(request);
                        sendVideosFound(video_data);
                    }
                }
            }catch (IOException | ClassNotFoundException e){
                System.err.println("Consumer disconnected");
            }
        }
        private void send_to_subscribers(){
            try{
                // send all related videos of the last 24 hours.
                Date maxDate=null;
                System.err.println(consumer.getChannelName().getName());
                synchronized (videos){
                    for(ArrayList<Value> values : videos){
                        if(values.get(0).getChannelName().getName().equals(consumer.getChannelName().getName())){
                            System.err.println(2);
                            continue;
                        }
                        if(maxDate == null){
                            maxDate = values.get(0).getDate_uploaded();
                        }else{
                            if(maxDate.getTime() < values.get(0).getDate_uploaded().getTime()){
                                maxDate = values.get(0).getDate_uploaded();
                            }
                        }
                        long diff = new Date().getTime() - values.get(0).getDate_uploaded().getTime();
                        long days = diff / (24 * 60 * 60 * 1000);
                        if(days <= 1){
                           send_video(values);
                        }else
                            System.err.println(3);
                    }
                }
                while (true){
                    LinkedBlockingQueue<String> subscriptions;
                    synchronized (videos){
                        videos.wait();
                    }
                    synchronized (consumers){   // check if consumer unsubscribed from all topics of this broker.
                        boolean exists = false;
                        for(Consumer c : consumers){
                            if(c.getChannelName().getName().equals(consumer.getChannelName().getName())){
                                exists = true;
                                subscriptions = c.getSubscriptions();
                                if(subscriptions.isEmpty()){
                                    this.in = null;
                                    this.out = null;
                                    this.connection.close();
                                    break;
                                }
                            }
                        }
                        if(!exists){
                            this.in = null;
                            this.out = null;
                            this.connection.close();
                        }
                    }
                    synchronized (videos){
                        for(ArrayList<Value> values : videos){
                            if(values.get(0).getChannelName().getName().equals(consumer.getChannelName().getName())){
                                continue;
                            }
                            if(maxDate == null || values.get(0).getDate_uploaded().getTime() > maxDate.getTime()){
                                send_video(values);
                                if(maxDate == null || values.get(0).getDate_uploaded().getTime() > maxDate.getTime())
                                    maxDate = values.get(0).getDate_uploaded();
                            }

                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        private void send_video(ArrayList<Value> values){
            try{
                out.writeObject("new-video\n");
                out.flush();
                String answer = (String) in.readObject();
                if(answer.matches("ack\n")){
                    out.writeObject(values.get(0).toString());
                    out.flush();
                    answer = (String)in.readObject();
                    if(answer.matches("ack\n")){
                        out.writeObject(values.size());
                        out.flush();
                        for(int i = 0; i<values.size(); i++){
                            out.writeObject(values.get(i));
                            out.flush();
                        }
                    }
                }
            }catch (Exception e){
                System.err.println("Disconnected");
            }
        }
        private ArrayList<String> getVideoData(String s){
            ArrayList<String> data = new ArrayList<>();
            synchronized (videos){
                for(ArrayList<Value> values : videos){
                    ArrayList<String> hashtags = new ArrayList<>();
                    for(String hs : values.get(0).getVideoFile().getAssociatedHashtags()){
                        Broker temp = Broker.this.hash(hs);
                        if(temp.getPort() == Broker.this.getPort()){
                            hashtags.add(hs);
                        }
                    }
                    if(values.get(0).getChannelName().getName().equals(s) || hashtags.contains(s)){
                        data.add(values.get(0).toString());
                    }
                }
            }
            return data;
        }
        private void sendVideoRequested(ArrayList<Value> video){

            try{
                if(video.size() == 0) {
                    out.writeObject("no-videos-found\n");
                    out.flush();
                }else{
                    out.writeObject("video-found\n");
                    out.flush();
                    out.writeObject(video.size());
                    for(Value value : video){
                        out.writeObject(value);
                        out.flush();
                    }
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        private ArrayList<Value> findVideo(String s){
            String[] video_data = s.split("\\|");
            ArrayList<Value> video_to_send = new ArrayList<>();
            for(ArrayList<Value> values : videos){
                if(values.get(0).getChannelName().getName().equals(video_data[0].split("=")[1]) && values.get(0).getVideoFile().getVideoName().equals(video_data[1].split("=")[1])){
                    video_to_send = values;
                }
            }
            return video_to_send;
        }
        private void sendVideosFound(ArrayList<String> videos){
            try{
                if(videos.isEmpty()){
                    out.writeObject("no-videos-found\n");
                    out.flush();
                }else{
                    out.writeObject("videos-found\n");
                    out.flush();
                    out.writeObject(videos.size());
                    for(String video : videos){
                        out.writeObject(video);
                        out.flush();
                    }
                }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}