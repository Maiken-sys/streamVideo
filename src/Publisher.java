import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.security.MessageDigest;
import java.math.BigInteger;

public class Publisher implements AppNodeImpl {
    private int port;
    public Socket pubSocket;        //it is the socket that client uses to communicate with server
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


    public Publisher(int port){
        this.port = port;
    }


    //*******************************************************************************
    //Node override methods****************************************************
    //*******************************************************************************
    @Override
    public void init(int x) { }

    @Override
    public List<Broker> getBrokers() {
        return null;
    }

    @Override
    public void connect() {
        try {
            pubSocket = new Socket("127.0.0.1", this.port);
            out = new ObjectOutputStream(pubSocket.getOutputStream());
            in = new ObjectInputStream(pubSocket.getInputStream());
        } catch (UnknownHostException e) {
            System.err.println("Προσπαθείς να συνδεθεις σε άγνωστο host!!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }


    //*******************************************************************************
    //Publisher-Only methods**********************************************************
    //*******************************************************************************
    @Override
    public void addHashTag(String s) {
        channelName.addHashtag(s);
        notifyBrokersForHashTags(s);
    }

    @Override
    public void removeHashTag(String s) {
        channelName.removeHashtag(s);
    }

    @Override
    public List<Broker> getBrokerList() {
        return null;
    }

    @Override
    public Broker hashTopic(String s) {
        // take the md5 hex hash of hashtag
        // take md5 hex hash of brokers' ip + port
        // create a bigint from the hash code
        // do hashtag_hash % highest broker_hash
        // return first broker with value higher than hashtag_hash
        BigInteger hash_value = md5(s);
        ArrayList<Broker> brokers = new ArrayList<>(getBrokerList());
        ArrayList<BigInteger> brokers_hash = new ArrayList<>();
        for(Broker b : brokers){
            BigInteger value = md5(String.valueOf(Integer.parseInt(b.getIp().split("/.").toString()) + b.getPort()));
            brokers_hash.add(value);
        }
        Collections.sort(brokers_hash);
        hash_value = hash_value.mod(brokers_hash.get(brokers_hash.size() - 1));
        for(BigInteger i : brokers_hash){
            if(hash_value.compareTo(i) == -1){
                for(Broker b : brokers){
                    if(md5(String.valueOf(Integer.parseInt(b.getIp().split("/.").toString()) + b.getPort())).equals(i))
                        return b;
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

    @Override
    public void push(String s, Value v) {

    }

    @Override
    public void notifyFailure(Broker broker) {

    }

    @Override
    public void notifyBrokersForHashTags(String s) {
        Broker broker = hashTopic(s);
    }

    public int getPort() {
        return this.port;
    }

    @Override
    public ArrayList<Value> generateChunks(String s) {
        return null;
    }



    //*******************************************************************************
    //Consumer-Only methods**********************************************************
    //*******************************************************************************
    @Override
    public void register(Broker broker, String s) {

    }

    @Override
    public void disconnect(Broker broker, String s) {

    }

    @Override
    public void playData(String s, Value v) {

    }
}