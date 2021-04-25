import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.security.MessageDigest;

public class PublisherImpl implements Publisher{

    String ip;
    @Override
    public void init(int x) {

    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }

    @Override
    public List<Broker> getBrokers() {
        return null;
    }

    @Override
    public void addHashTag(String s) {

    }

    @Override
    public void removeHashTag(String s) {

    }

    @Override
    public List<Broker> getBrokerList() {
        return null;
    }

    @Override
    public Broker hashTopic(String s) {
        List<Broker> brokers = getBrokerList();
        for (Broker br:brokers) {
            System.out.println(2);
        }

        byte[] msg = s.getBytes(StandardCharsets.UTF_8);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.digest(msg);
            byte[] hvalues = md.digest(msg);
            byte[] bvalues = md.digest(msg);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }



        return null;
    }

    @Override
    public void push(String s, Value v) {

    }

    @Override
    public void notifyFailure(Broker broker) {

    }

    @Override
    public void notifyBrokersForHashTags(String s) {

    }

    @Override
    public ArrayList<Value> generateChunks(String s) {
        return null;
    }

    public String getIp() {
        return ip;
    }


    private static class Operations extends Thread{

        private Socket connection;
        public Operations(Socket connection){
            this.connection = connection;
        }

        @Override
        public void run(){


        }

    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        ServerSocket providerSocket = new ServerSocket(port);

        while (true){
            Socket broker_connection = providerSocket.accept();
            Thread operations = new Operations(broker_connection);
            operations.start();
        }
    }
}
