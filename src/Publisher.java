import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.security.MessageDigest;
import java.util.Scanner;

public class Publisher extends Thread implements AppNodeImpl {
    private ChannelName channel;
    private int port;
    private String ip;
    private Socket pubSocket;        //it is the socket that client uses to communicate with server
    ObjectOutputStream out = null;
    ObjectInputStream in = null;

    public Publisher(int port){
        this.port = port;
    }

    public static void main(String [] args) throws IOException {

        Publisher pub = new Publisher(4321);
        pub.connect();
        pub.addVideo("viral", pub.channel);
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
        }finally {
           this.disconnect();
        }
    }

    @Override
    public void disconnect() {
        try {
            this.in.close();
            this.out.close();
            this.pubSocket.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void updateNodes() {

    }


    //*******************************************************************************
    //Publisher-Only methods**********************************************************
    //*******************************************************************************
    @Override
    public void addHashTag(String s){
        this.channel.getHashtagsPublished().add(s);
        this.notifyBrokersForHashTags(s);
    }

    @Override
    public void removeHashTag(String s) {
        this.channel.getHashtagsPublished().remove(s);
    }

    @Override
    public List<Broker> getBrokerList() {
        return null;
    }

    @Override
    public Broker hashTopic(String s) {
        return null;
    }

   @Override
    public void push(String s, Value v) {}
/*         try {
            //this.out = new ObjectOutputStream();
        } catch (IOException ex) {
            System.out.println("File not found. ");
            ex.printStackTrace();
        }

        byte[] bytes = new byte[16*1024];

        try{
            int count = 0;
            while((count = in.read(bytes)) > 0){
                out.write(bytes, 0, count);
                System.out.println("push method");
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    } */

    @Override
    public void notifyFailure(Broker broker) {

    }

    @Override
    public void notifyBrokersForHashTags(String s) {
        try{
            this.out.writeObject(s);
            this.out.writeObject(this.channel);
        }catch (IOException e){
            System.out.println("Hashtag not given");
        }

    }

    public byte[]  addVideo(String hashtag, ChannelName channel) throws IOException {
        File file = new File("/home/mnanos/Videos/testVid.mp4");
        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] bFile = new byte[(int) file.length()];

        try{
            fileInputStream.read(bFile);
            fileInputStream.close();
            for (int i = 0; i < bFile.length; i++)
            {
                System.out.print((char) bFile[i]);
            }

        }catch(IOException e){
            e.printStackTrace();
        }
        return null;
    }



    @Override
    public ArrayList<Value> generateChunks(VideoFile video) {
        return null;
    }


    public int getPort() {
        return this.port;
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
