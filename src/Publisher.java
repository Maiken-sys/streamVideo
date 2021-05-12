import com.google.gson.JsonObject;
import ucar.nc2.util.IO;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Publisher implements AppNodeImpl {
    private ChannelName channel;
    private int port;
    private String ip;
    private ArrayList<Value> publishedVideos = new ArrayList<Value>();
    private Socket pubSocket;        //it is the socket that client uses to communicate with server
    ObjectOutputStream out = null;      // streams for the main broker
    ObjectInputStream in = null;

    public Publisher(int port, String name){
        this.port = port;
        this.channel = new ChannelName(name);
        initialize_structures();
    }

    @Override
    public List<Broker> getBrokers() {
        try{
            out.writeObject("send-brokers\n");
            out.flush();
            return (ArrayList<Broker>)in.readObject();
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    //*******************************************************************************
    //Node override methods****************************************************
    //*******************************************************************************
    @Override
    public void init(int x) { }


    private void initialize_structures(){
        final File folder = new File("savedVideos");
        for(final File file : folder.listFiles()){

        }
    }

    @Override
    public void connect() {
        try {
            pubSocket = new Socket("127.0.0.1", this.port);
            this.ip = this.pubSocket.getInetAddress().getHostAddress();
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
        if (!this.channel.getHashtagsPublished().contains(s)){
            this.channel.getHashtagsPublished().add(s);
        }
        this.notifyBrokersForHashTags(s);
    }

    @Override
    public void removeHashTag(String s) {
        this.channel.getHashtagsPublished().remove(s);
    }

    @Override
    public List<Broker> getBrokerList() {
        try {
            out.writeObject("send-brokers\n");    // ask for brokers list
            out.flush();
            return (ArrayList<Broker>)in.readObject();
        }catch (ClassNotFoundException | IOException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Broker hashTopic(String s) {
        try {
            out.writeObject("hash\n");
            out.flush();
            out.writeObject(s);
            out.flush();
            return (Broker)in.readObject();
        } catch (IOException | ClassNotFoundException ioException) {
            ioException.printStackTrace();
        }
        return null;
    }

    @Override
    public void push(String hashtag, String videoName, ObjectOutputStream out)  {   //bad practice but f it
        ArrayList<VideoFile> chunks;
        chunks = this.channel.getUserVideoFilesMap().get(videoName);
        if(chunks == null)
            return;
        System.out.println("Retrieving chunks of video with title " + videoName + "....");

        try{
            out.writeObject("video\n");
            out.flush();
            out.writeObject(videoName);
            out.writeInt(chunks.size());
            out.flush();
            for (VideoFile chunk : chunks){
                Value value = new Value(chunk, channelName);
                out.writeObject(value);
                out.flush();
            }

        }catch(IOException e){
            System.err.println("Your push method has an IO problem");
            e.printStackTrace();
        }
    }


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

    public void addVideo(String videoName){
        try{
            Path path = Paths.get("savedVideos"+ videoName + ".mp4");
            if(!Files.exists(path)){
                System.err.println("Video does not exist..");
                return;
            }

            byte[] bFile = Files.readAllBytes(path);
            this.generateChunks(bFile, videoName);
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
    }



    @Override
    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName) {
        ArrayList<VideoFile> list = new ArrayList<VideoFile>();
        int chunk_size = 524288; //512kb per chunk

        int start = 0;
        while(start < video.length){
            int end = Math.min(video.length, start + chunk_size);
            VideoFile chunk = new VideoFile(end - start);
            chunk.videoFileChunk = Arrays.copyOfRange(video, start, end);
            list.add(chunk);
            start += chunk_size;
        }
        this.channel.getUserVideoFilesMap().put(videoName, list);
        return list;
    }


    public int getPort() {
        return this.port;
    }



    private class Connection extends Thread{

        Socket socket = null;
        private int port;
        private String ip;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        String request;
        public Connection(String ip, int port){
            this.ip = ip;
            this.port = port;
        }

        public void run(){
            try {
                socket = new Socket(ip, port);
                in = new ObjectInputStream(socket.getInputStream());
                out = new ObjectOutputStream(socket.getOutputStream());
                while(true){
                    request = (String)in.readObject();

                }

            } catch (IOException | ClassNotFoundException ioException) {
                ioException.printStackTrace();
            }
        }
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