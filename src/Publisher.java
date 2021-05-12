import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Publisher extends Thread implements AppNodeImpl {
    private ChannelName channel;
    private int port;
    private String ip;
    private Socket pubSocket;        //it is the socket that client uses to communicate with server
    ArrayList<VideoFile> video;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


    public Publisher(int port){
        this.port = port;
        System.out.println("Please enter your channel Name.");
        Scanner sc = new Scanner(System.in);
        String name = sc.nextLine();
        this.channel = new ChannelName(name);

    }

    public static void main(String [] args) throws IOException {

        Publisher pub = new Publisher(4321);
        pub.connect();
        pub.push("gamw","test.mp4");

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
        return null;
    }

    @Override
    public Broker hashTopic(String s) {
        return null;
    }

   @Override
    public void push(String hashtag, String videoName)  {
        try{
            this.addVideo(videoName);
        }catch (IOException e1){
            System.err.println("Couldnt add video...");
            e1.printStackTrace();
        }


        ArrayList<VideoFile> chunks = new ArrayList<VideoFile>();
        chunks = this.channel.getUserVideoFilesMap().get(videoName);
        System.out.println("Retrieving chunks of video with title " + videoName + "....");

        try{
            out.writeInt(chunks.size());
            out.flush();
            for (VideoFile chunk : chunks){
                Value value = new Value(chunk);
                out.writeObject(value);
                out.flush();

            }


            System.out.println("Server should receive " + chunks.size() + "chunks");


        }catch(IOException e){
            System.err.println("Your push method has an IO problem");
            e.printStackTrace();
        }finally{
            try{
                System.out.println("Client Closing Connection");
                in.close();
                out.close();
            }catch (IOException e){
                e.printStackTrace();
            }
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

    public void addVideo(String videoName) throws IOException {
        //File file = new File("C:\\Users\\Admin\\Downloads\\test.mp4");
        //FileInputStream fileInputStream = new FileInputStream(file);
        //byte[] bFile = new byte[(int) file.length()];
        Path path = Paths.get("C:\\Users\\Admin\\Downloads\\" + videoName);
        byte[] bFile = Files.readAllBytes(path);
        //for (int i = 0; i < bFile.length; i++){ System.out.println((char) bFile[i] + "   " + bFile[i] ); }
        /*
        try{
            fileInputStream.read(bFile);
            fileInputStream.close();
            //for (int i = 0; i < bFile.length; i++){ System.out.println((char) bFile[i] + "   " + bFile[i] ); }

        }catch(IOException e){
            e.printStackTrace();
        }

         */

        this.generateChunks(bFile, videoName);
    }



    @Override
    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName) {
        ArrayList<VideoFile> list= new ArrayList<VideoFile>();
        int chunk_size = 524288; //512kb per chunk

        int start = 0;
        while(start< video.length){
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
