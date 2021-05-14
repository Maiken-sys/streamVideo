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


    public Publisher(int port, String channelname){
        this.port = port;
        this.channel = new ChannelName(channelname);

    }

    public static void main(String [] args) throws IOException {

        Publisher pub = new Publisher(4321, "m");
        pub.connect();
        pub.start();


        //Publisher pub2 = new Publisher(4321, "n");
        //pub2.start();
        //pub2.connect();
        //pub2.push("lampai", "test.mp4");


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
            out = new ObjectOutputStream(this.pubSocket.getOutputStream());
            in = new ObjectInputStream(this.pubSocket.getInputStream());
            this.ip = this.pubSocket.getInetAddress().getHostAddress();
        } catch (UnknownHostException e) {
            System.err.println("Προσπαθείς να συνδεθεις σε άγνωστο host!!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void disconnect() {
        try{
            System.out.println("Client Closing Connection");
            in.close();
            out.close();
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
            this.addVideo(videoName);           //Calls method addVideo(line 174) that reads the mp4 file from the pc
        }catch (IOException e1){
            System.err.println("Couldnt add video...");
            e1.printStackTrace();
        }


        ArrayList<VideoFile> chunks = new ArrayList<VideoFile>(); //here we will save temporary the chunks
        chunks = this.channel.getUserVideoFilesMap().get(videoName);    //retrieving the chunks from channel based on the video's name
        System.out.println("Retrieving chunks of video with title " + videoName + "....");

        try{
            out.writeInt(1);
            out.flush();
            out.writeInt(chunks.size());    //notify broker for list size
            out.flush();
            for (VideoFile chunk : chunks){
                Value value = new Value(chunk);
                out.writeObject(value);     //sending each chunk to broker
                out.flush();
            }

            System.out.println("Server should receive " + chunks.size() + "chunks");


        }catch(IOException e){
            System.err.println("Your push method has an IO problem");
            e.printStackTrace();
        }


   }


    @Override
    public void notifyFailure(Broker broker) {

    }

    @Override
    public void notifyBrokersForHashTags(String hashtag) {
        try{
            this.out.writeObject(hashtag);
            this.out.writeObject(this.channel);
        }catch (IOException e){
            System.out.println("Hashtag not given");
        }

    }

    public void addVideo(String videoName) throws IOException {
        Path path = Paths.get("C:\\Users\\Admin\\Downloads\\" + videoName);  //locate the file that publisher want to push
        byte[] bFile = Files.readAllBytes(path);              //reads the mp4 file into a byte[] array

        this.generateChunks(bFile, videoName);              //Calls method generateChunks (line 184)
    }



    @Override
    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName) {
        ArrayList<VideoFile> list= new ArrayList<VideoFile>();      //chunks will be saved on ArrayList list
        int chunk_size = 524288; //512kb per chunk

        int start = 0;      //variable start locates the 1st byte of each chunk
        while(start< video.length){
            int end = Math.min(video.length, start + chunk_size);  //variable end locates the last byte of each chunk
            VideoFile chunk = new VideoFile(end - start); //connects every chunk with a VideoFile class object
            chunk.videoFileChunk = Arrays.copyOfRange(video, start, end); //puts the chunk to the byte array of the object
            list.add(chunk);
            start += chunk_size;
        }
        this.channel.getUserVideoFilesMap().put(videoName, list);
        return list;
    }


    public int getPort() {
        return this.port;
    }

    public void run() {
        int option;

        while(true){
            System.out.println("Select option: ");
            System.out.println("1.Upload a video");
            System.out.println("2.Delete a video");
            System.out.println("3.Exit Program");
            Scanner sc = new Scanner(System.in);
            option = sc.nextInt();
            sc.nextLine();

            switch(option){
                case 1:
                    System.out.println("Choose the video you want to upload: ");
                    String title = sc.nextLine();
                    System.out.println("Choose the hashtag you want to add: ");
                    String hashtag = sc.nextLine();
                    this.push(hashtag, title);
                    break;

                case 3:
                    try {
                        out.writeInt(3);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    this.disconnect();
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
