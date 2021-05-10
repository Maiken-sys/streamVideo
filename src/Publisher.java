import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Publisher extends Thread implements AppNodeImpl {
    private ChannelName channel;
    private int port;
    private String ip;
    private Socket pubSocket;        //it is the socket that client uses to communicate with server
    ObjectOutputStream out = null;
    ObjectInputStream in = null;

    public Publisher(int port){
        this.port = port;
        this.channel = new ChannelName("channel1");

    }

    public static void main(String [] args) throws IOException {

        Publisher pub = new Publisher(4321);
        pub.connect();
        pub.addVideo("viral", pub.channel, "newVid");
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
    public void push(String s, Value v) {

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

    public void addVideo(String hashtag, ChannelName channel, String videoName) throws IOException {
        File file = new File("C:\\Users\\Admin\\Downloads\\test.mp4");
        FileInputStream fileInputStream = new FileInputStream(file);
        this.addHashTag(hashtag);
        this.notifyBrokersForHashTags(hashtag);
        byte[] bFile = new byte[(int) file.length()];

        try{
            fileInputStream.read(bFile);
            fileInputStream.close();
            //for (int i = 0; i < bFile.length; i++){ System.out.println((char) bFile[i] + "   " + bFile[i] ); }

        }catch(IOException e){
            e.printStackTrace();
        }



        this.generateChunks(bFile);
    }



    @Override
    public ArrayList<VideoFile> generateChunks(byte[] video) {
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
