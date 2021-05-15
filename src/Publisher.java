import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Publisher implements AppNodeImpl, Serializable {
    private ChannelName channelName;
    private String ip;
    private LinkedBlockingQueue<Value> videos_for_channel = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Value> videos_for_hashtags = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<HashMap<Broker, String>> notify = new LinkedBlockingQueue<>();


    private Object lock1 = new Object();
    private Object lock2 = new Object();


    private Socket pubSocket;        //it is the socket that client uses to communicate with server
    ObjectOutputStream out = null;      // streams for the main broker
    ObjectInputStream in = null;
    static Scanner sc = new Scanner(System.in);
    private ArrayList<Broker> broker_connections = new ArrayList<>();

    public Publisher(String name){
        this.channelName = new ChannelName(name);

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


    private void initialize_structures(){       // connect to hash(username) broker to receive user's data.
        try {
            Broker broker = hashTopic(channelName.getName());   // get broker that hashes to name.
            pubSocket.close();
            out = null;
            in = null;
            broker_connections.add(broker);
            Thread connection = new Connection(broker.getIp(), broker.getPort(), videos_for_channel, videos_for_hashtags, true);   // connection that reads broker requests related to name.
            connection.start();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void connect() {
        try {
            pubSocket = new Socket("127.0.0.1", 4321);  // connect to random broker
            this.ip = this.pubSocket.getInetAddress().getHostAddress();
            out = new ObjectOutputStream(pubSocket.getOutputStream());
            in = new ObjectInputStream(pubSocket.getInputStream());
            initialize_structures();
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
        if (!this.channelName.getHashtagsPublished().contains(s)){
            this.channelName.addHashtag(s);
        }
        this.notifyBrokersForHashTags(s);
    }

    @Override
    public void removeHashTag(String s) {
        this.channelName.getHashtagsPublished().remove(s);
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
            String broker_ip = (String) in.readObject();
            int broker_port = (Integer) in.readObject();
            return new Broker(broker_ip, broker_port);
        } catch (IOException | ClassNotFoundException ioException) {
            ioException.printStackTrace();
        }
        return null;
    }

    @Override
    public void push(String key, Value videoName, ObjectOutputStream out)  {
        ArrayList<VideoFile> chunks = this.generateChunks(videoName.getVideoFile().getVideoFileChunk(), videoName.getVideoFile().getVideoName());
        if(chunks == null) {
            return;
        }
        System.out.println("Retrieving chunks of video with title " + videoName.getVideoFile().getVideoName() + "....");
        try{
            out.writeObject("video\n");
            out.flush();
            out.writeObject(videoName.getVideoFile().getVideoName());
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
    public void notifyFailure(ObjectOutputStream out) {
        try {
            out.writeObject("404");
            out.flush();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void notifyBrokersForHashTags(String s) {
        try{
            Broker broker = hashTopic(s);
            boolean already_connected = false;
            for(Broker broker1 : broker_connections){
                if(broker1.getIp().equals(broker.getIp()) && broker1.getPort() == broker.getPort()){    // if already connected to broker notify thread to send new video.
                    already_connected = true;
                }
            }
            if(!already_connected){
                broker_connections.add(broker);
                new Connection(broker.getIp(), broker.getPort(), videos_for_channel, videos_for_hashtags).start();   // send video
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void addVideo(){
        int int_answer = -1;
        String str_answer = "";
        ArrayList<String> files = listFilesForFolder(new File("savedVideos"));
        System.out.println("Select video to upload [1-" + files.size() + "]");
        System.out.println("===========================");
        for(int i=0; i<files.size();i++){
            System.out.println(i+1 + "." + " " + files.get(i));
        }
        System.out.println("0. Exit");
        while (int_answer < 0 || int_answer > files.size()){
            try{
                int_answer = Integer.parseInt(sc.nextLine());
            }catch (Exception e){}
        }
        if(int_answer == 0)
            return;
        VideoFile newVid = new VideoFile("savedVideos/"+files.get(int_answer-1));
        do{
            System.out.println("Add Hashtag? [y/n]");
            str_answer = sc.nextLine();
            if(str_answer.matches("y|Y")){
                System.out.println("Enter hashtag: ");
                newVid.addHashtag(sc.nextLine().strip());
            }
        }while (!str_answer.matches("n|N"));
        System.out.println("Video added!");
        videos_for_channel.offer(new Value(newVid, channelName));
        videos_for_hashtags.offer(new Value(newVid, channelName));
        ArrayList<String> hashtags = newVid.getAssociatedHashtags();
        for(String hs : hashtags){
            notifyBrokersForHashTags(hs);
        }
    }

    public static ArrayList<String> listFilesForFolder(final File folder) {
        ArrayList<String> files = new ArrayList<String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                files.add(fileEntry.getName());
            }
        }
        return files;
    }

    @Override
    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName) {
        ArrayList<VideoFile> list = new ArrayList<VideoFile>();
        int chunk_size = 524288; //512kb per chunk

        int start = 0;
        while(start < video.length){
            int end = Math.min(video.length, start + chunk_size);
            VideoFile chunk = new VideoFile(end - start, channelName.getChannelName());
            chunk.videoFileChunk = Arrays.copyOfRange(video, start, end);
            list.add(chunk);
            start += chunk_size;
        }
        this.channelName.getUserVideoFilesMap().put(videoName, list);
        return list;
    }


    public ChannelName getChannelName(){
        return channelName;
    }


    private class Connection extends Thread{

        Socket socket = null;
        private int port;
        private String ip;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        String request;
        Value videoFile;
        LinkedBlockingQueue<Value> channel_subs;
        LinkedBlockingQueue<Value> hashtag_subs;
        boolean hashed_to_name = false;
        public Connection(String ip, int port, LinkedBlockingQueue<Value> channel_subs, LinkedBlockingQueue<Value> hashtag_subs){
            this.ip = ip;
            this.port = port;
            this.channel_subs = channel_subs;
            this.hashtag_subs = hashtag_subs;
            this.videoFile = channel_subs.peek();
        }
        public Connection(String ip, int port, LinkedBlockingQueue<Value> channel_subs, LinkedBlockingQueue<Value> hashtag_subs, boolean hashed_to_name){
            this.ip = ip;
            this.port = port;
            this.channel_subs = channel_subs;
            this.hashtag_subs = hashtag_subs;
            this.hashed_to_name = hashed_to_name;
        }

        public void run(){
            try {
                socket = new Socket(ip, port);
                in = new ObjectInputStream(socket.getInputStream());
                out = new ObjectOutputStream(socket.getOutputStream());
                Publisher.this.pubSocket = socket;
                Publisher.this.in = in;
                Publisher.this.out = out;
                out.writeObject("publisher\n");
                out.flush();
                out.writeObject(channelName);
                out.flush();
                sendData();          // this thread connects to the hash(publisher hashtag) broker and sends videos that match hashtag.
                while (true){
                    // send videos
                    if(this.hashed_to_name){
                        if(this.videoFile != getLast(videos_for_channel)){
                            this.videoFile = getLast(videos_for_channel);
                            if(this.videoFile != null) {
                                sendVideo();
                            }
                        }
                    }else{

                    }
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        private void sendData(){
            try {
                out.writeObject("new\n");
                out.flush();
                out.writeObject(channelName.getName());
                out.flush();
                request = (String)in.readObject();
                if(request.matches("send\n"))
                    sendVideo();
            } catch (IOException | ClassNotFoundException ioException) {
                ioException.printStackTrace();
            }
        }
        private void sendVideo(){
            Publisher.this.push("", videoFile, out);
        }

        public Value getLast(LinkedBlockingQueue<Value> queue){
            for(int i=0;i < queue.size()-1; i++){
                queue.offer(queue.poll());
            }
            Value v = queue.poll();
            queue.offer(v);
            return v;
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