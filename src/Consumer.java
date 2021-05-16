import java.awt.*;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Consumer implements AppNodeImpl {
    private Socket conSocket;        //it is the socket that client uses to communicate with server
    private ChannelName channelName;
    private Info broker_data;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


    public Consumer(String name){
        this.channelName = new ChannelName(name);
    }


    //*******************************************************************************
    //Node override methods****************************************************
    //*******************************************************************************
    @Override
    public void init(int x) {

    }

    @Override
    public List<Broker> getBrokers(){
        try {
            out.writeObject("send-brokers\n");    // ask for brokers list
            out.flush();
            broker_data = (Info) in.readObject();
            return broker_data.getBrokers();
        }catch (ClassNotFoundException | IOException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void connect() {
        try {
            conSocket = new Socket("127.0.0.1", 4321);
            out = new ObjectOutputStream(conSocket.getOutputStream());
            in = new ObjectInputStream(conSocket.getInputStream());
            getBrokers();
        } catch (UnknownHostException e) {
            System.err.println("Προσπαθεις να συνδεθεις σε άγνωστο host!!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            this.disconnect();
        }
    }

    @Override
    public void disconnect() {
        try {
            out = null;
            in = null;
            conSocket.close();
            System.err.println("Disconnected");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void updateNodes() {

    }


    public void request(String s, ObjectOutputStream out, ObjectInputStream in) {
        ArrayList<Value> values = new ArrayList<>();
        try {
            out.writeObject("sending-value\n");
            out.flush();
            out.writeObject(s);
            out.flush();
            int numOfValues = (Integer)in.readObject();
            for(int i = 0; i < numOfValues; i++)
                values.add((Value) in.readObject());
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    public ChannelName getChannelName() {
        return channelName;
    }

    //*******************************************************************************
    //Consumer-Only methods**********************************************************
    //*******************************************************************************
    @Override
    public void register(Broker broker, String s) {
        try{
            conSocket = new Socket(broker.getIp(), broker.getPort());
            out.writeObject("subscribe\n");
            out.flush();
            out.writeObject(s);
            out.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void disconnect(Broker broker, String s) {
        try{
            conSocket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void playData(String s, Value v) {
        try {
            Desktop.getDesktop().open(new File("savedVideos/" + v.getVideoFile().getVideoName()));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }


    private class Connection extends Thread{    // Threads standing by to receive data

        Socket socket;
        ObjectOutputStream out;
        ObjectInputStream in;
        LinkedBlockingQueue<Value> queue;
        public Connection(Socket socket, LinkedBlockingQueue<Value> queue){
            try {
                this.socket = socket;
                this.queue = queue;
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }

        public void run(){
            while(true){


            }
        }
    }

    //*******************************************************************************
    //Publisher-Only methods**********************************************************
    //*******************************************************************************
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
        return null;
    }

    @Override
    public void push(String hashtag, Value videoName, ObjectOutputStream out){

    }

    @Override
    public void notifyFailure(ObjectOutputStream out) {

    }

    @Override
    public void notifyBrokersForHashTags(String s) {

    }

    @Override
    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName) {
        return null;
    }
}
