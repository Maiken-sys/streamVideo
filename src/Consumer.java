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
    private ArrayList<Broker> brokers;
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
            ArrayList<Broker> brokers = new ArrayList<>();
            out.writeObject("send-brokers\n");    // ask for brokers list
            out.flush();
            int count = (Integer)in.readObject();
            for(int i = 0; i < count; i++){
                String[] broker = ((String)in.readObject()).split("-");
                brokers.add(new Broker(broker[1], Integer.parseInt(broker[2])));
            }
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
        } catch (IOException exception) {
            exception.printStackTrace();
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


    public void request(String s) {
        ArrayList<Value> values = new ArrayList<>();
        brokers = (ArrayList<Broker>) getBrokers();
        try {
            out.writeObject("hash\n");  // hash string.
            out.flush();
            out.writeObject(s);
            String b_ip = (String)in.readObject();
            int b_port = (Integer)in.readObject();
            this.disconnect();
            conSocket = new Socket(b_ip, b_port);
            out = new ObjectOutputStream(conSocket.getOutputStream());
            in = new ObjectInputStream(conSocket.getInputStream());
            out.writeObject("sending-value\n");
            out.flush();
            out.writeObject(s);
            out.flush();
            String answer = (String)in.readObject();
            if(answer.matches("no-videos-found\n")) {
                System.err.println(answer);
            }
            else{
                int sz;
                int numOfValues = (Integer)in.readObject();
                for(int i = 0; i < numOfValues; i++){
                    ArrayList<Value> video_data = new ArrayList<>();
                    sz = (Integer)in.readObject();
                    for(int j =0; j< sz;j++){
                        video_data.add((Value) in.readObject());
                    }
                    VideoFile file = new VideoFile(video_data, video_data.get(0).getVideoFile().getVideoName());
                    file.saveVideo(channelName.getName());
                }
            }
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
            Desktop.getDesktop().open(new File(channelName.getName() + "/" + v.getVideoFile().getVideoName()));
        } catch (IOException ioException) {
            ioException.printStackTrace();
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
