import java.awt.*;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Consumer implements AppNodeImpl {
    public int port;
    public Socket conSocket;        //it is the socket that client uses to communicate with server
    private String name;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


    public Consumer(int port, String name){
        this.port = port;
        this.name = name;
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
            return (ArrayList<Broker>)in.readObject();
        }catch (ClassNotFoundException | IOException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void connect() {
        try {
            conSocket = new Socket("127.0.0.1", this.port);
            out = new ObjectOutputStream(conSocket.getOutputStream());
            in = new ObjectInputStream(conSocket.getInputStream());
            getBrokerList();
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
        out = null;
        in = null;
        conSocket = null;
        System.err.println("Disconnected");
    }

    @Override
    public void updateNodes() {

    }


    public void request(String s) {
        ArrayList<Value> values = new ArrayList<>();
        if(s.charAt(0) != '#')
            s = '#' + s;
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
        try {
            Desktop.getDesktop().open(new File("savedVideos/" + v.getVideoFile().getVideoName()));
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
    public void push(String hashtag, String videoName, ObjectOutputStream out){

    }

    @Override
    public void notifyFailure(Broker broker) {

    }

    @Override
    public void notifyBrokersForHashTags(String s) {

    }

    @Override
    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName) {
        return null;
    }
}
