import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Consumer implements AppNodeImpl {
    public int port;
    public Socket conSocket;        //it is the socket that client uses to communicate with server
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


    public Consumer(int port){
        this.port = port;
    }


    //*******************************************************************************
    //Node override methods****************************************************
    //*******************************************************************************
    @Override
    public void init(int x) {

    }

    @Override
    public List<Broker> getBrokers(){
        return null;
    }

    @Override
    public void connect() {
        try {
            conSocket = new Socket("127.0.0.1", this.port);
            out = new ObjectOutputStream(conSocket.getOutputStream());
            in = new ObjectInputStream(conSocket.getInputStream());
        } catch (UnknownHostException e) {
            System.err.println("Προσπαθεις να συνδεθεις σε άγνωστο host!!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }finally {
            this.disconnect();
        }
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

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
    public void push(String s, Value v) {

    }

    @Override
    public void notifyFailure(Broker broker) {

    }

    @Override
    public void notifyBrokersForHashTags(String s) {

    }

    @Override
    public ArrayList<Value> generateChunks(VideoFile video) {
        return null;
    }


}
