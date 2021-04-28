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

public class Publisher implements AppNodeImpl {
    private int port;
    public Socket pubSocket;        //it is the socket that client uses to communicate with server
    ObjectOutputStream out = null;
    ObjectInputStream in = null;

    public Publisher(int port){
        this.port = port;
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
        }
    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

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
        List<Broker> brokers = getBrokerList();
        for (Broker br : brokers) {
            System.out.println(2);
        }

        byte[] msg = s.getBytes(StandardCharsets.UTF_8);
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.digest(msg);
            byte[] hvalues = md.digest(msg);
            byte[] bvalues = md.digest(msg);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


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

    public int getPort() {
        return this.port;
    }

    @Override
    public ArrayList<Value> generateChunks(String s) {
        return null;
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
