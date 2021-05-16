import org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class AppNode extends Thread implements Node, AppNodeImpl{
    private Publisher publisher;
    private Consumer consumer;
    private String name;
    private static Scanner sc = new Scanner(System.in);


    public AppNode(String name){
        this.name = name;
        publisher = new Publisher(name);
        consumer = new Consumer(name);
    }

    public static void main(String [] Args){
        System.out.println("Enter you username:");
        AppNode appNode1 = new AppNode(sc.nextLine());
        appNode1.start();
    }


    public void run(){
        publisher.connect();
        consumer.connect();
        System.out.println("CHOOSE ACTION");
        System.out.println("===========================");
        System.out.println("1. ADD VIDEO.");
        System.out.println("2. SEARCH TOPIC.");
        int answer = Integer.parseInt(sc.nextLine());
        switch (answer){
            case 1: publisher.addVideo();
            case 2:
                System.out.println("ENTER TOPIC");
                consumer.request(sc.nextLine());
            default:
        }
    }


    private void addVideo(){
        publisher.addVideo();
    }



    @Override
    public void init(int x) {

    }

    @Override
    public List<Broker> getBrokers() {
        return null;
    }


    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }

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
    public void push(String key, Value videoName, ObjectOutputStream out) {

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