import org.checkerframework.checker.units.qual.C;

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
        publisher = new Publisher(4321, name);
        consumer = new Consumer(4321, name);
    }

    public static void main(String [] Args){
        System.out.println("Enter you username");
        AppNode appNode1 = new AppNode(sc.nextLine());
        appNode1.start();
    }


    public void run(){
        publisher.connect();
        consumer.connect();
        getInput();

    }


    private void addVideo(){
        publisher.addVideo( sc.nextLine());
        //publisher.push("#hello","test");
    }

    private void getInput(){
        System.out.println("Select action");
        System.out.println("---------------------------");
        System.out.println("1. Search Channel");
        System.out.println("2. Search Hashtag");
        System.out.println("3. Publish Video");
        System.out.println("4. Add Hashtags to existing Video");
        System.out.println("5. Delete Video");
        System.out.println("6. Delete Hashtags of existing Video");
        int request;
        while (true){
            try{
                request = Integer.parseInt(sc.nextLine());
                break;
            }catch (Exception e){}
        }
        switch (request){
            case 1:;
            case 2:;
            case 3:;
            case 4:;
            case 5:;
            case 6:;
            default: return;
        }
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
    public void push(String hashtag, String videoName, ObjectOutputStream out) {

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