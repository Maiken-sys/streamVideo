import java.util.ArrayList;
import java.util.List;

public class AppNode extends Thread implements Node, AppNodeImpl{
    public Publisher publisher;
    public Consumer consumer;



    public static void main(String [] Args){
        System.out.println("mmmnaiasdasda");
    }


    public void run(){

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
    public void push(String hashtag, String videoName) {

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




