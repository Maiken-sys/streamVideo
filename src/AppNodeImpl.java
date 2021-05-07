import java.util.ArrayList;
import java.util.List;
public interface AppNodeImpl extends Node{
    ChannelName channelName = null;



    public void addHashTag(String s);
    public void removeHashTag(String s);
    public List<Broker> getBrokerList();
    public Broker hashTopic(String s);
    public void push(String s, Value v);
    public void notifyFailure(Broker broker);
    public void notifyBrokersForHashTags(String s);
    public ArrayList<Value> generateChunks(VideoFile video);

    public void register(Broker broker, String s);
    public void disconnect(Broker broker, String s);
    public void playData(String s, Value v);



}
