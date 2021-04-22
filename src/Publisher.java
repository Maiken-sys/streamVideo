import java.util.ArrayList;
import java.util.List;
public interface Publisher extends Node{
    ChannelName channelName = null;

    public void addHashTag(String s);
    public void removeHashTag(String s);
    public List<Broker> getBrokerList();
    public Broker hashTopic(String s);
    public void push(String s, Value v);
    public void notifyFailure(Broker broker);
    public void notifyBrokersForHashTags(String s);
    public ArrayList<Value> generateChunks(String s);
}
