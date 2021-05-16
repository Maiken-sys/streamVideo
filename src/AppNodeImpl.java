import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
public interface AppNodeImpl extends Node{
    ChannelName channelName = null;



    public void addHashTag(String s);
    public void removeHashTag(String s);
    public List<Broker> getBrokerList();
    public Broker hashTopic(String s);

    void push(String key, Value videoName, ObjectOutputStream out);

    void notifyFailure(ObjectOutputStream out);

    public void notifyBrokersForHashTags(String s);
    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName);

    public void register(Broker broker, String s);
    public void disconnect(Broker broker, String s);
    public void playData(String s, Value v);



}
