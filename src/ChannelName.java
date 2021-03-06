import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ChannelName implements Serializable {

    private String channelName;
    private List<String> hashtagsPublished;
    private HashMap<String, ArrayList<VideoFile>> userVideoFilesMap;

    public ChannelName() {

    }

    public ChannelName(String channelName){
        this.channelName = channelName;
        hashtagsPublished = new ArrayList<>();
        userVideoFilesMap = new HashMap<>();
    }





    public List<String> getHashtagsPublished(){
        return hashtagsPublished;
    }

    public void addHashtag(String s){
        hashtagsPublished.add(s);
    }

    public HashMap<String, ArrayList<VideoFile>> getUserVideoFilesMap() {
        return this.userVideoFilesMap;
    }

    public String getName(){
        return channelName;
    }
}