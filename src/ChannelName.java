import org.checkerframework.checker.units.qual.C;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ChannelName {

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



    public String getChannelName(){
        return channelName;
    }

    public List<String> getHashtagsPublished(){
        return hashtagsPublished;
    }

    public HashMap<String, ArrayList<VideoFile>> getUserVideoFilesMap() {
        return this.userVideoFilesMap;
    }
}