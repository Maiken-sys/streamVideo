import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ChannelName {

    private String channelName;
    private List<String> hashtagsPublished;
    private HashMap<String, ArrayList<Value>> userVideoFilesMap;

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

    public HashMap<String, ArrayList<Value>> getUserVideoFilesMap() {
        return this.userVideoFilesMap;
    }

}
