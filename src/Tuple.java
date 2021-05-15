import java.util.ArrayList;

public class Tuple {

    private ChannelName channelName;
    private String videoName;
    private String videoDuration;

    private int brokerId;
    private ArrayList<String> hashtags;

    public Tuple(ChannelName channelName, String videoName, String videoDuration){
        this.channelName = channelName;
        this.videoName = videoName;
        this.videoDuration = videoDuration;
    }

    public Tuple(int brokerId, ChannelName channelName, ArrayList<String> hashtags){
        this.brokerId = brokerId;
        this.channelName = channelName;
        this.hashtags = new ArrayList<>(hashtags);
    }

    public ChannelName getChannelName(){
        return channelName;
    }
    public String getVideoName(){
        return videoName;
    }
    public String getVideoDuration(){
        return videoDuration;
    }
}
