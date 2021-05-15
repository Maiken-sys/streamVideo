import java.io.Serializable;

public class Value implements Serializable {
    private VideoFile videoFile;
    private ChannelName channelName;

    public Value(VideoFile videoFile, ChannelName channelName) {
        this.videoFile = videoFile;
        this.channelName = channelName;
    }

    public VideoFile getVideoFile() {
        return videoFile;
    }

    public ChannelName getChannelName(){
        return channelName;
    }

    @Override
    public String toString(){
        return "channel="+channelName.getName() +"|video="+videoFile.getVideoName()+"|length="+videoFile.getLength();
    }
}
