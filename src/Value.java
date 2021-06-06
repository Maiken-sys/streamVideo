import java.io.Serializable;
import java.util.Date;


public class Value implements Serializable {
    private VideoFile videoFile;
    private ChannelName channelName;
    private Date date_uploaded;

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

    public Date getDate_uploaded(){
        return date_uploaded;
    }

    public void setDate_uploaded(){
        this.date_uploaded = new Date();
    }

    public void discardVideo(){
        this.videoFile.deleteChunks();
    }

    public void setDate_uploaded(Date date_uploaded){
        this.date_uploaded = date_uploaded;
    }

    public void addHashtag(String s){
        this.videoFile.addHashtag(s);
    }

    @Override
    public String toString(){
        return "channel="+channelName.getName() +"|video="+videoFile.getVideoName()+"|length="+videoFile.getLength()+"|uploaded="+date_uploaded;
    }
}
