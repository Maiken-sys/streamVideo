import java.io.Serializable;

public class Value implements Serializable {
    private VideoFile videoFile;
    private static final long serialVersionUID = 6529685098267757690L;

    public Value(VideoFile videoFile) {
        this.videoFile = videoFile;
    }

    public VideoFile getVideoFile() {
        return videoFile;
    }
}
