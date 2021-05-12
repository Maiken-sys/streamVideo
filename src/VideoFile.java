import java.io.*;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.mp4.MP4Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;



public class VideoFile implements Serializable{

    ChannelName myChannel;
    String videoName;
    String channelName;
    String dateCreated;
    private String length;
    private int chunk_size;
    String framerate;
    ArrayList<String> associatedHashtags = new ArrayList<String>();
    byte[] videoFileChunk;



    public VideoFile(int chunk_size){
        this.videoFileChunk = new byte[chunk_size];
    }

    public VideoFile(ArrayList<Value> values, String videoName){
        int size = 0;
        this.videoName = videoName;
        for(Value value : values){
            size += value.getVideoFile().getVideoFileChunk().length;
        }
        this.videoFileChunk = new byte[size];
        int pointer = 0;
        for(Value value : values){
            byte[] temp = value.getVideoFile().getVideoFileChunk();
            for(int i = 0; i < temp.length; i++){
                this.videoFileChunk[pointer] = temp[i];
                pointer++;
            }
        }
    }

    public VideoFile(String path) throws TikaException, IOException, SAXException {
        readVideo(path);
    }


    public void addHashtag(String hashtag){
        if(hashtag.charAt(0) != '#')
            hashtag = "#" + hashtag;
        associatedHashtags.add(hashtag);
    }

    public String getVideoName(){
        return videoName;
    }
    private void readVideo(String path) throws IOException, SAXException, TikaException{
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        FileInputStream inputstream = new FileInputStream(new File(path));
        ParseContext pcontext = new ParseContext();
        MP4Parser MP4Parser = new MP4Parser();
        MP4Parser.parse(inputstream, handler, metadata, pcontext);
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        Date test = metadata.getDate(TikaCoreProperties.CREATED);
        String strDate = formatter.format(test);
        dateCreated = strDate;
        length = metadata.get("xmpDM:duration");
        System.out.println(metadata);
    }

    public byte[] getVideoFileChunk() {
        return videoFileChunk;
    }

    public void saveVideo() throws IOException {
        FileOutputStream out = new FileOutputStream("newVideos/" + this.videoName + ".mp4");
        out.write(this.videoFileChunk);
    }
}