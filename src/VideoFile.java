import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.SimpleFormatter;


import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.mp4.MP4Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;



public class VideoFile implements Serializable {

    private static final long serialVersionUID = 6529685098267757690L;
    ChannelName myChannel;
    String videoName;
    //String channelName;
    String dateCreated;
    private String length;
    private int chunk_size;
    String framerate;
    ArrayList<String> associatedHashtags;
    byte[] videoFileChunk;
    String path;

    public VideoFile(String hashtag, ChannelName myChannel, String videoName){
        this.associatedHashtags = new ArrayList<>();
        this.associatedHashtags.add(hashtag);
        this.myChannel = myChannel;
        this.videoName = videoName;
    }
    public VideoFile(String videoName, String hashtag){
        this.videoName = videoName;
        this.associatedHashtags.add(hashtag);
    }
    public VideoFile(int chunk_size){
        this.videoFileChunk = new byte[chunk_size];
    }



    public static void main(String[] args) throws TikaException, IOException, SAXException {
        ArrayList<String> hs = new ArrayList<>();
        hs.add("test1");
        hs.add("test2");
        VideoFile t = new VideoFile("newVid", "viral" );
        t.readVideo();
        System.out.println(t.length);
        System.out.println(t.dateCreated);
    }


    private void readVideo() throws IOException, SAXException, TikaException{
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
        videoFileChunk = convert(path);
        System.out.println(metadata);
    }

    public byte[] convert(String path) throws IOException {

        FileInputStream fis = new FileInputStream(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] b = new byte[1024];
        for (int readNum; (readNum = fis.read(b)) != -1;) {
            bos.write(b, 0, readNum);
        }
        byte[] bytes = bos.toByteArray();
        return bytes;
    }



}