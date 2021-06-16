package com.distributedsystems.pubsubsystem;

import android.os.Build;

import androidx.annotation.RequiresApi;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.mp4.MP4Parser;
import org.apache.tika.sax.BodyContentHandler;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


public class VideoFile implements Serializable{

    private ChannelName channel;
    private String videoName;
    private String channelName;
    private String dateCreated;
    private String length;
    private ArrayList<String> associatedHashtags = new ArrayList<String>();
    byte[] videoFileChunk;



    public VideoFile(int chunk_size, String channelName, VideoFile videoFile){
        this.videoFileChunk = new byte[chunk_size];
        this.channelName = channelName;
        this.videoName = videoFile.getVideoName();
        this.length = videoFile.getLength();

    }

    public VideoFile(ArrayList<Value> values, String videoName){
        int size = 0;
        this.videoName = videoName;
        this.channel = values.get(0).getChannelName();
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

    @RequiresApi(api = Build.VERSION_CODES.O)
    public VideoFile(String path) {
        readVideoData(path);
        try{
            this.videoFileChunk = Files.readAllBytes(Paths.get(path));
            this.videoName = path.substring(path.lastIndexOf('/')+1);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void deleteChunks(){
        this.videoFileChunk = null;
    }


    public void addHashtag(String hashtag){
        if(hashtag.isEmpty()){
            return;
        }
        if(!hashtag.startsWith("#")){
            hashtag = "#" + hashtag;
        }
        this.associatedHashtags.add(hashtag);
    }

    public String getVideoName(){
        return videoName;
    }

    private void readVideoData(String path){
        try{
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        FileInputStream inputstream = new FileInputStream(new File(path));
        ParseContext pcontext = new ParseContext();
        MP4Parser MP4Parser = new MP4Parser();
        MP4Parser.parse(inputstream, handler, metadata, pcontext);
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
        Date test = metadata.getDate(TikaCoreProperties.CREATED);
        String strDate = formatter.format(test);
        this.dateCreated = strDate;
        this.length = metadata.get("xmpDM:duration");
        }catch (Exception e){}
    }

    public byte[] getVideoFileChunk() {
        return videoFileChunk;
    }

    public void saveVideo(String name) throws IOException {
        File f = new File(name);
        if(!f.exists())
            f.mkdir();
        File temp = new File(f+"/"+videoName);
        int i = 1;
        while (temp.exists()){
            temp = new File(f + "/("+i+") "+videoName);
            i++;
        }
        FileOutputStream out = new FileOutputStream(temp.toString());
        out.write(this.videoFileChunk);
    }


    public ArrayList<String> getAssociatedHashtags(){
        return this.associatedHashtags;
    }

    public String getLength(){
        return length;
    }

}