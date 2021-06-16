package com.distributedsystems.pubsubsystem;

import android.os.AsyncTask;
import android.os.Build;
import android.util.Log;

import androidx.annotation.RequiresApi;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;

public class Publisher {
    private ChannelName channelName;
    private String ip;
    private LinkedBlockingQueue<Value> videos = new LinkedBlockingQueue<>();


    private Socket pubSocket;
    ObjectOutputStream out;
    ObjectInputStream in;
    static Scanner sc = new Scanner(System.in);
    private ArrayList<Broker> broker_connections = new ArrayList<>();

    public Publisher(String name){
        this.channelName = new ChannelName(name);
        this.ip = "10.0.2.2";
    }

    public void deleteVideo(){
        if(videos.isEmpty()){
            System.err.println("NO VIDEOS PUBLISHED");
            return;
        }
        System.out.println("SELECT VIDEO TO DELETE");
        System.out.println("==========================");
        int i = 1;
        ArrayList<Value> published_videos = new ArrayList<>();
        for(Value value : videos){
            published_videos.add(value);
            System.out.println(i++ + ". " + value);
        }
        System.out.println("0. EXIT");
        int answer = -1;
        while (answer<0 || answer>videos.size()){
            answer = Integer.parseInt(sc.nextLine());
        }
        if(answer == 0){
            return;
        }
        for(String hashtag : published_videos.get(answer-1).getVideoFile().getAssociatedHashtags()){
            notifyBrokers(hashtag, published_videos.get(answer-1), true);
        }
        notifyBrokers(channelName.getName(), published_videos.get(answer-1), true);


        broker_connections.clear();
        videos.remove(published_videos.get(answer-1));
    }


    public void initialize_structures(){
        try {
            Broker broker = hashTopic(channelName.getName());
            new Connection().execute(new DataPack(broker.getIp(), broker.getPort(), videos, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void connect() {
        try {
            pubSocket = new Socket("10.0.2.2", 4321);  // connect to random broker
            out = new ObjectOutputStream(pubSocket.getOutputStream());
            in = new ObjectInputStream(pubSocket.getInputStream());
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }


    public void disconnect() {
        try {
            in.close();
            out.close();
            pubSocket.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public Broker hashTopic(String s) {
        try {
            if(pubSocket.isClosed()){
                this.connect();
            }
            out.writeObject("hash\n");
            out.flush();
            out.writeObject(s);
            out.flush();
            String broker_ip = (String) in.readObject();
            String broker_port = (String) in.readObject();
            this.disconnect();
            return new Broker(broker_ip, Integer.parseInt(broker_port));
        } catch (IOException | ClassNotFoundException ioException) {
            ioException.printStackTrace();
        }
        return null;
    }


    public void push(Value videoName, ObjectOutputStream out)  {
        ArrayList<VideoFile> chunks = this.generateChunks(videoName.getVideoFile().getVideoFileChunk(), videoName.getVideoFile().getVideoName(), videoName.getVideoFile());
        if(chunks == null) {
            return;
        }
        System.out.println("sending: " + videoName.getVideoFile().getVideoName() + "...");
        try{
            out.writeObject("video\n");
            out.flush();
            out.writeObject(videoName.getDate_uploaded());
            out.flush();
            out.writeInt(videoName.getVideoFile().getAssociatedHashtags().size());
            out.flush();
            for(String hs : videoName.getVideoFile().getAssociatedHashtags()){
                out.writeObject(hs);
                out.flush();
            }
            out.writeInt(chunks.size());
            out.flush();
            for (VideoFile chunk : chunks){
                Value value = new Value(chunk, channelName);
                out.writeObject(value);
                out.flush();
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void notifyBrokers(String s, Value videoFile, boolean delete) {
        try{
            Broker broker = hashTopic(s);
            for(Broker broker1 : broker_connections){
                if(broker1.getPort() == broker.getPort()){
                    return;
                }
            }
            broker_connections.add(broker);
            new Connection().execute(new DataPack(broker.getIp(), broker.getPort(), videos, videoFile, delete));

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @RequiresApi(api = Build.VERSION_CODES.O)
    public void addVideo(){
        int int_answer = -1;
        String str_answer = "";
        ArrayList<String> files = listFilesForFolder(new File("savedVideos"));
        System.out.println("Select video to upload [1-" + files.size() + "]");
        System.out.println("===========================");
        for(int i=0; i<files.size();i++){
            System.out.println(i+1 + "." + " " + files.get(i));
        }
        System.out.println("0. Exit");
        while (int_answer < 0 || int_answer > files.size()){
            try{
                int_answer = Integer.parseInt(sc.nextLine());
            }catch (Exception e){}
        }
        if(int_answer == 0)
            return;
        VideoFile newVid = new VideoFile("savedVideos/"+files.get(int_answer-1));
        do{
            System.out.println("Add Hashtag? [y/n]");
            str_answer = sc.nextLine();
            if(str_answer.matches("y|Y")){
                System.out.println("Enter hashtag: ");
                newVid.addHashtag(sc.nextLine().trim());
            }
        }while (!str_answer.matches("n|N"));
        System.out.println("Video added!");
        Value video = new Value(newVid, channelName);
        synchronized (videos){
            video.setDate_uploaded();
            videos.offer(video);
        }
        ArrayList<String> hashtags = newVid.getAssociatedHashtags();
        for(String hs : hashtags){
            notifyBrokers(hs, video, false);
        }
        notifyBrokers(channelName.getName(), video, false);
        broker_connections.clear();
    }

    public static ArrayList<String> listFilesForFolder(final File folder) {
        ArrayList<String> files = new ArrayList<String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                files.add(fileEntry.getName());
            }
        }
        return files;
    }


    public ArrayList<VideoFile> generateChunks(byte[] video, String videoName, VideoFile videoFile) {
        ArrayList<VideoFile> list = new ArrayList<VideoFile>();
        int chunk_size = 524288;
        int start = 0;
        while(start < video.length){
            int end = Math.min(video.length, start + chunk_size);
            VideoFile chunk = new VideoFile(end - start, channelName.getName(), videoFile);
            chunk.videoFileChunk = Arrays.copyOfRange(video, start, end);
            list.add(chunk);
            start += chunk_size;
        }
        this.channelName.getUserVideoFilesMap().put(videoName, list);
        return list;
    }


    public ChannelName getChannelName(){
        return channelName;
    }


    private class Connection extends AsyncTask<DataPack, String, String> {

        private Socket socket;
        private ObjectInputStream in;
        private ObjectOutputStream out;
        private String request;
        private int port;
        private Value videoFile;
        private LinkedBlockingQueue<Value> videos;
        private boolean init = false;
        private boolean delete = false;


        public Connection(){

        }
        @Override
        protected String doInBackground(DataPack... dataPacks){
            try {
                this.port = dataPacks[0].getPort();
                this.init = dataPacks[0].isInit();
                this.delete = dataPacks[0].isDelete();
                this.videoFile = dataPacks[0].getVideoFile();
                this.videos = dataPacks[0].getVideos();
                socket = new Socket(ip, port);
                in = new ObjectInputStream(socket.getInputStream());
                out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject("publisher\n");
                out.flush();
                out.writeObject(channelName);
                out.flush();
                if(!init){
                    sendData();
                }else if(delete){
                    out.writeObject("delete\n");
                    out.flush();
                    out.writeObject(videoFile.toString());
                    out.flush();
                }else {
                    out.writeObject("init\n");
                    out.flush();
                    int published_videos = (Integer)in.readObject();
                    synchronized (videos){
                        for(int i=0; i<published_videos; i++){
                            videos.offer((Value) in.readObject());
                        }
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
        private void sendData(){
            try {
                out.writeObject("new-video\n");
                out.flush();
                request = (String)in.readObject();
                if(request.matches("ack\n")){
                    sendVideo();
                }
            } catch (IOException | ClassNotFoundException ioException) {
                ioException.printStackTrace();
            }
        }
        private void sendVideo() throws IOException {
            Publisher.this.push(videoFile, out);
        }
    }

    private class DataPack{

        String ip;
        int port;
        LinkedBlockingQueue<Value> videos;
        Value videoFile;
        boolean delete = false;
        boolean init = false;


        public DataPack(String ip, int port, LinkedBlockingQueue<Value> videos, Value videoFile, boolean delete){
            this.ip = ip;
            this.port = port;
            this.videos = videos;
            this.delete = delete;
            this.videoFile = videoFile;
        }

        public DataPack(String ip, int port, LinkedBlockingQueue<Value> videos, boolean init){
            this.ip = ip;
            this.port = port;
            this.videos = videos;
            this.init = init;
        }
        public int getPort(){
            return port;
        }
        public boolean isInit(){
            return init;
        }
        public Value getVideoFile(){
            return videoFile;
        }
        public boolean isDelete(){
            return delete;
        }
        public  LinkedBlockingQueue<Value> getVideos(){
            return videos;
        }
    }
}