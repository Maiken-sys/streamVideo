package com.distributedsystems.pubsubsystem;

import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Consumer {
    public Socket conSocket;
    private ChannelName channelName;
    private LinkedBlockingQueue<String> subscriptions = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Integer> connections = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<String> videos = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<String> request_data = new LinkedBlockingQueue<>();
    private String ip;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


    public Consumer(String name){
        this.channelName = new ChannelName(name);
        this.ip = "10.0.2.2";
    }


    public LinkedBlockingQueue<String> getSubscriptions(){
        return subscriptions;
    }

    public Info getBrokers(){
        try {
            out.writeObject("send-brokers\n");
            out.flush();
            Info broker_data = (Info)in.readObject();
            return broker_data;
        }catch (ClassNotFoundException | IOException e){
            e.printStackTrace();
        }
        return null;
    }



    public void connect() {     // connect to a random broker to get broker info.
        try {
            conSocket = new Socket(ip, 4321);
            out = new ObjectOutputStream(conSocket.getOutputStream());
            in = new ObjectInputStream(conSocket.getInputStream());

        } catch (IOException exception) {
            exception.printStackTrace();
            this.disconnect();
        }
    }

    public void disconnect() {
        try {
            out = null;
            in = null;
            conSocket.close();
        } catch (IOException | NullPointerException e) {
            e.printStackTrace();
        }
    }


    public void sendInfo(){
        try{
            out.writeObject("consumer\n");
            out.flush();
            out.writeObject(channelName);
            out.flush();
            in.readBoolean();
            out.writeObject("init\n");
            out.flush();
        }catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public void initialize_connections(){  // connect to all subscribed brokers (stand by for new videos).
        Info broker_data = getBrokers();
        if(broker_data != null){
            for(int port : broker_data.getSubscriptions()){
                new Connection(this).execute(new DataPack(ip, port, true, null, subscriptions, connections, videos, request_data));
            }
        }
    }

    public String[] request(String s) {
        Info broker_data = getBrokers();
        int broker_port = broker_data.getValuePort(s);
        String[] result = new String[0];
        if(broker_port != -1){
            new Connection(this).execute(new DataPack(ip, broker_port, false, s, subscriptions, connections, videos, request_data));
        }
        return result;
    }

    public ChannelName getChannelName() {
        return channelName;
    }


    public void playData(String s, VideoFile v) {
        /*try {
            Desktop.getDesktop().open(new File(s + "/" + v.getVideoName()));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }*/
    }


    public LinkedBlockingQueue<String> getRequest_data() {
        return request_data;
    }

    public void subscribe(String topic){
        subscriptions.add(topic);
    }

    public void unsubscribe(String topic){
        subscriptions.remove(topic);
    }

    public void startConnection(String ip, int port){
        new Connection(this).execute(new DataPack(ip, port, true, "", subscriptions, connections, videos, request_data));
    }

    public LinkedBlockingQueue<String> getVideos() {
        return videos;
    }

    private class Connection extends AsyncTask<DataPack, String, String[]> {

        Scanner sc = new Scanner(System.in);
        ObjectInputStream in;
        ObjectOutputStream out;
        Socket socket;
        int port;
        String ip;
        boolean listener;
        String request;
        LinkedBlockingQueue<String> subscriptions;
        LinkedBlockingQueue<Integer> connections;
        LinkedBlockingQueue<String> videos;
        Consumer caller;
        public Connection(Consumer caller){
            this.caller = caller;
        }

        @Override
        protected String[] doInBackground(DataPack... dataPacks){
            try{
                String[] result;
                DataPack dataPack = dataPacks[0];
                this.ip = dataPack.getIp();
                this.port = dataPack.getPort();
                this.listener = dataPack.isListener();
                this.request = dataPack.getRequest();
                this.subscriptions = dataPack.getSubscriptions();
                this.connections = dataPack.getConnections();
                this.videos = dataPack.getVideos();
                socket = new Socket(ip, port);
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());
                out.writeObject("consumer\n");
                out.flush();
                out.writeObject(Consumer.this.getChannelName());
                out.flush();
                in.readBoolean();
                if(listener){
                    try{
                        synchronized (connections){
                            for(Integer port : connections){
                                if(port == this.port){
                                    this.in = null;
                                    this.out = null;
                                    socket.close();
                                    return null;
                                }
                            }
                            connections.add(this.port);
                        }
                        String msg="";
                        out.writeObject("listener\n");
                        out.flush();
                        while(true){
                            msg = (String)in.readObject();
                            if(msg.matches("new-video\n")){
                                out.writeObject("ack\n");
                                out.flush();
                                String title = (String) in.readObject();
                                synchronized (videos){
                                    if(videos.contains(title)){
                                        out.writeObject("exists\n");
                                        out.flush();
                                    }else{
                                        out.writeObject("ack\n");
                                        out.flush();
                                        int total_chunks = (Integer)in.readObject();
                                        ArrayList<Value> values = new ArrayList<>();
                                        for(int i = 0; i<total_chunks; i++){
                                            values.add((Value) in.readObject());
                                        }
                                        VideoFile video = new VideoFile(values, values.get(0).getVideoFile().getVideoName());
                                        System.err.println(video.getVideoName());
                                        //video.saveVideo(channelName.getName());
                                        videos.offer(title);
                                    }
                                }
                            }
                        }
                    }catch (IOException | ClassNotFoundException e){
                        System.err.println("Disconnected");
                        e.printStackTrace();
                    }
                }else{
                    send_request(request);
                    String[] videos_found = receive_answer();
                    boolean is_subscribed;

                    out.writeObject("check-subscription\n");
                    out.flush();
                    out.writeObject(request);
                    out.flush();
                    is_subscribed = in.readBoolean();
                    result = new String[videos_found.length+1];
                    result[0] = String.valueOf(is_subscribed);
                    for(int i=1; i<videos_found.length;i++){
                        result[i] = videos_found[i-1];
                    }
                    if(is_subscribed){

                    }
                    // TODO FIX SUB / UNSUB
                    /*if(sub){
                        out.writeObject("subscribe\n");
                        out.flush();
                        out.writeObject(request);
                        out.flush();
                        synchronized (subscriptions){
                            subscriptions.add(request);
                        }
                        Consumer.this.startConnection(this.ip, this.port);
                    }else if(unsub){
                        Consumer.this.unsubscribe(request);
                        out.writeObject("unsubscribe\n");
                        out.flush();
                        out.writeObject(request);
                        out.flush();
                        synchronized (subscriptions){
                            subscriptions.remove(request);
                        }
                    }*/
                    synchronized (request_data){
                        for(String str : result){
                            request_data.offer(str);
                        }
                    }
                    if(videos_found != null && videos_found.length>0){
                        String video_string = select_video(videos_found);
                        if(!video_string.isEmpty()){
                            out.writeObject(video_string);
                            out.flush();
                            receive_video();
                        }else{
                            this.socket.close();
                        }
                    }else{
                        System.err.println("NO VIDEOS FOUND");
                    }
                    return result;
                }
            }catch (IOException e){
                e.printStackTrace();
            }
            return null;
         }
         private void send_request(String request){
             try{
                 out.writeObject("request\n");
                 out.flush();
                 out.writeObject(request);
                 out.flush();
             }catch (Exception e){
                 e.printStackTrace();
             }
         }
         private String[] receive_answer(){
            try{
                String answer = (String)in.readObject();
                if(answer.matches("videos-found\n")){
                    int total_videos = (Integer)in.readObject();
                    String[] video_data = new String[total_videos];
                    for(int i = 0; i < total_videos; i++){
                        video_data[i] = (String)in.readObject();
                    }
                    return video_data;
                }
                return new String[0];
            }catch (IOException | ClassNotFoundException e){
                e.printStackTrace();
            }
            return null;
         }
         private String select_video(String[] videos){
             System.out.println("FOUND " + videos.length + " VIDEOS");
             System.out.println("============================");
             for(int i = 0; i < videos.length; i++){
                 System.out.println(i+1 + ". " + videos[i]);
             }
             System.out.println("0. EXIT");
             int vid=-1;
             do{
                 try{vid = Integer.parseInt(sc.nextLine());}catch (Exception e){}
             }while (vid < 0 && vid > videos.length);
             if(vid == 0)
                 return "";
             return videos[vid-1];
         }


         private void receive_video(){
             try{
                 ArrayList<Value> video_chunks = new ArrayList<>();
                 String answer = (String) in.readObject();
                 if(answer.matches("video-found\n")) {
                     int chunks = (Integer) in.readObject();
                     for(int i = 0; i < chunks; i++){
                         video_chunks.add((Value) in.readObject());
                     }
                     VideoFile video = new VideoFile(video_chunks, video_chunks.get(0).getVideoFile().getVideoName());
                     video.saveVideo(channelName.getName());
                     System.out.println("PLAY VIDEO: " + video.getVideoName() + " ? [y/n]");
                     String str;
                     do{
                         str = sc.nextLine();
                     }while (!str.equalsIgnoreCase("y") && !str.equalsIgnoreCase("n"));
                     if(str.equalsIgnoreCase("y"))
                        playData(channelName.getName(), video);
                 }
             }catch (Exception e){
                 e.printStackTrace();
             }
         }

        @Override
        protected void onPostExecute(String[] strings) {
            super.onPostExecute(strings);
            Consumer.this.videos = videos;
        }
    }

    private class DataPack{

        private String ip;
        private int port;
        private boolean listener;
        private String request;
        private LinkedBlockingQueue<String> subscriptions;
        private LinkedBlockingQueue<Integer> connections;
        private LinkedBlockingQueue<String> videos;
        LinkedBlockingQueue<String> requst_data;

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public boolean isListener() {
            return listener;
        }

        public String getRequest() {
            return request;
        }

        public LinkedBlockingQueue<String> getSubscriptions() {
            return subscriptions;
        }

        public LinkedBlockingQueue<Integer> getConnections() {
            return connections;
        }

        public LinkedBlockingQueue<String> getVideos() {
            return videos;
        }

        public DataPack(String ip, int port, boolean listener, String request, LinkedBlockingQueue<String> subscriptions, LinkedBlockingQueue<Integer> connections, LinkedBlockingQueue<String> videos, LinkedBlockingQueue<String> requst_data){
            this.ip = ip;
            this.port = port;
            this.listener = listener;
            this.request = request;
            this.subscriptions = subscriptions;
            this.connections = connections;
            this.videos = videos;
            this.requst_data = requst_data;
        }
    }
}