package com.distributedsystems.pubsubsystem;

import android.os.AsyncTask;
import android.util.Log;
import android.util.Pair;

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
    private String ip;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;
    boolean interrupt = false;


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
                new Connection().execute(new DataPack(ip, port, true, null, subscriptions, connections, videos, -1));
            }
        }
    }

    public Pair<String[], VideoFile> request(String s, int action) {
        Info broker_data = getBrokers();
        int broker_port = broker_data.getValuePort(s);
        Pair<String[], VideoFile> pair = new Pair<>(new String[0], null);
        if(broker_port != -1){
            try {
                pair = new Connection().execute(new DataPack(ip, broker_port, false, s, subscriptions, connections, videos, action)).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }else{
        }
        return pair;
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



    public void interrupt(){
        this.interrupt = true;
    }



    public void subscribe(String topic){
        subscriptions.add(topic);
    }

    public void unsubscribe(String topic){
        subscriptions.remove(topic);
    }

    public void startConnection(String ip, int port){
        new Connection().execute(new DataPack(ip, port, true, "", subscriptions, connections, videos, -1));
    }

    public LinkedBlockingQueue<String> getVideos() {
        return videos;
    }

    private class Connection extends AsyncTask<DataPack, String, Pair<String[], VideoFile>> {

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
        int action = -1;    // -1: send request, 0: receive video, 1: subscribe, 2: unsubscribe

        @Override
        protected Pair<String[], VideoFile> doInBackground(DataPack... dataPacks){
            try{
                String[] result = null;
                VideoFile videoFile = null;
                DataPack dataPack = dataPacks[0];
                this.ip = dataPack.getIp();
                this.port = dataPack.getPort();
                this.listener = dataPack.isListener();
                this.request = dataPack.getRequest();
                this.subscriptions = dataPack.getSubscriptions();
                this.connections = dataPack.getConnections();
                this.videos = dataPack.getVideos();
                this.action = dataPack.getAction();
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
                        socket.setSoTimeout(500);
                        while(true && !interrupt){
                            try{
                                msg = (String)in.readObject();
                            }catch (Exception e){
                                continue;
                            }
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
                                        // TODO SAVE VIDEO TO INTERNAL STORAGE
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
                    if(action == -1){
                        send_request(request);
                        String[] videos_found = receive_answer();
                        boolean is_subscribed;
                        out.writeObject("check-subscription\n");
                        out.flush();
                        out.writeObject(request);
                        out.flush();
                        is_subscribed = in.readBoolean();
                        result = new String[videos_found.length+2];
                        result[0] = request;
                        result[1] = String.valueOf(is_subscribed);
                        for(int i=0; i<videos_found.length;i++){
                            result[i+2] = videos_found[i];
                        }
                    }

                    if(action == 1){
                        out.writeObject("request\n");
                        out.flush();
                        out.writeObject("subscribe\n");
                        out.flush();
                        out.writeObject(request);
                        out.flush();
                        synchronized (subscriptions){
                            subscriptions.add(request);
                        }
                        //Consumer.this.startConnection(this.ip, this.port);

                    }
                    if(action == 2){
                        Consumer.this.unsubscribe(request);
                        out.writeObject("request\n");
                        out.flush();
                        out.writeObject("unsubscribe\n");
                        out.flush();
                        out.writeObject(request);
                        out.flush();
                        synchronized (subscriptions){
                            subscriptions.remove(request);
                        }
                    }
                    if(action == 0){
                        out.writeObject("request\n");
                        out.flush();
                        out.writeObject(request);
                        out.flush();
                        videoFile = receive_video();
                    }
                    return new Pair<>(result, videoFile);
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

         private VideoFile receive_video(){
             try{
                 ArrayList<Value> video_chunks = new ArrayList<>();
                 String answer = (String) in.readObject();
                 if(answer.matches("video-found\n")) {
                     int chunks = (Integer) in.readObject();
                     for(int i = 0; i < chunks; i++){
                         video_chunks.add((Value) in.readObject());
                     }
                     VideoFile video = new VideoFile(video_chunks, video_chunks.get(0).getVideoFile().getVideoName());
                     return video;
                 }
             }catch (Exception e){
                 e.printStackTrace();
             }
             return null;
         }

        @Override
        protected void onPostExecute(Pair<String[], VideoFile> pair) {
            super.onPostExecute(pair);
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
        private int action;

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

        public int getAction(){
            return action;
        }

        public DataPack(String ip, int port, boolean listener, String request, LinkedBlockingQueue<String> subscriptions, LinkedBlockingQueue<Integer> connections, LinkedBlockingQueue<String> videos, int action){
            this.ip = ip;
            this.port = port;
            this.listener = listener;
            this.request = request;
            this.subscriptions = subscriptions;
            this.connections = connections;
            this.videos = videos;
            this.action = action;
        }
    }

}