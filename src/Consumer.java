import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;

public class Consumer {
    private Socket conSocket;
    private ChannelName channelName;
    private LinkedBlockingQueue<String> subscriptions = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Integer> connections = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<String> videos = new LinkedBlockingQueue<>();
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


    public Consumer(String name){
        this.channelName = new ChannelName(name);
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
            conSocket = new Socket("127.0.0.1", 4321);
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
        } catch (IOException ioException) {
            ioException.printStackTrace();
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
        for(int port : broker_data.getSubscriptions()){
            new Connection(broker_data.getValueIp(port), port, true, null, subscriptions, connections, videos).start();
        }
    }

    public void request(String s) {
        Info broker_data = getBrokers();
        int broker_port = broker_data.getValuePort(s);
        if(broker_port != -1){
            Thread connection = new Connection(broker_data.getValueIp(broker_port), broker_port, false, s, subscriptions, connections, videos);
            connection.start();
            try{
                connection.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }else{
            System.err.println("TOPIC NOT FOUND");
        }
    }

    public ChannelName getChannelName() {
        return channelName;
    }


    public void playData(String s, VideoFile v) {
        try {
            Desktop.getDesktop().open(new File(s + "/" + v.getVideoName()));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }



    public void subscribe(String topic){
        subscriptions.add(topic);
    }

    public void unsubscribe(String topic){
        subscriptions.remove(topic);
    }

    public void startConnection(String ip, int port){
        new Connection(ip, port, true, "", subscriptions, connections, videos).start();
    }

    private class Connection extends Thread{

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
         public Connection(String ip, int port, boolean listener, String request, LinkedBlockingQueue<String> subscriptions, LinkedBlockingQueue<Integer> connections, LinkedBlockingQueue<String> videos){
             try{
                 this.ip = ip;
                 this.port = port;
                 this.listener = listener;
                 this.request = request;
                 this.subscriptions = subscriptions;
                 this.connections = connections;
                 this.videos = videos;
                 socket = new Socket(ip, port);
                 out = new ObjectOutputStream(socket.getOutputStream());
                 in = new ObjectInputStream(socket.getInputStream());
             }catch (IOException e){
                 e.printStackTrace();
             }
         }

         public void run(){
            try{
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
                                    return;
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
                                        int total_chunks = (Integer)in.readObject();
                                        ArrayList<Value> values = new ArrayList<>();
                                        for(int i = 0; i<total_chunks; i++){
                                            values.add((Value) in.readObject());
                                        }
                                        VideoFile video = new VideoFile(values, values.get(0).getVideoFile().getVideoName());
                                        System.err.println(video.getVideoName());
                                        video.saveVideo(channelName.getName());
                                        videos.offer(title);
                                    }
                                }
                            }
                        }
                    }catch (IOException | ClassNotFoundException e){
                        System.err.println("Disconnected");
                    }
                }else{
                    send_request(request);
                    String[] videos_found = receive_answer();
                    boolean is_subscribed;
                    boolean sub = false;
                    boolean unsub = false;
                    out.writeObject("check-subscription\n");
                    out.flush();
                    out.writeObject(request);
                    out.flush();
                    is_subscribed = in.readBoolean();
                    if(is_subscribed)
                        unsub = ask_unsubscribe(request);
                    else
                        sub = ask_subscribe(request);
                    if(sub){
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
                    }
                    if(videos_found != null && videos_found.length>0){   // if topic exists check if consumer is subscribed.
                        String video_string = select_video(videos_found);
                        if(!video_string.isBlank()){
                            out.writeObject(video_string);
                            out.flush();
                            receive_video();
                        }else{
                            this.socket.close();
                        }
                    }else{
                        System.err.println("NO VIDEOS FOUND");
                    }
                }
            }catch (IOException e){
                e.printStackTrace();
            }
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

         private boolean ask_subscribe(String topic){
             String answer;
             if(topic.equals(channelName.getName())){
                 return false;
             }
             System.out.println("SUBSCRIBE TO " + topic + " ? [y/n]");
             do{
                 answer = sc.nextLine();
             }while (!answer.equalsIgnoreCase("y") && !answer.equalsIgnoreCase("n"));
             if(answer.equals("y"))
                 return true;
            return false;
         }

         private boolean ask_unsubscribe(String topic){

             String answer;
             System.out.println("UNSUBSCRIBE FROM " + topic + " ? [y/n]");
             do{
                 answer = sc.nextLine();
             }while (!answer.equalsIgnoreCase("y") && !answer.equalsIgnoreCase("n"));
             if(answer.equals("y"))
                 return true;
             return false;
         }
    }
}