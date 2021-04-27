import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class broker_manager{
    //  only brokers connect here

    private static final Object lock = new Object();
    static final BlockingQueue<HashMap<Integer, String>> queue = new LinkedBlockingQueue<HashMap<Integer, String>>();
    public static void main(String[] args){

        queue.offer(new HashMap<Integer, String>());
        int port = Integer.parseInt(args[0]);
        try{
            ServerSocket broker = new ServerSocket(port, 10);
            broker.setReuseAddress(true);
            while(true){
                Socket br = broker.accept();
                System.out.println("New Broker connected");
                Thread operations = new Operations(br);
                operations.start();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }



    public static class Operations extends Thread{
        Socket connection;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        BrokerImpl broker;
        String ip;
        int port;
        public Operations(Socket connection){
            this.connection = connection;
        }

        @Override
        public void run(){
            try{
                out = new ObjectOutputStream(connection.getOutputStream());
                in = new ObjectInputStream(connection.getInputStream());
                broker = (BrokerImpl) in.readObject();
                ip = broker.getIp();
                port = broker.getPort();
                synchronized (lock){
                    HashMap<Integer, String> temp = new HashMap<Integer, String>(queue.take());
                    temp.put(port, ip);
                    queue.offer(temp);
                }
                while (true){
                    try{
                        out.writeObject(queue.peek());
                        out.flush();
                        sleep(2_000);   // exec every 2 sec.
                    }catch (Exception e){
                        System.out.println("Broker disconnected");
                        synchronized (lock){
                            HashMap<Integer, String> temp = new HashMap<Integer, String>(queue.take());
                            temp.remove(port);
                            queue.offer(temp);
                        }
                        break;
                    }
                }
            }catch (IOException | InterruptedException | ClassNotFoundException e){
                e.printStackTrace();
            }
        }
    }
}
