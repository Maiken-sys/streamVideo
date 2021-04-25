import java.io.*;
import java.net.Socket;
import java.util.List;

public class ConsumerImpl extends Thread implements Consumer {

    Message msg;
    int port;
    String ip;

    public ConsumerImpl(String ip, int port){
        this.ip = ip;
        this.port = port;
    }

    public String getIp(){
        return ip;
    }
    public static class Message{

        Object obj;
        public Message(Object obj){
            this.obj = obj;
        }
        public Object getObj(){
            return obj;
        }
    }

    @Override
    public List<Broker> getBrokers(){
        return null;
    }

    @Override
    public void register(Broker broker, String s) {

    }

    @Override
    public void disconnect(Broker broker, String s) {

    }

    @Override
    public void playData(String s, Value v) {

    }

    @Override
    public void init(int x) {

    }

    @Override
    public void connect() {

    }

    @Override
    public void disconnect() {

    }

    @Override
    public void updateNodes() {

    }


    public void run(){
        Socket requestSocket = null;
        BufferedReader in = null;
        PrintWriter out = null;

        try{
            requestSocket = new Socket(ip, port);

            out = new PrintWriter(requestSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(requestSocket.getInputStream()));

            out.write("hello.\n");
            out.flush();


            String status = in.readLine();
            if(status.contains("unregistered")){
                //register();
            }

        }catch (IOException ioException){
            ioException.printStackTrace();
        }finally {
            try{
                in.close();
                out.close();
                requestSocket.close();
            }catch (IOException ioException){
                ioException.printStackTrace();
            }
        }
    }


    public static void main(String[] args){
        Message msg = new Message(5);
        ConsumerImpl c1 = new ConsumerImpl("127.0.0.1", 4321);
        //Consumer c2 = new ConsumerImpl("127.0.0.1", 4322, msg);
        c1.start();
    }
}
