import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class BrokerImpl implements Broker {
    static List<ConsumerImpl> registeredUsers = new ArrayList<>();
    static List<PublisherImpl> registeredPublishers = new ArrayList<>();

    public static void main(String [] args){

        int port = Integer.parseInt(args[0]);
        try{
            ServerSocket broker = new ServerSocket(port, 10);
            broker.setReuseAddress(true);
            while(true){
                Socket appNode = broker.accept();
                System.out.println("The next line after broker.accept(); just executed.");
                System.out.println("New consumer connected " + appNode.getInetAddress().getHostAddress());

                Thread operations = new Operations(appNode);
                operations.start();

                //ActionsForClients clientSock = new ActionsForClients(appNode);
                //new Thread(clientSock).start();
            }

        } catch (IOException e){
            e.printStackTrace();
        }
    }



    @Override
    public void calculateKeys() {

    }

    @Override
    public Publisher acceptConnection(Publisher publisher) {
        return null;
    }

    @Override
    public Consumer acceptConnection(Consumer consumer) {
        return null;
    }

    @Override
    public void notifyPublisher(String s) {

    }

    @Override
    public void notifyBrokersOnChanges() {

    }

    @Override
    public void pull(String s) {

    }

    @Override
    public void filterConsumers(String s) {

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

    @Override
    public List<Broker> getBrokers() {
        return null;
    }


    private static class Operations extends Thread{

        Socket connection;
        BufferedReader in = null;
        PrintWriter out = null;
        public Operations(Socket connection){
            this.connection = connection;
            try{
                out = new PrintWriter(connection.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            }catch (IOException ioException){
                ioException.printStackTrace();
            }
        }

        @Override
        public void run(){
            getIdentity();
            //
        }

        private void getIdentity(){
            try{
                //ConsumerImpl.Message msg = (ConsumerImpl.Message)in.readObject();
                String id = in.readLine();
                System.out.println(connection.getInetAddress().toString());
                System.out.println(id);
                String reply = "hello-unregistered.\n";
                for(ConsumerImpl c : registeredUsers){
                    if(c.getIp().contains(connection.getInetAddress().toString())){
                        reply = ("hello-registered.\n");
                        break;
                    }
                }


                out.write(reply);
                out.flush();


            }catch (IOException e){
                e.printStackTrace();
            }finally {
                try{
                    in.close();
                    out.close();
                    connection.close();
                }catch (IOException ioException){
                    ioException.printStackTrace();
                }
            }
        }
    }
}