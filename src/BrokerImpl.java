import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class BrokerImpl implements Broker {


    public static void main(String [] Args){
        ServerSocket broker = null;

        try{
            broker = new ServerSocket(32000, 10);
            broker.setReuseAddress(true);
            while(true){
                Socket appNode = broker.accept();
                System.out.println("The next line after broker.accept(); just executed.");
                System.out.println("New appNode connected" + appNode.getInetAddress().getHostAddress());


                ActionsForClients clientSock = new ActionsForClients(appNode);
                new Thread(clientSock).start();
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
}
