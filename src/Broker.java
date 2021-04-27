import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
public interface Broker extends Node {
    List<ConsumerImpl> registeredUsers = new ArrayList<ConsumerImpl>();
    List<PublisherImpl> registeredPublishers = new ArrayList<PublisherImpl>();


    public void calculateKeys();
    public Publisher acceptConnection(Publisher publisher);
    public Consumer acceptConnection(Consumer consumer);
    public void notifyPublisher(String s);
    public void notifyBrokersOnChanges();
    public void pull(String s);
    public void filterConsumers(String s);
    public String getRequest();

}
