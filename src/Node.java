import java.util.List;

public interface Node {

    List<Broker> brokers = null;
    public void init(int x);
    public void connect();
    public void disconnect();
    public void updateNodes();
    public List<Broker> getBrokers();
}
