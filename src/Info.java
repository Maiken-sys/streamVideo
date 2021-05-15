import java.io.Serializable;
import java.util.ArrayList;

public class Info implements Serializable {

    private ArrayList<Broker> brokers;   // brokers{port, ip}
    private Tuple tuple;

    public Info(ArrayList<Broker> brokers, Tuple tuple){
        this.brokers = brokers;
        this.tuple = tuple;
    }

    public ArrayList<Broker> getBrokers(){
        return brokers;
    }
    public Tuple getTuple(){
        return tuple;
    }
}
