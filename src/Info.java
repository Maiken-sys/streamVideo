import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;


public class Info implements Serializable {

    private HashMap<Integer, String> brokers = new HashMap<>();
    private ArrayList<Pair> broker_data = new ArrayList<>();
    private ArrayList<Integer> subscriptions = new ArrayList<>();

    public void addBroker(int port, String ip){
        brokers.put(port, ip);
    }

    public void addBrokerData(int port, String topic){
        boolean exists = false;
        for(Pair pair : broker_data){
            if(pair.getPort() == port){
                if(pair.contains(topic)){
                    exists = true;
                }
            }
        }
        if(!exists)
            broker_data.add(new Pair(port, topic));
    }


    public int getValuePort(String s){
        for(Pair triplet : broker_data){
            if(triplet.contains(s)){
                return triplet.getPort();
            }
        }
        return -1;
    }


    public String getValueIp(int port){
        return brokers.get(port);
    }


    public void update(Info info){
        for(int port : info.getSubscriptions()){
            subscriptions.add(port);
        }
        for(Pair pair : info.getBroker_data()){
            broker_data.add(pair);
        }
        for(Integer key : info.getBrokers().keySet()){
            brokers.put(key, info.getBrokers().get(key));
        }
    }

    public HashMap<Integer, String> getBrokers(){
        return brokers;
    }

    public ArrayList<Pair> getBroker_data(){
        return broker_data;
    }

    public void addSubscriber(int port){
        if(!subscriptions.contains(port))
            subscriptions.add(port);
    }

    public ArrayList<Integer> getSubscriptions(){
        return subscriptions;
    }


    @Override
    public String toString(){
        String str = "";
        for(Pair pair : broker_data){
            str += pair + "|";
        }
        return str;
    }

    private class Pair implements Serializable {

        int port;
        String topic;

        public Pair(int port, String topic){
            this.port = port;
            this.topic = topic;
        }

        public int getPort(){
            return port;
        }


        public boolean contains(String s){
            if(topic.equals(s))
                return true;
            else
                return false;
        }

        @Override
        public String toString(){
            return "port="+port+"|topic="+topic;
        }
    }
}
