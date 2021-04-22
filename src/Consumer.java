public interface Consumer extends Node{

    public void register(Broker broker, String s);
    public void disconnect(Broker broker, String s);
    public void playData(String s, Value v);

}
