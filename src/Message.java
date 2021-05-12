import java.io.Serializable;

public class Message implements Serializable {
    String a;
    String b;

    public Message(String a, String b){
        super();
        this.a = a;
        this.b = b;
    }



    public String getA() { return a; }

    public void setA(){
        this.a = a;
    }

    public String getB(){
        return b;
    }

    public void setB(){
        this.b = b;
    }
}
