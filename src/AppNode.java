import java.util.Scanner;

public class AppNode extends Thread {
    private Publisher publisher;
    private Consumer consumer;
    private String name;
    private static Scanner sc = new Scanner(System.in);
    Object lock = new Object();


    public AppNode(String name){
        this.name = name;
        publisher = new Publisher(name);
        consumer = new Consumer(name);
    }

    public static void main(String [] args){
        System.out.println("Enter you username:");
        AppNode appNode1 = new AppNode(sc.nextLine());
        appNode1.start();
    }


    public void run(){
        publisher.connect();
        publisher.initialize_structures();
        consumer.connect();
        consumer.sendInfo();
        consumer.initialize_connections();
        while(true){
            int answer;
            synchronized (lock){
                System.out.println("CHOOSE ACTION");
                System.out.println("========================");
                System.out.println("1. ADD VIDEO");
                System.out.println("2. DELETE VIDEO");
                System.out.println("3. SEARCH TOPIC");
                answer = Integer.parseInt(sc.nextLine());
            }
            synchronized (lock){
                if(answer == 1){
                    publisher.addVideo();
                }else  if(answer == 2){
                    publisher.deleteVideo();
                }else if(answer == 3){
                    System.out.println("ENTER TOPIC");
                    consumer.request(sc.nextLine());
                }
            }
        }
    }

}